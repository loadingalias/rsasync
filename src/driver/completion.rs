//! The future of one driver operation.

use core::task::{Context, Poll};

use crate::completion::{Entry, Gone};
use crate::sync::Arc;
use crate::table::{OpId, Request};

use super::{Held, Shared};

/// The future of one operation: it gives the result and the resources after the terminal event.
/// It can move to another thread while the home stays driven, and it wakes the waker of its latest
/// poll.
#[derive(Debug)]
pub(crate) struct Completion {
    pub(super) shared: Arc<Shared>,
    id: OpId,
    /// A drop asks the home to cancel the operation: a receive, not a send.
    cancel_on_drop: bool,
    done: bool,
}

impl Completion {
    pub(super) fn new(shared: Arc<Shared>, id: OpId, cancel_on_drop: bool) -> Self {
        Self {
            shared,
            id,
            cancel_on_drop,
            done: false,
        }
    }

    /// The identity of the operation and its driver.
    pub(super) fn id(&self) -> super::OperationId {
        super::OperationId {
            driver: self.shared.driver,
            op: self.id,
        }
    }

    /// Asks the home to cancel the operation. The consumer keeps its interest, and later takes
    /// the result.
    pub(super) fn cancel(&self) {
        if !self.done {
            self.shared.requests.post(self.id, Request::Cancel);
            self.shared.wake();
        }
    }

    /// Makes the output from the result and the resources in their slot when they are ready, before
    /// the home can use the slot again, and otherwise registers the waker of `cx`. It has no poll
    /// budget: the driver of a later operation of the direction takes the result of an operation
    /// with it, outside the poll of a task.
    pub(super) fn take_with<T>(
        &mut self,
        cx: &Context<'_>,
        take: impl FnOnce(i32, &mut Held, &Self) -> T,
    ) -> Poll<Result<T, Gone>> {
        let id = self.id;
        let Some(entry) = self.entry() else {
            return Poll::Ready(Err(Gone));
        };
        let output = entry.poll_take(cx, id, |result, held| take(result, held, self));
        if output.is_ready() {
            self.done = true;
            // The home frees the slot at its next turn, and it needs no wake for that.
            self.shared.requests.post(id, Request::EndInterest);
        }
        output
    }

    fn entry(&self) -> Option<&Entry<Held>> {
        self.shared.entries.get(self.id.index() as usize)
    }

    /// The poll of the future of the operation: [`take_with`](Self::take_with), with the poll
    /// budget of the task.
    #[expect(
        clippy::panic,
        reason = "a poll after `Ready` breaks the contract of `Future`, and the caller has no recovery path"
    )]
    pub(super) fn poll_with<T>(
        &mut self,
        cx: &mut Context<'_>,
        take: impl FnOnce(i32, &mut Held, &Self) -> T,
    ) -> Poll<Result<T, Gone>> {
        if self.done {
            panic!("an `Operation` was polled after it gave `Ready`");
        }
        crate::coop::poll(cx, |cx| self.take_with(cx, take))
    }
}

impl Drop for Completion {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        if let Some(entry) = self.entry() {
            entry.cancel_wait();
        }
        self.shared.requests.post(self.id, Request::EndInterest);
        if self.cancel_on_drop {
            self.shared.requests.post(self.id, Request::Cancel);
            self.shared.wake();
        }
    }
}
