//! The future of one driver operation.

use core::future::Future;
use core::pin::Pin;
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
    shared: Arc<Shared>,
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

    fn entry(&self) -> Option<&Entry<Held>> {
        self.shared.entries.get(self.id.index() as usize)
    }
}

impl Future for Completion {
    type Output = Result<(i32, Held), Gone>;

    #[expect(
        clippy::panic,
        reason = "a poll after `Ready` breaks the contract of `Future`, and the caller has no recovery path"
    )]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.done {
            panic!("an `Operation` was polled after it gave `Ready`");
        }
        let id = self.id;
        let Some(entry) = self.entry() else {
            return Poll::Ready(Err(Gone));
        };
        let output = entry.poll_take(cx, id);
        if output.is_ready() {
            self.done = true;
            // The home frees the slot at its next turn, and it needs no wake for that.
            self.shared.requests.post(id, Request::EndInterest);
        }
        output
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
