//! The future of one driver operation.

use core::task::{Context, Poll};

use crate::completion::{Entry, Gone};
use crate::sync::Arc;
use crate::table::{OpId, Request};

#[cfg(all(feature = "local", target_os = "linux"))]
use super::Consumer;
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
    /// Where the consumer polls the operation.
    #[cfg(all(feature = "local", target_os = "linux"))]
    consumer: Consumer,
    done: bool,
}

impl Completion {
    pub(super) fn new(shared: Arc<Shared>, id: OpId, cancel_on_drop: bool) -> Self {
        Self {
            shared,
            id,
            cancel_on_drop,
            #[cfg(all(feature = "local", target_os = "linux"))]
            consumer: Consumer::Any,
            done: false,
        }
    }

    /// The same future, with `consumer` as its consumer, which the tag of its operation names.
    #[cfg(all(feature = "local", target_os = "linux"))]
    pub(super) const fn with_consumer(mut self, consumer: Consumer) -> Self {
        self.consumer = consumer;
        self
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
            self.end_taken(id);
        }
        output
    }

    /// Frees the slot of the operation `id`, whose result the consumer took. On the thread of the
    /// home, outside a turn, the driver frees it now, so the next operation of its direction does
    /// not look at it. Else the home frees it at its next turn, and needs no wake for that.
    fn end_taken(&self, id: OpId) {
        #[cfg(all(feature = "local", target_os = "linux"))]
        if crate::task::with_driver(id, |driver, id| driver.end_taken(&self.shared, id)) == Ok(true) {
            return;
        }
        self.shared.requests.post(id, Request::EndInterest);
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
        #[cfg(all(feature = "local", target_os = "linux"))]
        if self.consumer == Consumer::Home {
            return crate::coop::poll(cx, |cx| self.poll_home(cx, take));
        }
        crate::coop::poll(cx, |cx| self.take_with(cx, take))
    }

    /// The poll of a home operation: its driver takes the result in place, or keeps the waiter.
    /// With no driver of the home on this thread, outside a turn, it gives [`Gone`]: the context
    /// is refused, and the drop of the future ends the interest.
    #[cfg(all(feature = "local", target_os = "linux"))]
    fn poll_home<T>(
        &mut self,
        cx: &Context<'_>,
        take: impl FnOnce(i32, &mut Held, &Self) -> T,
    ) -> Poll<Result<T, Gone>> {
        let this = &*self;
        let polled = crate::task::with_driver_task(cx.waker(), take, |driver, task, take| {
            driver.poll_home(&this.shared, this.id, task, cx.waker(), |result, held| {
                take(result, held, this)
            })
        });
        match polled {
            Ok(Some(Poll::Ready(Ok(output)))) => {
                self.done = true;
                Poll::Ready(Ok(output))
            }
            Ok(Some(poll)) => poll,
            Ok(None) | Err(_) => Poll::Ready(Err(Gone)),
        }
    }
}

impl Drop for Completion {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        // The waiter of a home operation is at its driver, which the end of interest clears.
        #[cfg(all(feature = "local", target_os = "linux"))]
        let home = self.consumer == Consumer::Home;
        #[cfg(not(all(feature = "local", target_os = "linux")))]
        let home = false;
        if !home && let Some(entry) = self.entry() {
            entry.cancel_wait();
        }
        // On the thread of the home, outside a turn, the driver ends the interest now, and the
        // cancel needs no wake.
        #[cfg(all(feature = "local", target_os = "linux"))]
        {
            let cancel = self.cancel_on_drop;
            if crate::task::with_driver(self.id, |driver, id| driver.forgo_now(&self.shared, id, cancel)) == Ok(true) {
                return;
            }
        }
        self.shared.requests.post(self.id, Request::EndInterest);
        if self.cancel_on_drop {
            self.shared.requests.post(self.id, Request::Cancel);
            self.shared.wake();
        }
    }
}
