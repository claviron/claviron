//! Owns Hyper's spawned H2 transport and request-writer tasks per origin.
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};
use tokio_util::task::TaskTracker;
#[derive(Clone)]
pub(super) struct OriginExecutor {
    shared: Arc<ExecutorState>,
}
struct ExecutorState {
    force: CancellationToken,
    tasks: TaskTracker,
}
pin_project_lite::pin_project! {
    // Keep Hyper's future in one field. An async select wrapper can retain
    // multiple copies in its generated state and cross Tokio's boxing threshold.
    struct OriginTask<F> {
        #[pin] future: F,
        #[pin] cancelled: WaitForCancellationFutureOwned,
    }
}
impl<F: Future> Future for OriginTask<F> {
    type Output = ();

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<()> {
        let this = self.project();
        // Preserve the original biased-select contract: force wins this poll.
        if this.cancelled.poll(cx).is_ready() {
            return Poll::Ready(());
        }
        this.future.poll(cx).map(|_| ())
    }
}
impl OriginExecutor {
    pub(super) fn new(
        transport: &crate::proxy::service::transport::OriginTransport
    ) -> Self {
        Self {
            shared: Arc::new(ExecutorState {
                force: transport.force.clone(),
                tasks: transport.io.clone(),
            }),
        }
    }
}
impl<F> hyper::rt::Executor<F> for OriginExecutor
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn execute(
        &self,
        future: F,
    ) {
        self.shared.tasks.spawn(OriginTask {
            future,
            cancelled: self.shared.force.clone().cancelled_owned(),
        });
    }
}
