//! Shared joins for task-service lifecycle barriers; dropping a waiter never
//! cancels the execution it observes.

use std::time::Duration;

use futures_util::FutureExt;
use futures_util::future::{BoxFuture, Shared, join_all};
use tokio::task::{AbortHandle, JoinHandle};

pub(crate) struct Completion {
    abort: AbortHandle,
    joined: Shared<BoxFuture<'static, ()>>,
}

impl Completion {
    pub(crate) fn new(handle: JoinHandle<()>) -> Self {
        Self {
            abort: handle.abort_handle(),
            joined: async move {
                let _ = handle.await;
            }
            .boxed()
            .shared(),
        }
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.abort.is_finished()
    }

    pub(crate) fn waiter(&self) -> BoxFuture<'static, ()> {
        self.joined.clone().boxed()
    }

    #[cfg(test)]
    pub(crate) fn abort(&self) {
        self.abort.abort();
    }
}

pub(crate) async fn wait(
    completions: Vec<BoxFuture<'static, ()>>,
    timeout: Option<Duration>,
) -> bool {
    let joined = join_all(completions);
    match timeout {
        Some(duration) => tokio::time::timeout(duration, joined).await.is_ok(),
        None => {
            joined.await;
            true
        }
    }
}

#[cfg(test)]
#[path = "../tests/completion.rs"]
mod tests;
