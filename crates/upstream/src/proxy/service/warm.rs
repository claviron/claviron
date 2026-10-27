//! Provider-generation ownership for warm maintenance, separate from profile IO.
use std::future::Future;
use std::sync::{Arc, Mutex};

use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

#[derive(Default)]
pub(crate) struct Owner {
    current: Mutex<Option<Arc<Generation>>>,
}
struct Generation {
    cancellation: CancellationToken,
    tasks: TaskTracker,
}
pub(crate) struct Run {
    owner: Arc<Owner>,
    generation: Arc<Generation>,
}
impl Owner {
    pub(crate) fn start(
        self: &Arc<Self>,
        context: &CancellationToken,
    ) -> Result<Run, &'static str> {
        let mut current =
            self.current.lock().unwrap_or_else(|p| p.into_inner());
        if current.is_some() {
            return Err("upstream warm maintenance is already running");
        }
        let generation = Arc::new(Generation {
            cancellation: context.child_token(),
            tasks: TaskTracker::new(),
        });
        *current = Some(Arc::clone(&generation));
        Ok(Run { owner: Arc::clone(self), generation })
    }

    pub(crate) fn spawn(
        &self,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> bool {
        // Attachment is fenced against generation removal/close. Both trackers
        // are entered before either lifecycle owner can report completion.
        let current = self.current.lock().unwrap_or_else(|p| p.into_inner());
        let Some(generation) =
            current.as_ref().filter(|g| !g.cancellation.is_cancelled())
        else {
            return false;
        };
        let cancelled = generation.cancellation.clone();
        generation.tasks.spawn(async move {
            tokio::select! { biased; _ = cancelled.cancelled() => {}, _ = future => {} }
        });
        true
    }
}
impl Run {
    fn close(&self) {
        {
            let mut current =
                self.owner.current.lock().unwrap_or_else(|p| p.into_inner());
            if current
                .as_ref()
                .is_some_and(|active| Arc::ptr_eq(active, &self.generation))
            {
                current.take();
            }
        }
        self.generation.cancellation.cancel();
        self.generation.tasks.close();
    }
    pub(crate) async fn drain(self) {
        self.close();
        self.generation.tasks.wait().await;
    }
}
impl Drop for Run {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
#[path = "../../../tests/warm.rs"]
mod tests;
