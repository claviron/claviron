use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use tokio::sync::watch;

use crate::ExecutionFuture;
use crate::registry::{Error, Result};

struct Completion(watch::Sender<bool>);

impl Completion {
    fn new() -> Self {
        Self(watch::channel(false).0)
    }
    fn finish(&self) {
        self.0.send_replace(true);
    }
    async fn wait(&self) {
        let mut receiver = self.0.subscribe();
        receiver
            .wait_for(|done| *done)
            .await
            .expect("completion owner remains alive");
    }
}

pub(super) struct ThreadExecution {
    thread: Mutex<Option<JoinHandle<Result<()>>>>,
    finished: Completion,
    joined: Completion,
    outcome: Mutex<Option<Result<()>>>,
}

impl ThreadExecution {
    pub(super) fn start(task: ExecutionFuture) -> Result<Arc<Self>> {
        let execution = Arc::new(Self {
            thread: Mutex::new(None),
            finished: Completion::new(),
            joined: Completion::new(),
            outcome: Mutex::new(None),
        });
        let owned = execution.clone();
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        let thread = std::thread::Builder::new()
            .name("claviron-current-thread".to_owned())
            .spawn(move || {
                tracing::dispatcher::with_default(&dispatch, || {
                    let _completion = FinishOnDrop(owned.clone());
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()?;
                    runtime.block_on(async {
                        let task = tokio::spawn(task);
                        task.await?
                    })
                })
            })?;
        *execution.thread.lock().unwrap() = Some(thread);
        Ok(execution)
    }

    pub(super) async fn wait_finished(&self) {
        self.finished.wait().await;
    }

    pub(super) async fn join(self: &Arc<Self>) -> Result<()> {
        self.finished.wait().await;
        let thread = self.thread.lock().unwrap().take();
        if let Some(thread) = thread {
            let execution = self.clone();
            // Reap only after runtime teardown. The blocking pool never hosts
            // the long-running executor or its listener/connection tasks.
            tokio::task::spawn_blocking(move || {
                let outcome = thread.join().unwrap_or_else(|panic| {
                    let message = panic
                        .downcast_ref::<String>()
                        .map(String::as_str)
                        .or_else(|| panic.downcast_ref::<&str>().copied())
                        .unwrap_or("non-string panic payload");
                    Err(Error::msg(format!(
                        "current-thread executor panicked: {message}"
                    )))
                });
                *execution.outcome.lock().unwrap() = Some(outcome);
                execution.joined.finish();
            })
            .await?;
        } else {
            self.joined.wait().await;
        }
        self.outcome.lock().unwrap().take().ok_or_else(|| {
            Error::msg("current-thread execution result was already consumed")
        })?
    }
}

struct FinishOnDrop(Arc<ThreadExecution>);

impl Drop for FinishOnDrop {
    fn drop(&mut self) {
        self.0.finished.finish();
    }
}
