use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::runtime::Handle;
use tokio::sync::{Mutex as AsyncMutex, watch};
use tokio::task::{AbortHandle, JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use super::thread::ThreadExecution;
use crate::registry::{Error, Result};
use crate::{ExecutionFuture, PreparedExecution, RunContext};

/// Owned reactor and submitted jobs, retained by a shared domain or dedicated root.
#[derive(Clone, Default)]
pub(super) struct ResidentExecution(Arc<Mutex<State>>);

#[derive(Default)]
struct State {
    closed: bool,
    owner: Option<Arc<Owner>>,
    jobs: Vec<Arc<Job>>,
}

struct Owner {
    thread: Arc<ThreadExecution>,
    ready: watch::Receiver<Option<Handle>>,
    shutdown: CancellationToken,
}

impl Owner {
    fn start() -> Result<Arc<Self>> {
        let (ready, receiver) = watch::channel(None);
        let shutdown = CancellationToken::new();
        let stopped = shutdown.clone();
        let thread = ThreadExecution::start(Box::pin(async move {
            ready.send_replace(Some(Handle::current()));
            stopped.cancelled().await;
            Ok(())
        }))?;
        Ok(Arc::new(Self { thread, ready: receiver, shutdown }))
    }

    async fn handle(&self) -> Result<Handle> {
        let mut ready = self.ready.clone();
        tokio::select! {
            result = ready.wait_for(|handle| handle.is_some()) => {
                result.map_err(|_| Error::msg("current-thread executor exited before readiness"))?
                    .as_ref().cloned()
                    .ok_or_else(|| Error::msg("current-thread executor has no handle"))
            }
            () = self.thread.wait_finished() => {
                Err(Error::msg("current-thread executor exited before readiness"))
            }
        }
    }
}

impl Drop for Owner {
    fn drop(&mut self) {
        // Abrupt lifecycle abandonment still releases this owner's reactor.
        // Normal finalization signals and joins it explicitly.
        self.shutdown.cancel();
    }
}

struct Job {
    task: AsyncMutex<Option<JoinHandle<Result<()>>>>,
    abort: AbortHandle,
    aborted: AtomicBool,
}

impl Job {
    fn start(
        handle: &Handle,
        task: ExecutionFuture,
    ) -> Arc<Self> {
        let task = handle.spawn(task);
        let abort = task.abort_handle();
        Arc::new(Self {
            task: AsyncMutex::new(Some(task)),
            abort,
            aborted: AtomicBool::new(false),
        })
    }

    async fn join(&self) -> Result<()> {
        let mut task = self.task.lock().await;
        let Some(pending) = task.as_mut() else {
            // The submitting future already received this job's outcome.
            return Ok(());
        };
        // Borrow the handle: cancelling this waiter must not detach the job.
        let joined = pending.await;
        task.take();
        match joined {
            Err(error)
                if error.is_cancelled()
                    && self.aborted.load(Ordering::Acquire) =>
            {
                Ok(())
            }
            joined => joined?,
        }
    }
}

struct AbortOnDrop(Option<Arc<Job>>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if let Some(job) = &self.0 {
            job.aborted.store(true, Ordering::Release);
            job.abort.abort();
        }
    }
}

impl PreparedExecution for ResidentExecution {
    fn execute(
        &self,
        task: ExecutionFuture,
        context: RunContext,
    ) -> ExecutionFuture {
        self.submit(task, context, true)
    }

    fn execute_child(
        &self,
        task: ExecutionFuture,
        context: RunContext,
    ) -> ExecutionFuture {
        self.submit(task, context, false)
    }
}

impl ResidentExecution {
    fn submit(
        &self,
        task: ExecutionFuture,
        context: RunContext,
        start_owner: bool,
    ) -> ExecutionFuture {
        let execution = self.clone();
        Box::pin(async move {
            let owner = {
                let mut state = execution.0.lock().unwrap();
                if state.closed {
                    return Err(Error::msg(
                        "current-thread execution domain is finalized",
                    ));
                }
                if state.owner.is_none() {
                    if !start_owner {
                        return Err(Error::msg(
                            "current-thread child has no root execution owner",
                        ));
                    }
                    state.owner = Some(Owner::start()?);
                }
                state.owner.as_ref().unwrap().clone()
            };
            let handle = owner.handle().await?;
            let job = {
                let mut state = execution.0.lock().unwrap();
                if state.closed {
                    return Err(Error::msg(
                        "current-thread execution domain is finalized",
                    ));
                }
                let job = Job::start(
                    &handle,
                    Box::pin(
                        async move {
                            let result = task.await;
                            drop(context);
                            result
                        }
                        .in_current_span(),
                    ),
                );
                state.jobs.push(job.clone());
                job
            };
            let mut guard = AbortOnDrop(Some(job.clone()));
            let outcome = job.join().await;
            execution.remove(&job);
            guard.0 = None;
            outcome
        })
    }
    fn remove(
        &self,
        job: &Arc<Job>,
    ) {
        self.0.lock().unwrap().jobs.retain(|owned| !Arc::ptr_eq(owned, job));
    }

    pub(super) async fn drain(&self) -> Result<()> {
        let jobs = self.0.lock().unwrap().jobs.clone();
        let mut failure = None;
        for job in jobs {
            if let Err(error) = job.join().await {
                tracing::error!(%error, "current-thread job failed during drain");
                failure.get_or_insert(error);
            }
            self.remove(&job);
        }
        failure.map_or(Ok(()), Err)
    }

    pub(super) async fn finalize(&self) -> Result<()> {
        let owner = {
            let mut state = self.0.lock().unwrap();
            state.closed = true;
            state.owner.clone()
        };
        let drained = self.drain().await;
        if let Some(owner) = owner {
            owner.shutdown.cancel();
            let joined = owner.thread.join().await;
            self.0.lock().unwrap().owner = None;
            drained.and(joined)
        } else {
            drained
        }
    }
}
