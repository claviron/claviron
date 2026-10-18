//! Each service generation owns and drains every operation it admits.
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use claviron_orbit::placement::{Scope, Scoped};
use claviron_runtime::registry::{Component, ComponentOrder, Runnable};
use claviron_runtime::{AppState, RunContext};
use futures_util::StreamExt;
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use tokio::sync::Notify;

use crate::control::WorkerControl;
use crate::lifecycle::RejectionReason;

/// Admission state and accepted-but-not-yet-polled work, protected by one lock
/// so submission cannot race past a generation's decision to stop accepting.
#[derive(Default)]
struct Admission {
    running: bool,
    pending: VecDeque<BoxFuture<'static, ()>>,
}

/// Handoff from short command/event callbacks to a runtime-owned executor.
///
/// Stores accepted futures but starts no tasks itself. Its driver closes
/// admission and drains all handed-off work before returning. Operation scope
/// conflicts are decided by the fleet operation table, not by this queue.
#[derive(Default)]
pub(crate) struct OperationQueue {
    admission: Mutex<Admission>,
    changed: Notify,
}

impl OperationQueue {
    pub(crate) fn submit(
        &self,
        work: BoxFuture<'static, ()>,
    ) -> Result<(), RejectionReason> {
        let mut admission =
            self.admission.lock().unwrap_or_else(|e| e.into_inner());
        if !admission.running {
            return Err(RejectionReason::OperationRunnerStopped);
        }
        admission.pending.push_back(work);
        self.changed.notify_one();
        Ok(())
    }

    fn open(&self) {
        let mut admission =
            self.admission.lock().unwrap_or_else(|e| e.into_inner());
        assert!(
            !admission.running,
            "operation runner generations must not overlap"
        );
        admission.running = true;
    }

    fn take(
        &self,
        close: bool,
    ) -> VecDeque<BoxFuture<'static, ()>> {
        let mut admission =
            self.admission.lock().unwrap_or_else(|e| e.into_inner());
        if close {
            admission.running = false;
        }
        std::mem::take(&mut admission.pending)
    }
}

/// Master-side Runnable that executes accepted worker lifecycle operations.
///
/// One generation polls all active operation futures concurrently, keeping each
/// operation's ownership guard alive until completion. Management callbacks and
/// the monitor only submit work; they do not own its lifetime. Targeted stop
/// closes admission, then waits for every accepted operation without aborting it
/// or adding a deadline. A blocked drain can therefore block this service's stop,
/// and no accepted task is left behind for the next generation.
pub(crate) struct WorkerOperationRunner {
    control: Arc<WorkerControl>,
}

impl WorkerOperationRunner {
    pub(crate) fn new(control: Arc<WorkerControl>) -> Self {
        Self { control }
    }
}

impl Scoped for WorkerOperationRunner {
    fn scope(&self) -> Scope {
        Scope::Master
    }
}

impl Component<AppState> for WorkerOperationRunner {
    type Deps = ();

    fn name(&self) -> &'static str {
        "WorkerOperationRunner"
    }
    fn order(&self) -> ComponentOrder {
        ComponentOrder::new().after::<WorkerControl>()
    }
    fn as_runnable(
        self: Arc<Self>
    ) -> Option<Arc<dyn Runnable<AppState, Self::Deps>>> {
        Some(self)
    }

    fn as_scoped(&self) -> Option<&dyn Scoped> {
        Some(self)
    }
}

#[async_trait]
impl Runnable<AppState> for WorkerOperationRunner {
    async fn run(
        self: Arc<Self>,
        _deps: (),
        _: AppState,
        context: RunContext,
    ) -> claviron_runtime::registry::Result<()> {
        self.control.operation_queue.drive(context).await
    }
}

impl OperationQueue {
    pub(crate) async fn drive(
        &self,
        context: RunContext,
    ) -> claviron_runtime::registry::Result<()> {
        let queue = self;
        queue.open();
        let mut active = FuturesUnordered::new();
        loop {
            let stopping = context.is_cancelled();
            active.extend(queue.take(stopping));
            if stopping {
                // Admission closed under the same lock as submission. Every
                // accepted future (including one racing stop) is now ours.
                // There is deliberately no timeout and no abort here.
                while active.next().await.is_some() {}
                return Ok(());
            }
            tokio::select! {
                biased;
                _ = context.cancelled() => {},
                _ = queue.changed.notified() => {},
                _ = active.next(), if !active.is_empty() => {},
            }
        }
    }
}
