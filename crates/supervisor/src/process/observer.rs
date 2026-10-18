//! Poll shared kernel-exit observations within one restartable generation.
use std::sync::Arc;

use async_trait::async_trait;
use claviron_orbit::placement::{Scope, Scoped};
use claviron_runtime::registry::{Component, ComponentOrder, Runnable};
use claviron_runtime::{AppState, RunContext};
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;

use crate::control::WorkerControl;

/// Master-side Runnable that polls shared kernel-exit observations for tracked
/// worker identities and wakes monitoring when an exact process disappears.
///
/// It replaces detached per-process watcher tasks with one runtime-owned future.
/// The fleet retains the handles; lifecycle actions still own signalling,
/// reaping and exit classification. Stopping this service stops only its polling,
/// not the workers or other waiters sharing an exit observation. Restart rebuilds
/// its pending observations from the current fleet table.
pub(crate) struct WorkerExitObserver {
    control: Arc<WorkerControl>,
}
impl WorkerExitObserver {
    pub(crate) fn new(control: Arc<WorkerControl>) -> Self {
        Self { control }
    }
}
impl Scoped for WorkerExitObserver {
    fn scope(&self) -> Scope {
        Scope::Master
    }
}

impl Component<AppState> for WorkerExitObserver {
    type Deps = ();

    fn name(&self) -> &'static str {
        "WorkerExitObserver"
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
impl Runnable<AppState> for WorkerExitObserver {
    async fn run(
        self: Arc<Self>,
        _deps: (),
        _: AppState,
        context: RunContext,
    ) -> claviron_runtime::registry::Result<()> {
        loop {
            // Subscribe before collecting so an installed process cannot fall
            // between the inventory read and the next wakeup.
            let changed = self.control.changed.notified();
            let mut exits = self
                .control
                .workers
                .read()
                .await
                .iter()
                .filter_map(|row| row.process.as_ref())
                .filter(|process| !process.has_exited())
                .map(|process| {
                    let exit = process.exit();
                    async move { exit.exited().await }
                })
                .collect::<FuturesUnordered<_>>();
            tokio::select! {
                biased;
                _ = context.cancelled() => return Ok(()),
                _ = changed => {},
                _ = exits.next(), if !exits.is_empty() => {},
            }
        }
    }
}
