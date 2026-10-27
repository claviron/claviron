//! Task-scoped lifecycle ownership for pool controllers.
//!
//! The lease loop remains in the parent module. This type owns the local
//! controller generations so task reload can drain one pool task without
//! cancelling unrelated pool daemons and their leases.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::completion::Completion;
use claviron_helpers::sync::LockUnpoisoned;
use claviron_runtime::AppState;
use futures_util::future::BoxFuture;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::controller::{PoolTask, run};
use crate::process::ReloadAction;
use crate::runner::{StopCause, TaskRunner};
use crate::snapshot::TaskLifecycle;

struct ActivePoolController {
    runner: Arc<TaskRunner>,
    cancel: CancellationToken,
    handle: Completion,
}

/// Owns one local controller generation per configured pool task.
///
/// Controllers remain independent even though they use the same Orbit lock
/// table. Reload can therefore stop, drain, and replace one task's slots while
/// every unrelated task keeps both its child processes and leases.
#[derive(Default)]
pub(crate) struct PoolReloader {
    active: Mutex<HashMap<String, ActivePoolController>>,
}

impl PoolReloader {
    pub(crate) fn start(
        &self,
        task: PoolTask,
        state: AppState,
        lease_ttl: Duration,
    ) -> bool {
        let name = task.runner.name().to_string();
        let runner = Arc::clone(&task.runner);
        let mut active = self.active.lock_unpoisoned();

        if let Some(current) = active.get(&name) {
            if !current.handle.is_finished() || current.runner.is_running() {
                warn!(
                    task = name,
                    "task pool controller is already active; skipping duplicate"
                );
                return false;
            }
            active.remove(&name);
        }

        // Reset only after proving the prior controller generation no longer
        // owns the runner's cancellation token.
        runner.reset_token();
        runner.begin_generation();
        let cancel = CancellationToken::new();
        let controller_cancel = cancel.clone();
        let handle = Completion::new(tokio::spawn(async move {
            run(task, state, controller_cancel, lease_ttl).await;
        }));
        active.insert(name, ActivePoolController { runner, cancel, handle });
        true
    }

    pub(crate) fn cancel_all(
        &self,
        cause: StopCause,
    ) -> usize {
        let active = self.active.lock_unpoisoned();
        for controller in active.values() {
            // Publish the cause before the controller cancels its slot tokens.
            controller.runner.cancel_for(cause);
            controller.cancel.cancel();
        }
        active.len()
    }

    pub(crate) fn begin_reload(
        &self,
        targets: &BTreeMap<String, ReloadAction>,
    ) -> usize {
        let active = self.active.lock_unpoisoned();
        let mut count = 0;
        for (name, action) in targets {
            let Some(controller) = active.get(name) else {
                continue;
            };
            // The controller keeps renewing each owned lease until run()
            // observes cancellation, drains the corresponding slot, and drops
            // ownership after the task body has stopped.
            controller.runner.cancel_for_reload(*action);
            controller.cancel.cancel();
            count += 1;
        }
        count
    }

    pub(crate) fn completions(
        &self,
        targets: Option<&BTreeMap<String, ReloadAction>>,
    ) -> Vec<BoxFuture<'static, ()>> {
        self.active
            .lock_unpoisoned()
            .iter()
            .filter(|(name, _)| {
                targets.is_none_or(|targets| targets.contains_key(*name))
            })
            .map(|(_, controller)| controller.handle.waiter())
            .collect()
    }

    pub(crate) fn finish_reload(
        &self,
        targets: &BTreeMap<String, ReloadAction>,
    ) {
        self.active
            .lock_unpoisoned()
            .retain(|name, _| !targets.contains_key(name));
    }

    pub(crate) fn running_count(&self) -> usize {
        self.active
            .lock()
            .unwrap()
            .values()
            .filter(|controller| {
                !controller.handle.is_finished()
                    || controller.runner.is_running()
            })
            .count()
    }

    pub(crate) fn runner(
        &self,
        name: &str,
    ) -> Option<Arc<TaskRunner>> {
        self.active
            .lock_unpoisoned()
            .get(name)
            .map(|controller| Arc::clone(&controller.runner))
    }

    pub(crate) fn snapshot(
        &self,
        name: &str,
    ) -> Option<(TaskLifecycle, Arc<TaskRunner>)> {
        self.active.lock_unpoisoned().get(name).map(|controller| {
            let lifecycle = if controller.cancel.is_cancelled() {
                TaskLifecycle::Stopping
            } else if controller.handle.is_finished()
                && !controller.runner.is_running()
            {
                TaskLifecycle::Finished
            } else {
                TaskLifecycle::Active
            };
            (lifecycle, Arc::clone(&controller.runner))
        })
    }
}
