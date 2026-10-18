//! Fleet-shared daemon pool slot acquisition, renewal, and release.

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use claviron_orbit::OrbitStateExt;
use claviron_orbit::lock::{
    Lock, LockAcquire, LockKey, LockOwner, LockType, OrbitLock,
};
use claviron_runtime::{AppState, RunMode};
use futures_util::FutureExt;
use tokio::task::JoinSet;
use tokio::time::{Instant, MissedTickBehavior, interval_at};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::config::TaskConfig;
use crate::registry::{TaskDispatch, TaskScope};
use crate::runner::TaskRunner;

struct TaskPoolSlot;

impl LockType for TaskPoolSlot {
    const NAMESPACE: &'static str = "claviron.tasks.pool";
}

enum Coordinator {
    Local,

    Fleet { locks: OrbitLock, owner: LockOwner, lease_ttl: Duration },
}

enum Ownership {
    Local,

    Fleet(Lock),
}

impl Ownership {
    fn renew(
        &mut self,
        lease_ttl: Duration,
    ) -> Result<(), String> {
        match self {
            Self::Local => Ok(()),

            Self::Fleet(lock) => {
                if lock.renew(lease_ttl).map_err(|error| error.to_string())? {
                    Ok(())
                } else {
                    Err("pool slot lease expired before renewal".to_string())
                }
            }
        }
    }
}

struct ActiveSlot {
    ownership: Ownership,
    token: CancellationToken,
    retiring: bool,
}

/// One configured fleet-arbitrated task handed from `TaskService` to its
/// controller.
///
/// Two dispatches arrive here. `pool` leases up to `concurrency` daemon slots.
/// `one` leases exactly one slot and owns either its schedule or its daemon
/// body. Both are stable ownership, so they share one controller.
pub(crate) struct PoolTask {
    pub(super) runner: Arc<TaskRunner>,
    cfg: Arc<TaskConfig>,
    scope: TaskScope,
    dispatch: TaskDispatch,
}

impl PoolTask {
    pub(crate) fn new(
        runner: Arc<TaskRunner>,
        cfg: Arc<TaskConfig>,
        scope: TaskScope,
        dispatch: TaskDispatch,
    ) -> Self {
        Self { runner, cfg, scope, dispatch }
    }

    /// How many slots this task wants across the fleet.
    ///
    /// `one` means one, whatever `concurrency` says: a second slot would be a
    /// second owner of the same schedule, which is the thing `one` exists to
    /// prevent.
    fn desired_slots(&self) -> u8 {
        match self.dispatch {
            TaskDispatch::One => 1,
            _ => self.cfg.concurrency,
        }
    }
}

impl Coordinator {
    fn for_runtime(
        state: &AppState,
        lease_ttl: Duration,
    ) -> Result<Self, String> {
        if matches!(state.run_mode(), RunMode::Standalone) {
            return Ok(Self::Local);
        }

        {
            let fleet = state
                .fleet()
                .ok_or_else(|| "Orbit fleet is unavailable".to_string())?;
            let owner = format!(
                "{}:pid{}",
                state.run_mode().as_process_label(),
                std::process::id()
            );
            Ok(Self::Fleet {
                locks: OrbitLock::new(fleet)
                    .map_err(|error| error.to_string())?,
                owner: LockOwner::from(owner),
                lease_ttl,
            })
        }
    }

    fn acquire(
        &self,
        task_name: &str,
        slot: u8,
    ) -> Result<Option<Ownership>, String> {
        match self {
            Self::Local => Ok(Some(Ownership::Local)),

            Self::Fleet { locks, owner, lease_ttl, .. } => {
                let key = LockKey::new::<TaskPoolSlot>(format!(
                    "task:{task_name}:pool:{slot}"
                ));
                match locks
                    .try_acquire(&key, owner, *lease_ttl)
                    .map_err(|error| error.to_string())?
                {
                    LockAcquire::Acquired(lease) => Ok(Some(Ownership::Fleet(
                        Lock::new(locks.clone(), lease),
                    ))),
                    LockAcquire::Occupied(_) => Ok(None),
                }
            }
        }
    }

    /// Bound one eligible manager's acquisitions per reconciliation to roughly its
    /// fair share. Without this budget an early runtime could synchronously
    /// take every slot before peers join.
    fn acquire_budget(
        &self,
        desired: u8,
        eligible_runtimes: u16,
    ) -> usize {
        match self {
            Self::Local => {
                let _ = eligible_runtimes;
                desired as usize
            }

            Self::Fleet { .. } => usize::from(desired)
                .div_ceil(usize::from(eligible_runtimes).max(1)),
        }
    }
}

/// Run the process-local pool manager owned by `TaskService`.
pub(super) async fn run(
    task: PoolTask,
    state: AppState,
    controller_token: CancellationToken,
    lease_ttl: Duration,
) {
    let coordinator = match Coordinator::for_runtime(&state, lease_ttl) {
        Ok(coordinator) => coordinator,
        Err(err) => {
            error!(error = %err, "task pool manager unavailable");
            return;
        }
    };

    let reconcile_every = lease_ttl
        .checked_div(3)
        .unwrap_or(lease_ttl)
        .max(Duration::from_millis(1));
    let mut ticker =
        interval_at(Instant::now() + reconcile_every, reconcile_every);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let mut active = HashMap::<u8, ActiveSlot>::new();
    let mut workers = JoinSet::<(u8, bool)>::new();

    acquire_missing(&coordinator, &task, &state, &mut active, &mut workers);
    task.runner.set_owned_slots(active.len());
    update_metrics(task.runner.name(), state.run_mode(), active.len());

    loop {
        tokio::select! {
            _ = controller_token.cancelled() => break,
            _ = ticker.tick() => {
                renew_owned(&task, lease_ttl, &mut active);
                acquire_missing(&coordinator, &task, &state, &mut active, &mut workers);
                task.runner.set_owned_slots(active.len());
        update_metrics(task.runner.name(), state.run_mode(), active.len());
            }
            joined = workers.join_next(), if !workers.is_empty() => {
                match joined {
                    Some(Ok((slot, panicked))) => {
                        active.remove(&slot);
                        let task_name = task.runner.name();
                        if panicked {
                            error!(task = task_name, slot, "task pool slot panicked");
                        } else {
                            debug!(task = task_name, slot, "task pool slot stopped");
                        }
                    }
                    Some(Err(err)) => {
                        error!(error = %err, "task pool worker join failed");
                    }
                    None => {}
                }
                task.runner.set_owned_slots(active.len());
        update_metrics(task.runner.name(), state.run_mode(), active.len());
            }
        }
    }

    for slot in active.values_mut() {
        slot.retiring = true;
        slot.token.cancel();
    }

    while !workers.is_empty() {
        tokio::select! {
            _ = ticker.tick() => {
                renew_draining(&task, lease_ttl, &mut active);
            }
            joined = workers.join_next() => {
                match joined {
                    Some(Ok((slot, panicked))) => {
                        active.remove(&slot);
                        if panicked {
                            error!(
                                task = task.runner.name(),
                                slot,
                                "task pool slot panicked during shutdown"
                            );
                        }
                    }
                    Some(Err(err)) => error!(error = %err, "task pool shutdown join failed"),
                    None => {}
                }
            }
        }
    }

    // Ownership locks drop only after their child has stopped, so a peer
    // cannot take the slot while the old process is still shutting down.
    active.clear();
    task.runner.set_owned_slots(0);
    update_metrics(task.runner.name(), state.run_mode(), 0);
    info!(task = task.runner.name(), "task pool controller stopped");
}

fn renew_owned(
    task: &PoolTask,
    lease_ttl: Duration,
    active: &mut HashMap<u8, ActiveSlot>,
) {
    for (slot_number, active_slot) in active.iter_mut() {
        if active_slot.retiring {
            continue;
        }
        if let Err(err) = active_slot.ownership.renew(lease_ttl) {
            warn!(
                task = task.runner.name(),
                slot = slot_number,
                error = %err,
                "task pool slot lost; stopping local instance"
            );
            active_slot.retiring = true;
            active_slot.token.cancel();
        }
    }
}

/// Keep intentionally retiring slots leased until their local bodies have
/// stopped. Otherwise a graceful drain longer than the remaining lease could
/// let a peer acquire the slot while the old child is still alive.
fn renew_draining(
    task: &PoolTask,
    lease_ttl: Duration,
    active: &mut HashMap<u8, ActiveSlot>,
) {
    for (slot_number, active_slot) in active.iter_mut() {
        if let Err(err) = active_slot.ownership.renew(lease_ttl) {
            warn!(
                task = task.runner.name(),
                slot = slot_number,
                error = %err,
                "task pool slot lease was lost while draining"
            );
        }
    }
}

fn eligible_runtime_count(
    scope: TaskScope,
    state: &AppState,
) -> Result<u16, String> {
    use claviron_orbit::service::{FleetMemberRole, current_members};
    let members = current_members(state)?;
    Ok(members
        .iter()
        .filter(|member| match scope {
            TaskScope::Master => member.role == FleetMemberRole::Master,
            TaskScope::Workers => member.role != FleetMemberRole::Master,
            TaskScope::Fleet => true,
        })
        .count() as u16)
}

fn acquire_missing(
    coordinator: &Coordinator,
    task: &PoolTask,
    state: &AppState,
    active: &mut HashMap<u8, ActiveSlot>,
    workers: &mut JoinSet<(u8, bool)>,
) {
    let eligible_runtimes = match eligible_runtime_count(task.scope, state) {
        Ok(0) => return,
        Ok(count) => count,
        Err(error) => {
            debug!(task = task.runner.name(), %error, "task pool membership unavailable; deferring acquisition");
            return;
        }
    };
    let desired = task.desired_slots();
    let mut acquire_budget =
        coordinator.acquire_budget(desired, eligible_runtimes);
    for slot_number in 1..=desired {
        if acquire_budget == 0 {
            break;
        }
        if active.contains_key(&slot_number) {
            continue;
        }

        let ownership = match coordinator
            .acquire(task.runner.name(), slot_number)
        {
            Ok(Some(ownership)) => ownership,
            Ok(None) => continue,
            Err(err) => {
                warn!(
                    task = task.runner.name(),
                    slot = slot_number,
                    error = %err,
                    "task pool slot lock failed; retrying on next reconciliation"
                );
                continue;
            }
        };
        acquire_budget -= 1;

        let token = task.runner.token().child_token();
        let task_token = token.clone();
        let task_runner = Arc::clone(&task.runner);
        let task_state = state.clone();
        let task_cfg = Arc::clone(&task.cfg);
        let scope = task.scope;
        let dispatch = task.dispatch;
        workers.spawn(async move {
            // A daemon owns its body for the life of the slot. A schedule owns
            // the trigger and fires a bounded body per occurrence.
            let slot = async move {
                if task_cfg.timing.is_daemon() {
                    task_runner
                        .run_daemon_slot(
                            slot_number,
                            task_state,
                            task_cfg,
                            scope,
                            dispatch,
                            task_token,
                        )
                        .await;
                } else {
                    task_runner
                        .run_scheduled_slot(
                            slot_number,
                            task_state,
                            task_cfg,
                            scope,
                            task_token,
                        )
                        .await;
                }
            };
            let result = AssertUnwindSafe(slot).catch_unwind().await;
            (slot_number, result.is_err())
        });
        active.insert(
            slot_number,
            ActiveSlot { ownership, token, retiring: false },
        );
        debug!(
            task = task.runner.name(),
            slot = slot_number,
            "task pool slot locked"
        );
    }
}

fn update_metrics(
    task_name: &str,
    run_mode: RunMode,
    local_slots: usize,
) {
    #[cfg(feature = "metrics")]
    {
        let role = crate::metrics::role_label_from_run_mode(run_mode);
        crate::metrics::set_workers_total(task_name, &role, local_slots as u64);
        crate::metrics::set_workers_healthy(
            task_name,
            &role,
            local_slots as u64,
        );
    }
    #[cfg(not(feature = "metrics"))]
    let _ = (task_name, run_mode, local_slots);
}

#[cfg(test)]
#[path = "../../tests/pool/controller.rs"]
mod tests;
