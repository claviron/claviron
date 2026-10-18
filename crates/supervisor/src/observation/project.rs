use std::time::Duration;

use claviron_orbit::OrbitEpoch;
use claviron_runtime::AppState;

use crate::control::WorkerControl;
use crate::lifecycle::BuildIdentity;
use crate::snapshot::{
    MasterRuntimeSnapshot, WorkerListSnapshot, WorkerOperation,
    WorkerOwnership, WorkerStatus,
};

pub(crate) async fn management_snapshot(
    control: &WorkerControl,
    state: &AppState,
) -> WorkerListSnapshot {
    let workers_expected = state.worker_count();
    // Heartbeat ages come from one more fold of the ring, the same fold the
    // monitor does; before boot loads a config there is no timeout to fold
    // with and no age to report.
    let heartbeat_ages = match control.config.load_full() {
        Some(config) => {
            control.observer.heartbeat_ages(config.heartbeat.timeout).await
        }
        None => Default::default(),
    };
    let workers = control
        .workers
        .read()
        .await
        .iter()
        .filter(|worker| worker.id.raw() <= workers_expected)
        .map(|worker| {
            let live = worker.phase().is_live();
            let owned = live.then_some(worker.process.as_ref()).flatten();
            let ownership = match owned {
                Some(process) if process.is_adopted() => {
                    WorkerOwnership::Adopted
                }
                Some(_) => WorkerOwnership::Spawned,
                None => WorkerOwnership::Unowned,
            };
            // The exact process in the slot: the handle this master owns,
            // or — for a slot it is claiming or has refused — the
            // worker-written presence that occupies the lane. That is the
            // PID an operator needs when boot is blocked; hiding it because
            // no handle is installed would hide the block's cause.
            let (process, uptime, incarnation) =
                match (owned, worker.phase().evidence()) {
                    (Some(process), _) => (
                        Some(process.identity()),
                        Some(process.age()),
                        worker.incarnation,
                    ),
                    (None, Some(presence)) => (
                        Some(presence.identity().process()),
                        Some(Duration::from_millis(
                            OrbitEpoch::now().as_unix_ms().saturating_sub(
                                presence.runtime_started_at_unix_ms(),
                            ),
                        )),
                        presence.identity().incarnation(),
                    ),
                    (None, None) => (None, None, worker.incarnation),
                };
            // The operation acting on this slot, by authority: the fleet
            // shutdown outranks a fleet-wide operation, which outranks the
            // slot's own. Once shutdown has latched, a slot whose stop or
            // restart is still running reports the shutdown — the operation
            // the master has told the worker about since.
            let operation = control
                .operations
                .fleet_shutdown()
                .or_else(|| control.operations.fleet_operation())
                .or_else(|| control.operations.slot_operation(worker.id))
                .map(|operation| WorkerOperation {
                    id: operation.id,
                    kind: operation.kind,
                    origin: operation.origin,
                    age: operation.started_at.elapsed(),
                });
            WorkerStatus {
                worker_id: worker.id.raw(),
                ownership,
                process,
                uptime,
                recovery_attempts: worker.recovery_attempts,
                incarnation,
                reload_revision: worker.reload_revision,
                phase: worker.phase().observed(),
                phase_age: worker.phase_age(),
                desired: worker.intent(),
                desired_age: worker.intent_age(),
                operation,
                heartbeat_age: heartbeat_ages.get(&worker.id).copied(),
                block_reason: worker.phase().block_reason(),
            }
        })
        .collect();
    let master = control.master().ok();
    WorkerListSnapshot {
        captured_at: std::time::SystemTime::now(),
        workers_expected,
        build: BuildIdentity::current(),
        master,
        master_runtime: master.map(|_| MasterRuntimeSnapshot {
            started_at_unix_ms: super::snapshot::unix_millis(
                state.runtime_started_at(),
            ),
            uptime: state.runtime_uptime(),
            reload_revision: state.reload_count() as u64,
        }),
        fleet_mode: control.fleet_mode(),
        workers,
    }
}
