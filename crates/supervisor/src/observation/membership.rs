//! Bind process ownership to service observation without making Orbit depend
//! on the supervisor or inferring membership from silent service publishers.

use claviron_orbit::service::{FleetMember, FleetMemberRole, FleetMembership};
use claviron_runtime::AppState;

use crate::{WorkerLifecycle, WorkerListSnapshot, WorkerManager};

/// Non-Runnable adapter from fresh supervisor snapshots to Orbit's service
/// membership contract, registered in master and worker processes.
///
/// Tells collectors which processes are expected to report services without
/// creating another process table or treating missing service reports as death.
/// Invalid or stale snapshots fail closed; reading membership never resizes or
/// starts workers.
pub(crate) struct WorkerServiceMembership;

impl FleetMembership for WorkerServiceMembership {
    fn members(
        &self,
        state: &AppState,
    ) -> Result<Vec<FleetMember>, String> {
        let snapshot = WorkerManager::new(state)
            .and_then(|manager| manager.list())
            .map_err(|error| error.to_string())?;
        from_snapshot(&snapshot)
    }
}

fn from_snapshot(
    snapshot: &WorkerListSnapshot
) -> Result<Vec<FleetMember>, String> {
    if snapshot.workers.len() != usize::from(snapshot.workers_expected) {
        return Err("worker membership snapshot is incomplete".into());
    }
    let mut members =
        vec![FleetMember { node_id: 0, role: FleetMemberRole::Master }];
    for (index, worker) in snapshot.workers.iter().enumerate() {
        if usize::from(worker.worker_id) != index + 1 {
            return Err(
                "worker membership snapshot has inconsistent slot identities"
                    .into(),
            );
        }
        if matches!(
            worker.lifecycle(),
            WorkerLifecycle::Stopped
                | WorkerLifecycle::Exited
                | WorkerLifecycle::Crashed
        ) {
            if worker.process.is_some() {
                return Err(
                    "terminal worker membership still has a live PID".into()
                );
            }
            continue;
        }
        // Starting/reloading/stopping remain expected: a temporary absence is
        // not proof of termination. Only the supervisor can exclude a slot.
        // The current single-family supervisor assigns node id == worker id.
        members.push(FleetMember {
            node_id: u16::from(worker.worker_id),
            role: FleetMemberRole::Worker,
        });
    }
    Ok(members)
}

#[cfg(test)]
#[path = "../../tests/observation/membership.rs"]
mod tests;
