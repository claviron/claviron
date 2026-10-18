use std::sync::Arc;

use async_trait::async_trait;
use claviron_orbit::OrbitStateExt;
use claviron_orbit::placement::{Scope, Scoped};
use claviron_orbit::service::FleetMembership;
use claviron_orbit::shm::{
    ResourceActivity, ShmActivitySource, ShmResourceDescriptor,
};
use claviron_runtime::registry::Component;
use claviron_runtime::{AppState, registry};

use crate::control::WorkerControl;
use crate::heartbeat::{HeartbeatProvider, HeartbeatRecord};
use crate::membership::WorkerServiceMembership;
use crate::observer::HeartbeatObserver;
use crate::publisher::WorkerSnapshotPublisher;
use crate::{manager, ownership, reload, server, shutdown, startup};

/// Supervisor-owned proof of whether any exact worker process from the shared
/// fleet is still alive. Presence carries PID plus process start stamp, so a
/// recycled PID cannot authorize preservation or destruction by accident.
struct SupervisorShmActivity {
    policy: crate::config::provider::SharedPolicy,
}

#[async_trait]
impl ShmActivitySource for SupervisorShmActivity {
    async fn activity(
        &self,
        state: &AppState,
        _descriptor: ShmResourceDescriptor,
    ) -> registry::Result<ResourceActivity> {
        let fleet = state.fleet().ok_or_else(|| {
            registry::Error::msg(
                "Orbit fleet is unavailable for SHM activity observation",
            )
        })?;
        let exact_process_is_alive = || {
            (1..=state.worker_capacity()).any(|worker_id| {
                crate::presence::latest_worker_presence(
                    &fleet,
                    claviron_runtime::WorkerId::new(worker_id),
                )
                .is_some_and(|presence| presence.exact_process_alive())
            })
        };
        if exact_process_is_alive() {
            return Ok(ResourceActivity::Active);
        }

        // Only a lane that has beaten but names no process needs watching. A
        // lane with a presence record is settled by the kernel: its exact
        // process (PID plus start stamp) was just proven dead above. A lane
        // that never beat has nobody behind it. Watching those too cost every
        // clean restart a full heartbeat interval of master boot for nothing,
        // which is the same rule boot reconciliation already follows
        // (`lifecycle::reconcile`: presence decides, the lane watch is only
        // for a lane without one).
        let heartbeat_lane =
            |worker_id: u8| claviron_orbit::NodeId::new(u16::from(worker_id));
        let unnamed = (1..=state.worker_capacity())
            .filter(|&worker_id| {
                crate::presence::latest_worker_presence(
                    &fleet,
                    claviron_runtime::WorkerId::new(worker_id),
                )
                .is_none()
            })
            .map(|worker_id| {
                (
                    worker_id,
                    fleet.lane_head::<HeartbeatRecord>(heartbeat_lane(
                        worker_id,
                    )),
                )
            })
            .filter(|&(_, head)| head != 0)
            .collect::<Vec<_>>();
        if unnamed.is_empty() {
            return Ok(ResourceActivity::Quiescent);
        }

        let Some(policy) = self.policy.load_full() else {
            return Ok(ResourceActivity::Unknown);
        };
        tokio::time::sleep(
            policy.heartbeat.interval
                + crate::lifecycle::reconcile::LANE_WATCH_MARGIN,
        )
        .await;

        if exact_process_is_alive() {
            return Ok(ResourceActivity::Active);
        }
        let moved = unnamed.into_iter().any(|(worker_id, head)| {
            fleet.lane_head::<HeartbeatRecord>(heartbeat_lane(worker_id))
                != head
        });
        Ok(if moved {
            ResourceActivity::Active
        } else {
            ResourceActivity::Quiescent
        })
    }
}

impl Component<AppState> for SupervisorShmActivity {
    type Deps = ();

    fn as_scoped(&self) -> Option<&dyn Scoped> {
        Some(self)
    }
    fn name(&self) -> &'static str {
        "SupervisorShmActivity"
    }
}
impl Scoped for SupervisorShmActivity {
    fn scope(&self) -> Scope {
        Scope::Master
    }
}

pub fn register(state: &AppState) -> registry::Result<()> {
    if state.is_standalone() {
        #[cfg(feature = "metrics")]
        crate::metrics::register_standalone(state)?;
    } else {
        register_supervised(state)?;
    }
    startup::install_events(state)?;
    ownership::install_events(state)?;
    reload::install_events(state)?;
    server::install_events(state)?;
    shutdown::install_events(state)?;
    if !state.is_standalone() {
        state.registry_ref().insert(Arc::new(WorkerServiceMembership))?;
        state
            .registry_ref()
            .bind_dyn::<dyn FleetMembership, WorkerServiceMembership>(
                |source| source,
            )?;
    }
    Ok(())
}

fn register_supervised(state: &AppState) -> registry::Result<()> {
    let registry = state.registry_ref();
    register_observation_shm(state)?;
    claviron_orbit::shm::register_reusable_ring::<
        crate::lifecycle::MasterGenerationRecord,
    >(state, "MasterGenerationShm", "master-generation-ring")?;
    let observer = Arc::new(HeartbeatObserver::new());
    let supervisor = Arc::new(WorkerControl::new(observer.clone()));
    registry.register(Arc::new(SupervisorShmActivity {
        policy: supervisor.config.clone(),
    }))?;
    registry.bind_dyn::<dyn ShmActivitySource, SupervisorShmActivity>(
        |source| source,
    )?;
    registry.register(Arc::new(
        crate::config::provider::SupervisorPolicy::new(
            supervisor.config.clone(),
            state,
        )?,
    ))?;
    registry.register(Arc::new(HeartbeatProvider::for_state(state)?))?;
    state
        .bind_config_environment::<crate::config::provider::SupervisorPolicy>(
        )?;
    state.bind_config_environment::<HeartbeatProvider>()?;
    registry.register(Arc::new(
        crate::management::worker_reload::WorkerReloadRunner::default(),
    ))?;

    registry.register(observer)?;
    registry.register(Arc::new(
        crate::lifecycle::master::MasterIdentityProvider::new(
            supervisor.clone(),
        ),
    ))?;
    registry.register(supervisor.clone())?;
    registry.register(Arc::new(
        crate::management::configuration::WorkerConfigurationReload::new(
            supervisor.clone(),
        ),
    ))?;
    registry.register(Arc::new(
        crate::process::observer::WorkerExitObserver::new(supervisor.clone()),
    ))?;
    registry.register(Arc::new(
        crate::management::runner::WorkerOperationRunner::new(
            supervisor.clone(),
        ),
    ))?;
    registry.register(Arc::new(
        crate::lifecycle::reconciler::WorkerReconciler::new(supervisor.clone()),
    ))?;
    registry.register(Arc::new(
        crate::monitor::service::WorkerMonitor::new(supervisor.clone()),
    ))?;
    claviron_orbit::shm::register_reusable_ring::<
        crate::snapshot::WorkerSnapshotRecord,
    >(state, "WorkerSnapshotShm", "worker-snapshot-ring")?;
    registry.register(Arc::new(WorkerSnapshotPublisher::for_state(
        supervisor.clone(),
        state,
    )?))?;
    state.bind_config_environment::<WorkerSnapshotPublisher>()?;
    manager::register(state, Some(supervisor.clone()))?;
    #[cfg(feature = "metrics")]
    crate::metrics::register_supervised(state, supervisor)?;

    Ok(())
}

fn register_observation_shm(state: &AppState) -> registry::Result<()> {
    claviron_orbit::shm::register_reusable_ring::<HeartbeatRecord>(
        state,
        "HeartbeatShm",
        "worker-heartbeat-ring",
    )?;
    claviron_orbit::shm::register_reusable_ring::<
        crate::presence::PresenceRecord,
    >(state, "WorkerPresenceShm", "worker-presence-ring")?;
    Ok(())
}

#[cfg(test)]
#[path = "../tests/registry.rs"]
mod tests;
