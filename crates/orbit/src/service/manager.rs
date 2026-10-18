use std::sync::Arc;

use claviron_runtime::events::Event;
use claviron_runtime::{
    AppState, ServiceManager, ServiceManagerError, ServiceStartPolicy, registry,
};

use super::publisher::ServiceSnapshotPublisher;
use super::snapshot::{
    self, FleetServiceListSnapshot, FleetServiceStatusSnapshot,
};
use super::{
    FleetMember, FleetMemberRole, FleetMembership, SNAPSHOT_PUBLISHER_NAME,
    ServiceControlRegistration,
};
use crate::{
    EVENT_PAYLOAD_MAX, FleetEventCodec, FleetEventRegistry, OrbitStateExt,
};

const SERVICE_COMMAND_TOPIC: &str = "claviron.services.command.v1";
const ORBIT_EVENT_HEADER_BYTES: usize = 12;
const SERVICE_COMMAND_HEADER_BYTES: usize = 1;
const MAX_SERVICE_NAME_BYTES: usize = EVENT_PAYLOAD_MAX
    - ORBIT_EVENT_HEADER_BYTES
    - SERVICE_COMMAND_TOPIC.len()
    - SERVICE_COMMAND_HEADER_BYTES;

#[derive(Debug, thiserror::Error)]
pub enum FleetServiceManagerError {
    #[error("Continuo ServiceManager is not registered")]
    LocalManagerUnavailable,
    #[error("fleet service control is not registered")]
    ControlUnavailable,
    #[error("service name cannot be empty")]
    EmptyServiceName,
    #[error(
        "service name is too long for the Orbit command frame: {actual} bytes, max {max}"
    )]
    ServiceNameTooLong { actual: usize, max: usize },
    #[error(
        "runnable service '{0}' is not registered on any reporting fleet member"
    )]
    ServiceNotFound(String),
    #[error(
        "runnable service '{0}' is not reloadable on every registered fleet placement"
    )]
    ServiceNotReloadable(String),
    #[error(
        "runnable service '{0}' is unavailable on at least one registered fleet placement"
    )]
    ServiceUnavailable(String),
    #[error(
        "service '{name}' cannot be {action} through fleet management: {reason}"
    )]
    ContinuityProtected {
        name: String,
        action: &'static str,
        reason: &'static str,
    },
    #[error("service management is unavailable during shutdown")]
    ShuttingDown,
    #[error("Orbit fleet is unavailable for service snapshots")]
    FleetUnavailable,
    #[error("service snapshot publisher is unavailable")]
    SnapshotPublisherUnavailable,
    #[error("service snapshot failed: {0}")]
    Snapshot(String),
    #[error(transparent)]
    Local(#[from] ServiceManagerError),
}

pub type FleetServiceManagerResult<T> = Result<T, FleetServiceManagerError>;

/// Fleet-facing adapter over the process-local Continuo `ServiceManager`.
///
/// The local manager remains authoritative. Supervised commands are published
/// as typed Orbit events and each receiving process applies them to its own
/// registered runnable set. Submission reports acceptance; callers observe
/// convergence through [`Self::list`].
#[derive(Clone)]
pub struct FleetServiceManager {
    state: AppState,
    local: Arc<ServiceManager>,
}

impl FleetServiceManager {
    pub fn new(state: &AppState) -> FleetServiceManagerResult<Self> {
        let local = state
            .registry_ref()
            .resolve::<ServiceManager>()
            .ok_or(FleetServiceManagerError::LocalManagerUnavailable)?;
        if state
            .registry_ref()
            .resolve::<ServiceControlRegistration>()
            .is_none()
        {
            return Err(FleetServiceManagerError::ControlUnavailable);
        }
        Ok(Self { state: state.clone(), local })
    }

    pub async fn list(
        &self
    ) -> FleetServiceManagerResult<FleetServiceListSnapshot> {
        let local = self.local_snapshot().await?;
        if self.state.is_standalone() {
            return Ok(local);
        }

        let fleet = self
            .state
            .fleet()
            .ok_or(FleetServiceManagerError::FleetUnavailable)?;
        let publisher = self
            .state
            .registry_ref()
            .resolve::<ServiceSnapshotPublisher>()
            .ok_or(FleetServiceManagerError::SnapshotPublisherUnavailable)?;
        snapshot::publish(&fleet, &local)
            .map_err(FleetServiceManagerError::Snapshot)?;
        let members = self.expected_members(fleet.fleet_capacity())?;
        Ok(snapshot::collect(
            &fleet,
            local.captured_at,
            &members,
            publisher.policy().max_age(),
        ))
    }

    fn expected_members(
        &self,
        capacity: u16,
    ) -> FleetServiceManagerResult<Vec<FleetMember>> {
        let members = match self
            .state
            .registry_ref()
            .resolve_dyn::<dyn FleetMembership>()
        {
            Some(source) => source.members(&self.state).map_err(|error| {
                FleetServiceManagerError::Snapshot(format!(
                    "service membership unavailable: {error}"
                ))
            })?,
            // Without a process owner, stay conservative: silence never reduces
            // expected membership. Standalone has already returned above.
            None => (0..self.state.runtime_process_count())
                .map(|node_id| FleetMember {
                    node_id,
                    role: if node_id == 0 {
                        FleetMemberRole::Master
                    } else {
                        FleetMemberRole::Worker
                    },
                })
                .collect(),
        };
        let mut nodes = std::collections::BTreeSet::new();
        if members.is_empty()
            || members.iter().any(|member| {
                member.node_id >= capacity || !nodes.insert(member.node_id)
            })
            || !nodes.contains(&self.state.fleet_node_id().raw())
        {
            return Err(FleetServiceManagerError::Snapshot("invalid service membership: duplicate/out-of-range nodes or missing current process".into()));
        }
        Ok(members)
    }

    /// Return the current fleet snapshot for one registered runnable service.
    pub async fn status(
        &self,
        name: &str,
    ) -> FleetServiceManagerResult<FleetServiceStatusSnapshot> {
        validate_service_name(name)?;
        let snapshot = self.list().await?;
        let service = snapshot
            .services
            .into_iter()
            .find(|service| service.name == name)
            .ok_or_else(|| {
                FleetServiceManagerError::ServiceNotFound(name.to_owned())
            })?;
        Ok(FleetServiceStatusSnapshot {
            captured_at: snapshot.captured_at,
            members_expected: snapshot.members_expected,
            members_reporting: snapshot.members_reporting,
            service,
        })
    }

    pub async fn start(
        &self,
        name: &str,
    ) -> FleetServiceManagerResult<()> {
        self.submit(name, ServiceCommandAction::Start).await
    }

    pub async fn stop(
        &self,
        name: &str,
    ) -> FleetServiceManagerResult<()> {
        self.submit(name, ServiceCommandAction::Stop).await
    }

    pub async fn reload(
        &self,
        name: &str,
    ) -> FleetServiceManagerResult<()> {
        self.submit(name, ServiceCommandAction::Reload).await
    }

    pub async fn restart(
        &self,
        name: &str,
    ) -> FleetServiceManagerResult<()> {
        self.submit(name, ServiceCommandAction::Restart).await
    }

    async fn submit(
        &self,
        name: &str,
        action: ServiceCommandAction,
    ) -> FleetServiceManagerResult<()> {
        validate_service_name(name)?;
        validate_continuity(name, action)?;
        if self.state.is_shutting_down() {
            return Err(FleetServiceManagerError::ShuttingDown);
        }

        let snapshot = self.list().await?;
        let service = snapshot
            .services
            .iter()
            .find(|service| service.name == name)
            .ok_or_else(|| {
                FleetServiceManagerError::ServiceNotFound(name.to_owned())
            })?;
        if action == ServiceCommandAction::Reload
            && service.placements.iter().any(|placement| !placement.reloadable)
        {
            return Err(FleetServiceManagerError::ServiceNotReloadable(
                name.to_owned(),
            ));
        }
        if matches!(
            action,
            ServiceCommandAction::Start | ServiceCommandAction::Restart
        ) && service.placements.iter().any(|placement| {
            placement.start_policy == ServiceStartPolicy::Unavailable
        }) {
            return Err(FleetServiceManagerError::ServiceUnavailable(
                name.to_owned(),
            ));
        }

        self.state
            .events()
            .emit(ServiceFleetCommand { action, name: name.to_owned() });
        Ok(())
    }

    async fn local_snapshot(
        &self
    ) -> FleetServiceManagerResult<FleetServiceListSnapshot> {
        let services = self.local.list().await?;
        Ok(snapshot::local(&self.state, services))
    }
}

fn validate_service_name(name: &str) -> FleetServiceManagerResult<()> {
    if name.is_empty() {
        return Err(FleetServiceManagerError::EmptyServiceName);
    }
    if name.len() > MAX_SERVICE_NAME_BYTES {
        return Err(FleetServiceManagerError::ServiceNameTooLong {
            actual: name.len(),
            max: MAX_SERVICE_NAME_BYTES,
        });
    }
    Ok(())
}

fn validate_continuity(
    name: &str,
    action: ServiceCommandAction,
) -> FleetServiceManagerResult<()> {
    let reason = match (name, action) {
        (
            "EventDispatcher",
            ServiceCommandAction::Stop | ServiceCommandAction::Restart,
        ) => Some(
            "the command listener executes inside this service and cannot await its own drain",
        ),
        ("FleetEventRegistry", ServiceCommandAction::Stop) => Some(
            "stopping every inbound Orbit bridge would remove the fleet recovery path",
        ),
        (SNAPSHOT_PUBLISHER_NAME, ServiceCommandAction::Stop) => Some(
            "stopping every publisher would remove fleet service observability",
        ),
        ("WorkerSnapshotPublisher", ServiceCommandAction::Stop) => Some(
            "stopping the process membership publisher would remove fleet service recovery",
        ),
        // The ingress adapter protects the actual listener carrying a remote
        // command. Orbit cannot assume management always rides HttpsServer.
        _ => None,
    };
    if let Some(reason) = reason {
        return Err(FleetServiceManagerError::ContinuityProtected {
            name: name.to_owned(),
            action: action.label(),
            reason,
        });
    }
    Ok(())
}

async fn apply(
    manager: &ServiceManager,
    name: &str,
    action: ServiceCommandAction,
) -> Result<(), ServiceManagerError> {
    match action {
        ServiceCommandAction::Start => manager.start(name).await?,
        ServiceCommandAction::Stop => manager.stop(name).await?,
        ServiceCommandAction::Reload => manager.reload(name).await?,
        ServiceCommandAction::Restart => manager.restart(name).await?,
    };
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ServiceCommandAction {
    Start,
    Stop,
    Reload,
    Restart,
}

impl ServiceCommandAction {
    const fn code(self) -> u8 {
        match self {
            Self::Start => 1,
            Self::Stop => 2,
            Self::Reload => 3,
            Self::Restart => 4,
        }
    }

    fn from_code(code: u8) -> Result<Self, String> {
        match code {
            1 => Ok(Self::Start),
            2 => Ok(Self::Stop),
            3 => Ok(Self::Reload),
            4 => Ok(Self::Restart),
            _ => Err(format!("unknown service command action {code}")),
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Start => "started",
            Self::Stop => "stopped",
            Self::Reload => "reloaded",
            Self::Restart => "restarted",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ServiceFleetCommand {
    action: ServiceCommandAction,
    name: String,
}

impl Event for ServiceFleetCommand {}

impl FleetEventCodec for ServiceFleetCommand {
    const TOPIC: &'static str = SERVICE_COMMAND_TOPIC;

    fn encode_orbit(&self) -> Vec<u8> {
        let mut payload =
            Vec::with_capacity(SERVICE_COMMAND_HEADER_BYTES + self.name.len());
        payload.push(self.action.code());
        payload.extend_from_slice(self.name.as_bytes());
        payload
    }

    fn decode_orbit(bytes: &[u8]) -> Result<Self, String> {
        let Some((&action, name)) = bytes.split_first() else {
            return Err("service command payload is empty".to_owned());
        };
        if name.is_empty() {
            return Err("service command name is empty".to_owned());
        }
        if name.len() > MAX_SERVICE_NAME_BYTES {
            return Err(format!(
                "service command name is too long: {} bytes, max {MAX_SERVICE_NAME_BYTES}",
                name.len()
            ));
        }
        Ok(Self {
            action: ServiceCommandAction::from_code(action)?,
            name: std::str::from_utf8(name)
                .map_err(|error| {
                    format!("service command name is not UTF-8: {error}")
                })?
                .to_owned(),
        })
    }
}

pub(super) fn register_command_listener(
    state: &AppState
) -> registry::Result<()> {
    state.events().listen::<ServiceFleetCommand, _>(
        |state, command| async move {
            if state.is_shutting_down() {
                return Ok(());
            }
            validate_continuity(&command.name, command.action)
                .map_err(|error| registry::Error::msg(error.to_string()))?;
            let manager = state
                .registry_ref()
                .resolve::<ServiceManager>()
                .ok_or_else(|| {
                    registry::Error::msg(
                        "Continuo ServiceManager is unavailable",
                    )
                })?;
            match apply(&manager, &command.name, command.action).await {
                Ok(()) | Err(ServiceManagerError::NotFound(_)) => Ok(()),
                Err(error) => Err(registry::Error::msg(error.to_string())),
            }
        },
    )
}

pub(super) fn register_command_bridge(
    state: &AppState
) -> registry::Result<()> {
    let events =
        state.registry_ref().resolve::<FleetEventRegistry>().ok_or_else(
            || registry::Error::msg("FleetEventRegistry is unavailable"),
        )?;
    events.register_bridge::<ServiceFleetCommand>(state)
}

#[cfg(test)]
#[path = "../../tests/service/manager.rs"]
mod tests;
