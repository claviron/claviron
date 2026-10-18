use std::collections::BTreeSet;
use std::sync::Arc;

use claviron_orbit::OrbitStateExt;
use claviron_orbit::service::{FleetMember, FleetMemberRole, FleetMembership};
use claviron_runtime::AppState;

use super::command::{self, ApplicationCommandAction};
use super::publisher::ApplicationSnapshotPublisher;
use super::snapshot::{
    self, FleetApplicationListSnapshot, FleetApplicationStatusSnapshot,
};
use crate::{ApplicationId, ApplicationManager};

#[derive(Debug, thiserror::Error)]
pub enum FleetApplicationManagerError {
    #[error("ApplicationManager is not registered")]
    LocalManagerUnavailable,
    #[error("application fleet control is not registered")]
    ControlUnavailable,
    #[error(
        "application '{0}' is not registered on any reporting fleet member"
    )]
    ApplicationNotFound(ApplicationId),
    #[error(
        "application fleet snapshot is incomplete: members {reporting}/{expected}"
    )]
    SnapshotIncomplete { expected: u16, reporting: u16 },
    #[error("application management is unavailable during shutdown")]
    ShuttingDown,
    #[error("Orbit fleet is unavailable for application snapshots")]
    FleetUnavailable,
    #[error("application fleet membership is unavailable")]
    MembershipUnavailable,
    #[error("application snapshot publisher is unavailable")]
    SnapshotPublisherUnavailable,
    #[error("application snapshot failed: {0}")]
    Snapshot(String),
    #[error("invalid application command: {0}")]
    Command(String),
}

pub type FleetApplicationManagerResult<T> =
    Result<T, FleetApplicationManagerError>;

#[derive(Clone)]
pub struct FleetApplicationManager {
    state: AppState,
    local: Arc<ApplicationManager>,
}

impl FleetApplicationManager {
    pub fn new(state: &AppState) -> FleetApplicationManagerResult<Self> {
        let local = state
            .registry_ref()
            .resolve::<ApplicationManager>()
            .ok_or(FleetApplicationManagerError::LocalManagerUnavailable)?;
        if state
            .registry_ref()
            .resolve::<super::ApplicationFleetControlRegistration>()
            .is_none()
        {
            return Err(FleetApplicationManagerError::ControlUnavailable);
        }
        Ok(Self { state: state.clone(), local })
    }

    pub fn list(
        &self
    ) -> FleetApplicationManagerResult<FleetApplicationListSnapshot> {
        let local = snapshot::local(&self.state, self.local.list());
        if self.state.is_standalone() {
            return Ok(local);
        }
        let fleet = self
            .state
            .fleet()
            .ok_or(FleetApplicationManagerError::FleetUnavailable)?;
        if self.state.is_worker() {
            snapshot::publish(&fleet, &local, super::local_member(&self.state))
                .map_err(FleetApplicationManagerError::Snapshot)?;
        }
        let publisher = self
            .state
            .registry_ref()
            .resolve::<ApplicationSnapshotPublisher>()
            .ok_or(
                FleetApplicationManagerError::SnapshotPublisherUnavailable,
            )?;
        let members = self.expected_members(fleet.fleet_capacity())?;
        Ok(snapshot::collect(
            &fleet,
            local.captured_at,
            &members,
            publisher.policy().max_age(),
        ))
    }

    pub fn status(
        &self,
        id: &ApplicationId,
    ) -> FleetApplicationManagerResult<FleetApplicationStatusSnapshot> {
        let snapshot = self.list()?;
        let application = snapshot
            .applications
            .into_iter()
            .find(|application| &application.id == id)
            .ok_or_else(|| {
                FleetApplicationManagerError::ApplicationNotFound(id.clone())
            })?;
        Ok(FleetApplicationStatusSnapshot {
            captured_at: snapshot.captured_at,
            members_expected: snapshot.members_expected,
            members_reporting: snapshot.members_reporting,
            application,
        })
    }

    pub fn start(
        &self,
        id: &ApplicationId,
    ) -> FleetApplicationManagerResult<()> {
        self.submit(id, ApplicationCommandAction::Start)
    }

    pub fn stop(
        &self,
        id: &ApplicationId,
    ) -> FleetApplicationManagerResult<()> {
        self.submit(id, ApplicationCommandAction::Stop)
    }

    pub fn restart(
        &self,
        id: &ApplicationId,
    ) -> FleetApplicationManagerResult<()> {
        self.submit(id, ApplicationCommandAction::Restart)
    }

    fn submit(
        &self,
        id: &ApplicationId,
        action: ApplicationCommandAction,
    ) -> FleetApplicationManagerResult<()> {
        if self.state.is_shutting_down() {
            return Err(FleetApplicationManagerError::ShuttingDown);
        }
        let snapshot = self.list()?;
        if snapshot.members_reporting != snapshot.members_expected {
            return Err(FleetApplicationManagerError::SnapshotIncomplete {
                expected: snapshot.members_expected,
                reporting: snapshot.members_reporting,
            });
        }
        if !snapshot
            .applications
            .iter()
            .any(|application| &application.id == id)
        {
            return Err(FleetApplicationManagerError::ApplicationNotFound(
                id.clone(),
            ));
        }
        let command = command::command(action, id.clone())
            .map_err(FleetApplicationManagerError::Command)?;
        self.state.events().emit(command);
        Ok(())
    }

    fn expected_members(
        &self,
        capacity: u16,
    ) -> FleetApplicationManagerResult<Vec<FleetMember>> {
        let source = self
            .state
            .registry_ref()
            .resolve_dyn::<dyn FleetMembership>()
            .ok_or(FleetApplicationManagerError::MembershipUnavailable)?;
        let members = source
            .members(&self.state)
            .map_err(FleetApplicationManagerError::Snapshot)?
            .into_iter()
            .filter(|member| member.role == FleetMemberRole::Worker)
            .collect::<Vec<_>>();
        let mut nodes = BTreeSet::new();
        if members.is_empty()
            || members.iter().any(|member| {
                member.node_id >= capacity || !nodes.insert(member.node_id)
            })
        {
            return Err(FleetApplicationManagerError::Snapshot(
                "invalid application fleet membership".to_owned(),
            ));
        }
        Ok(members)
    }
}

#[cfg(test)]
#[path = "../../tests/fleet/manager.rs"]
mod tests;
