use claviron_orbit::service::{FleetMember, FleetMemberRole, FleetMembership};
use claviron_runtime::{
    AppState, FleetCapacity, FleetNodeId, RunMode, WorkerCount, WorkerId,
};

use super::*;

struct TestMembership(Vec<FleetMember>);

impl FleetMembership for TestMembership {
    fn members(
        &self,
        _: &AppState,
    ) -> Result<Vec<FleetMember>, String> {
        Ok(self.0.clone())
    }
}

#[test]
fn application_membership_uses_live_workers_but_not_master() {
    let state = AppState::initialize(
        RunMode::Worker(WorkerId::new(2)),
        FleetNodeId::new(2),
        WorkerCount::new(2),
        FleetCapacity::new(3).unwrap(),
        None,
        ".",
    );
    state
        .registry_ref()
        .insert(Arc::new(TestMembership(vec![
            FleetMember { node_id: 0, role: FleetMemberRole::Master },
            FleetMember { node_id: 2, role: FleetMemberRole::Worker },
        ])))
        .unwrap();
    state
        .registry_ref()
        .bind_dyn::<dyn FleetMembership, TestMembership>(|source| source)
        .unwrap();
    let manager = FleetApplicationManager {
        state,
        local: Arc::new(ApplicationManager::new()),
    };

    assert_eq!(
        manager.expected_members(3).unwrap(),
        vec![FleetMember { node_id: 2, role: FleetMemberRole::Worker }]
    );
}

#[test]
fn application_membership_rejects_duplicate_worker_nodes() {
    let state = AppState::standalone("application-membership-test", ".");
    let member = FleetMember { node_id: 1, role: FleetMemberRole::Worker };
    state
        .registry_ref()
        .insert(Arc::new(TestMembership(vec![member, member])))
        .unwrap();
    state
        .registry_ref()
        .bind_dyn::<dyn FleetMembership, TestMembership>(|source| source)
        .unwrap();
    let manager = FleetApplicationManager {
        state,
        local: Arc::new(ApplicationManager::new()),
    };

    assert!(manager.expected_members(3).is_err());
}
