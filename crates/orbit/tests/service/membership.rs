use std::sync::{Arc, RwLock};

use claviron_runtime::{
    FleetCapacity, FleetNodeId, RunMode, WorkerCount, WorkerId,
};

use super::*;

struct Membership(RwLock<Result<Vec<FleetMember>, String>>);

impl FleetMembership for Membership {
    fn members(
        &self,
        _: &AppState,
    ) -> Result<Vec<FleetMember>, String> {
        self.0.read().unwrap().clone()
    }
}

#[test]
fn retained_worker_reads_growth_shrink_and_unavailability() {
    let state = AppState::initialize(
        RunMode::Worker(WorkerId::new(1)),
        FleetNodeId::new(1),
        WorkerCount::new(1),
        FleetCapacity::new(5).unwrap(),
        None,
        ".",
    );
    assert!(current_members(&state).is_err());
    let source = Arc::new(Membership(RwLock::new(Ok(vec![]))));
    state.registry_ref().insert(source.clone()).unwrap();
    state
        .registry_ref()
        .bind_dyn::<dyn FleetMembership, Membership>(|s| s)
        .unwrap();
    for ids in [vec![0, 1], vec![0, 1, 2, 3], vec![0, 1, 3]] {
        *source.0.write().unwrap() = Ok(ids
            .iter()
            .map(|&node_id| FleetMember {
                node_id,
                role: if node_id == 0 {
                    FleetMemberRole::Master
                } else {
                    FleetMemberRole::Worker
                },
            })
            .collect());
        assert_eq!(
            current_members(&state)
                .unwrap()
                .iter()
                .map(|m| m.node_id)
                .collect::<Vec<_>>(),
            ids
        );
        assert_eq!(state.worker_count(), 1);
    }
    *source.0.write().unwrap() = Err("stale snapshot".into());
    assert_eq!(current_members(&state).unwrap_err(), "stale snapshot");
    for ids in [vec![], vec![0, 2], vec![0, 1, 1], vec![0, 1, 5]] {
        *source.0.write().unwrap() = Ok(ids
            .into_iter()
            .map(|node_id| FleetMember {
                node_id,
                role: if node_id == 0 {
                    FleetMemberRole::Master
                } else {
                    FleetMemberRole::Worker
                },
            })
            .collect());
        assert!(current_members(&state).is_err());
    }
}
