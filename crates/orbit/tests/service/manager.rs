use super::*;

struct TestMembership(Result<Vec<FleetMember>, String>);
impl FleetMembership for TestMembership {
    fn members(
        &self,
        _: &AppState,
    ) -> Result<Vec<FleetMember>, String> {
        self.0.clone()
    }
}

#[test]
fn membership_comes_from_the_owner_and_never_falls_back_after_source_failure() {
    use claviron_runtime::{
        FleetCapacity, FleetNodeId, RunMode, Runtime, WorkerCount, WorkerId,
    };
    let member = |node_id| FleetMember {
        node_id,
        role: if node_id == 0 {
            FleetMemberRole::Master
        } else {
            FleetMemberRole::Worker
        },
    };
    let make_manager = || {
        let state = AppState::initialize(
            RunMode::Worker(WorkerId::new(2)),
            FleetNodeId::new(2),
            WorkerCount::new(2),
            FleetCapacity::new(3).unwrap(),
            None,
            ".",
        );
        FleetServiceManager { state, local: Runtime::default().manager() }
    };
    assert_eq!(make_manager().expected_members(3).unwrap().len(), 3);
    for (source, valid) in [
        (Ok(vec![member(0), member(2)]), true),
        (Err("stale worker snapshot".into()), false),
        (Ok(vec![member(0)]), false),
        (Ok(vec![member(0), member(2), member(2)]), false),
        (Ok(vec![member(0), member(2), member(3)]), false),
        (Ok(vec![]), false),
    ] {
        let manager = make_manager();
        manager
            .state
            .registry_ref()
            .insert(Arc::new(TestMembership(source)))
            .unwrap();
        manager
            .state
            .registry_ref()
            .bind_dyn::<dyn FleetMembership, TestMembership>(|source| source)
            .unwrap();
        let result = manager.expected_members(3);
        assert_eq!(result.is_ok(), valid);
        if valid {
            assert_eq!(result.unwrap(), [member(0), member(2)]);
        }
    }
}

use crate::FleetEventCodec;

#[test]
fn command_codec_round_trips_every_action() {
    for action in [
        ServiceCommandAction::Start,
        ServiceCommandAction::Stop,
        ServiceCommandAction::Reload,
        ServiceCommandAction::Restart,
    ] {
        let command =
            ServiceFleetCommand { action, name: "HttpsServer".to_owned() };
        assert_eq!(
            ServiceFleetCommand::decode_orbit(&command.encode_orbit()).unwrap(),
            command
        );
    }
}

#[test]
fn continuity_protects_the_command_and_observation_paths() {
    assert!(
        validate_continuity("EventDispatcher", ServiceCommandAction::Restart)
            .is_err()
    );
    assert!(
        validate_continuity("FleetEventRegistry", ServiceCommandAction::Stop)
            .is_err()
    );
    assert!(
        validate_continuity(
            SNAPSHOT_PUBLISHER_NAME,
            ServiceCommandAction::Stop
        )
        .is_err()
    );
    assert!(
        validate_continuity(
            "WorkerSnapshotPublisher",
            ServiceCommandAction::Stop
        )
        .is_err()
    );
    assert!(
        validate_continuity(
            "WorkerSnapshotPublisher",
            ServiceCommandAction::Restart
        )
        .is_ok()
    );
    assert!(
        validate_continuity("HttpsServer", ServiceCommandAction::Stop).is_ok()
    );
    assert!(
        validate_continuity("HttpsServer", ServiceCommandAction::Restart)
            .is_ok()
    );
}
