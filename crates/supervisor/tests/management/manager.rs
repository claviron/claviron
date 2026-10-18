use claviron_orbit::FleetEventCodec;

use super::{CommandKey, WorkerCommand, WorkerCommandAction};

#[test]
fn targeted_commands_require_current_membership_not_the_relays_bootstrap_count()
{
    use claviron_runtime::{
        AppState, FleetCapacity, FleetNodeId, RunMode, WorkerCount, WorkerId,
    };

    let state = AppState::initialize(
        RunMode::Worker(WorkerId::new(1)),
        FleetNodeId::new(1),
        WorkerCount::new(2),
        FleetCapacity::new(4).unwrap(),
        Some("resize-relay-test".into()),
        ".",
    );
    let manager = super::WorkerManager { state };
    // No current fleet observation exists. Neither an old in-range id
    // nor a newly added id may be decided from the relay's birth count.
    for id in [1, 3] {
        assert!(matches!(
            manager.submit(id, WorkerCommandAction::Start),
            Err(super::WorkerManagerError::Unavailable)
        ));
    }
}

#[test]
fn worker_command_round_trips() {
    let command = WorkerCommand {
        action: WorkerCommandAction::Reload,
        worker_id: 3,
        origin_worker_id: 2,
        expected_worker_count: 0,
        target_worker_count: 0,
        command: CommandKey::from_raw(0x2a).unwrap(),
    };
    assert_eq!(
        WorkerCommand::decode_orbit(&command.encode_orbit()).unwrap(),
        command
    );
}

#[test]
fn reload_all_command_uses_reserved_zero_id() {
    let command = WorkerCommand {
        action: WorkerCommandAction::ReloadAll,
        worker_id: 0,
        origin_worker_id: 2,
        expected_worker_count: 0,
        target_worker_count: 0,
        command: CommandKey::from_raw(0x2a).unwrap(),
    };
    assert_eq!(
        WorkerCommand::decode_orbit(&command.encode_orbit()).unwrap(),
        command
    );
    assert!(
        WorkerCommand::decode_orbit(
            &WorkerCommand {
                action: WorkerCommandAction::ReloadAll,
                worker_id: 1,
                origin_worker_id: 2,
                expected_worker_count: 0,
                target_worker_count: 0,
                command: CommandKey::from_raw(0x2a).unwrap()
            }
            .encode_orbit()
        )
        .is_err()
    );
}

#[test]
fn restart_all_command_carries_its_origin() {
    let command = WorkerCommand {
        action: WorkerCommandAction::RestartAll,
        worker_id: 0,
        origin_worker_id: 3,
        expected_worker_count: 0,
        target_worker_count: 0,
        command: CommandKey::from_raw(0x2a).unwrap(),
    };
    assert_eq!(
        WorkerCommand::decode_orbit(&command.encode_orbit()).unwrap(),
        command
    );
}

#[test]
fn restart_command_targets_one_worker() {
    let command = WorkerCommand {
        action: WorkerCommandAction::Restart,
        worker_id: 2,
        origin_worker_id: 1,
        expected_worker_count: 0,
        target_worker_count: 0,
        command: CommandKey::from_raw(0x2a).unwrap(),
    };
    assert_eq!(
        WorkerCommand::decode_orbit(&command.encode_orbit()).unwrap(),
        command
    );
}

#[test]
fn resize_command_carries_one_step_fleet_transition() {
    let command = WorkerCommand {
        action: WorkerCommandAction::Resize,
        worker_id: 0,
        origin_worker_id: 2,
        expected_worker_count: 3,
        target_worker_count: 4,
        command: CommandKey::from_raw(0x2a).unwrap(),
    };
    assert_eq!(
        WorkerCommand::decode_orbit(&command.encode_orbit()).unwrap(),
        command
    );

    let mut invalid = command;
    invalid.target_worker_count = 5;
    assert!(WorkerCommand::decode_orbit(&invalid.encode_orbit()).is_err());
}
