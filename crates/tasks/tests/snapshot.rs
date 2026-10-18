use std::time::Duration;

use claviron_orbit::{Fleet, NodeId};

use super::*;

fn members(ids: &[u16]) -> Vec<claviron_orbit::service::FleetMember> {
    use claviron_orbit::service::{FleetMember, FleetMemberRole};
    ids.iter()
        .map(|&node_id| FleetMember {
            node_id,
            role: if node_id == 0 {
                FleetMemberRole::Master
            } else {
                FleetMemberRole::Worker
            },
        })
        .collect()
}

#[test]
fn member_snapshot_batch_round_trips() {
    let writer =
        Fleet::join_as("task-snapshot-test", 2, NodeId::new(1)).unwrap();
    writer.reset_ring::<TaskSnapshotRecord>().unwrap();
    let captured_at = UNIX_EPOCH + Duration::from_secs(100);
    let snapshot = TaskListSnapshot {
        captured_at,
        members_expected: 2,
        members_reporting: 1,
        tasks: vec![TaskStatus {
            name: "queue".to_string(),
            kind: TaskKind::Command,
            scope: TaskScope::Workers,
            dispatch: TaskDispatch::Pool,
            timing: TaskTimingSummary::Daemon { enabled: true },
            concurrency: 4,
            placements: vec![TaskPlacementStatus {
                member: TaskMember { node_id: 1, role: TaskMemberRole::Worker },
                lifecycle: TaskLifecycle::Active,
                generation: 3,
                in_flight: 2,
                owned_slots: 3,
                last_execution: Some(TaskExecutionOutcome {
                    result: TaskExecutionResult::Failed,
                    finished_at: UNIX_EPOCH + Duration::from_secs(99),
                }),
                observed_at: captured_at,
            }],
        }],
    };

    publish(&writer, TaskMemberRole::Worker, &snapshot).unwrap();
    let collected = collect(
        &writer,
        captured_at,
        Duration::from_secs(5),
        &members(&[0, 1]),
    );
    assert_eq!(collected.members_reporting, 1);
    let placement = &collected.by_task["queue"][0];
    assert_eq!(placement.lifecycle, TaskLifecycle::Active);
    assert_eq!(placement.generation, 3);
    assert_eq!(placement.in_flight, 2);
    assert_eq!(
        placement.last_execution,
        snapshot.tasks[0].placements[0].last_execution
    );
    // The lane is still fresh after removal; it must not remain a placement.
    let removed =
        collect(&writer, captured_at, Duration::from_secs(5), &members(&[0]));
    assert_eq!(removed.members_reporting, 0);
    assert!(removed.by_task.is_empty());
}

#[test]
fn empty_member_snapshot_still_reports_presence_and_expires() {
    let fleet =
        Fleet::join_as("task-snapshot-empty", 2, NodeId::new(1)).unwrap();
    fleet.reset_ring::<TaskSnapshotRecord>().unwrap();
    let captured_at = UNIX_EPOCH + Duration::from_secs(100);
    let snapshot = TaskListSnapshot {
        captured_at,
        members_expected: 2,
        members_reporting: 1,
        tasks: Vec::new(),
    };

    publish(&fleet, TaskMemberRole::Worker, &snapshot).unwrap();

    let fresh =
        collect(&fleet, captured_at, Duration::from_secs(5), &members(&[0, 1]));
    assert_eq!(fresh.members_reporting, 1);
    assert!(fresh.by_task.is_empty());

    let stale = collect(
        &fleet,
        captured_at + Duration::from_secs(6),
        Duration::from_secs(5),
        &members(&[0, 1]),
    );
    assert_eq!(stale.members_reporting, 0);
}
