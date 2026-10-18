use std::sync::atomic::{AtomicU64, Ordering};

use super::*;

static NEXT_FLEET: AtomicU64 = AtomicU64::new(1);

struct TestRing(Fleet);

#[test]
fn service_snapshot_kind_does_not_overlap_orbit_lock_transport() {
    assert_ne!(SERVICE_SNAPSHOT_RING_KIND, orbitive::lock::LOCK_STATE_KIND);
    assert_ne!(
        SERVICE_SNAPSHOT_RING_KIND,
        orbitive::lock::LOCK_EVENT_RING_KIND
    );
}

impl Drop for TestRing {
    fn drop(&mut self) {
        self.0.shm_ring::<ServiceSnapshotRecord>().unwrap().unlink().unwrap();
    }
}

fn test_fleet() -> (Fleet, TestRing) {
    let name: &'static str = Box::leak(
        format!(
            "sv{:x}{:x}",
            std::process::id(),
            NEXT_FLEET.fetch_add(1, Ordering::Relaxed)
        )
        .into_boxed_str(),
    );
    let writer = Fleet::join_shm_as(name, 2, NodeId::new(1)).unwrap();
    let reader = Fleet::join_shm_as(name, 2, NodeId::ZERO).unwrap();
    reader.reset_ring::<ServiceSnapshotRecord>().unwrap();
    (writer, TestRing(reader))
}

#[test]
fn member_batch_round_trips_service_state_and_errors() {
    let (writer, reader) = test_fleet();
    let now = SystemTime::now();
    let snapshot = FleetServiceListSnapshot {
        captured_at: now,
        members_expected: 2,
        members_reporting: 1,
        services: vec![FleetServiceStatus {
            name: "HttpsServer".to_owned(),
            placements: vec![FleetServicePlacement {
                member: FleetMember {
                    node_id: 1,
                    role: FleetMemberRole::Worker,
                },
                status: ServiceStatus::Failed,
                dispatch: Dispatch::Pool,
                active_executions: 0,
                start_policy: ServiceStartPolicy::Manual,
                reloadable: true,
                generation: 4,
                reload_revision: 2,
                last_error: Some("run failed".to_owned()),
                last_error_truncated: false,
                last_reload_error: Some("reload failed".to_owned()),
                last_reload_error_truncated: false,
                observed_at: now,
            }],
        }],
    };

    publish(&writer, &snapshot).unwrap();
    let members = [
        FleetMember { node_id: 0, role: FleetMemberRole::Master },
        FleetMember { node_id: 1, role: FleetMemberRole::Worker },
    ];
    let collected = collect(&reader.0, now, &members, Duration::from_secs(1));

    assert_eq!(collected.members_reporting, 1);
    let placement = &collected.services[0].placements[0];
    assert_eq!(placement.status, ServiceStatus::Failed);
    assert_eq!(placement.dispatch, Dispatch::Pool);
    assert_eq!(placement.active_executions, 0);
    assert_eq!(placement.start_policy, ServiceStartPolicy::Manual);
    assert_eq!(placement.generation, 4);
    assert_eq!(placement.reload_revision, 2);
    assert_eq!(placement.last_error.as_deref(), Some("run failed"));
    assert_eq!(placement.last_reload_error.as_deref(), Some("reload failed"));

    // A stopped member's still-fresh batch must not appear as a placement
    // or satisfy a different live member's missing report.
    let stopped =
        collect(&reader.0, now, &members[..1], Duration::from_secs(1));
    assert_eq!((stopped.members_reporting, stopped.members_expected), (0, 1));
    assert!(stopped.services.is_empty());
    let only_worker =
        collect(&reader.0, now, &members[1..], Duration::from_secs(1));
    assert_eq!(
        (only_worker.members_reporting, only_worker.members_expected),
        (1, 1)
    );
    let stale = collect(
        &reader.0,
        now + Duration::from_secs(2),
        &members[1..],
        Duration::from_secs(1),
    );
    assert_eq!((stale.members_reporting, stale.members_expected), (0, 1));

    let mut master_snapshot = snapshot;
    master_snapshot.services[0].placements[0].member = members[0];
    master_snapshot.services[0].placements[0].status = ServiceStatus::Running;
    master_snapshot.services[0].placements[0].active_executions = 2;
    publish(&reader.0, &master_snapshot).unwrap();
    let all = collect(&reader.0, now, &members, Duration::from_secs(1));
    assert_eq!((all.members_reporting, all.members_expected), (2, 2));
    let stopped =
        collect(&reader.0, now, &members[..1], Duration::from_secs(1));
    assert_eq!((stopped.members_reporting, stopped.members_expected), (1, 1));
    assert_eq!(stopped.services[0].placements.len(), 1);
    assert_eq!(stopped.services[0].placements[0].member, members[0]);
    assert_eq!(stopped.services[0].placements[0].active_executions, 2);
}

#[test]
fn oversized_errors_are_truncated_explicitly() {
    let value = "ö".repeat(SERVICE_SNAPSHOT_MAX_ERROR_BYTES);
    let (bounded, truncated) =
        bounded(Some(&value), SERVICE_SNAPSHOT_MAX_ERROR_BYTES);

    assert!(truncated);
    assert!(bounded.unwrap().len() <= SERVICE_SNAPSHOT_MAX_ERROR_BYTES);
}
