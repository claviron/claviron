use super::*;

fn application(
    name: &str,
    member: FleetMember,
) -> FleetApplicationStatus {
    FleetApplicationStatus {
        id: ApplicationId::new(
            ApplicationRuntime::new("js").unwrap(),
            ApplicationName::new(name).unwrap(),
        ),
        placements: vec![FleetApplicationPlacement {
            member,
            description: ApplicationDescription::new("javascript", "fetch")
                .frameworks(["example-framework", "example-adapter"])
                .icon("server")
                .runner("example-runner"),
            lifecycle: ApplicationLifecycle::Running,
            enabled: true,
            generation: 4,
            pool: Some(ApplicationPoolStatus {
                accepting: Some(true),
                instances: 2,
                ready: None,
                busy: Some(1),
                retiring: None,
                backing_off: Some(0),
                exhausted: None,
            }),
            last_error: None,
            last_error_truncated: false,
            observed_at: UNIX_EPOCH,
        }],
    }
}

#[test]
fn member_batches_round_trip_unknown_pool_counters() {
    let member = FleetMember { node_id: 1, role: FleetMemberRole::Worker };
    let fleet =
        Fleet::join_as("application-snapshot-test", 2, NodeId::new(1)).unwrap();
    fleet.reset_ring::<ApplicationSnapshotRecord>().unwrap();
    let captured_at = SystemTime::now();
    let snapshot = FleetApplicationListSnapshot {
        captured_at,
        members_expected: 1,
        members_reporting: 1,
        applications: vec![application("wallet", member)],
    };

    publish(&fleet, &snapshot, member).unwrap();
    let collected =
        collect(&fleet, captured_at, &[member], Duration::from_secs(5));

    assert_eq!(collected.members_reporting, 1);
    assert_eq!(collected.applications.len(), 1);
    let placement = &collected.applications[0].placements[0];
    assert_eq!(placement.pool.as_ref().unwrap().ready, None);
    assert_eq!(placement.pool.as_ref().unwrap().busy, Some(1));
    assert_eq!(
        placement.description.frameworks,
        ["example-framework", "example-adapter"]
    );
    assert_eq!(placement.description.icon.as_deref(), Some("server"));
}

#[test]
fn oversized_errors_are_truncated_explicitly() {
    let member = FleetMember { node_id: 1, role: FleetMemberRole::Worker };
    let mut application = application("wallet", member);
    application.placements[0].last_error = Some("ö".repeat(400));
    let wire = WirePlacement::from_status(member, 1, 1, 0, &application);
    let decoded = WirePlacement::decode(&wire.encode().unwrap()).unwrap();

    assert!(decoded.last_error_truncated);
    assert!(decoded.last_error.unwrap().ends_with(TRUNCATED_SUFFIX));
}

#[test]
fn linked_snapshot_reuses_the_complete_application_row() {
    let member = FleetMember { node_id: 2, role: FleetMemberRole::Worker };
    let snapshot = FleetApplicationListSnapshot {
        captured_at: UNIX_EPOCH + Duration::from_millis(1234),
        members_expected: 1,
        members_reporting: 1,
        applications: vec![
            application("wallet", member),
            application("reports", member),
        ],
    };
    let rows = encode_local_rows(&snapshot, member).unwrap();
    let decoded = decode_link_rows(&rows).unwrap();
    assert_eq!(decoded.applications.len(), 2);
    assert_eq!(
        decoded.applications[0].placements[0].pool.as_ref().unwrap().busy,
        Some(1)
    );
    assert_eq!(decoded.applications[1].placements[0].generation, 4);
    assert_eq!(
        decoded.applications[0].placements[0].description,
        snapshot.applications[0].placements[0].description
    );
    assert!(decode_link_rows(&rows[..1]).is_err());

    let empty =
        FleetApplicationListSnapshot { applications: Vec::new(), ..snapshot };
    let empty_rows = encode_local_rows(&empty, member).unwrap();
    assert!(decode_link_rows(&empty_rows).unwrap().applications.is_empty());
}

#[test]
fn common_snapshot_ring_has_a_distinct_allocation() {
    assert_ne!(APPLICATION_SNAPSHOT_RING_KIND, 228);
    assert_ne!(APPLICATION_SNAPSHOT_RING_KIND, 229);
    assert_ne!(APPLICATION_SNAPSHOT_RING_KIND, 233);
    assert_ne!(APPLICATION_SNAPSHOT_RING_KIND, 234);
}
