use std::time::{Duration, UNIX_EPOCH};

use claviron_orbit::{Fleet, NodeId};
use claviron_runtime::WorkerId;
use claviron_sys::process::ProcessIdentity;

use super::{
    FLAG_BUILD_DIRTY, WIRE_BYTES, WireHeader, WorkerLifecycle,
    WorkerListSnapshot, WorkerOperation, WorkerOwnership, WorkerSnapshotRecord,
    WorkerStatus, collect, publish,
};
use crate::lifecycle::{
    BuildIdentity, CommandKey, DrainReason, ExitDisposition, FleetMode,
    MasterIdentity, OperationId, OperationKind, OperationOrigin,
    WorkerBlockReason, WorkerIntent, WorkerObservedPhase,
};

fn detailed_snapshot() -> WorkerListSnapshot {
    WorkerListSnapshot {
        captured_at: UNIX_EPOCH + Duration::from_secs(100),
        workers_expected: 3,
        build: BuildIdentity::from_wire(0x016f_f434, true),
        master: Some(MasterIdentity::new(
            ProcessIdentity::new(4_242, 0x0102_0304_0506),
            7,
        )),
        master_runtime: Some(super::MasterRuntimeSnapshot {
            started_at_unix_ms: 50_123,
            uptime: Duration::from_millis(49_877),
            reload_revision: 3,
        }),
        fleet_mode: FleetMode::ShuttingDown,
        workers: vec![
            // A worker whose rolling-restart drain the fleet shutdown took over,
            // acknowledged 3 s ago, wanted gone for 12 s.
            WorkerStatus {
                worker_id: 1,
                ownership: WorkerOwnership::Spawned,
                process: Some(ProcessIdentity::new(123, 0x0a0b_0c0d)),
                uptime: Some(Duration::from_secs(10)),
                recovery_attempts: 2,
                incarnation: 4,
                reload_revision: 3,
                phase: WorkerObservedPhase::DrainAcknowledged,
                phase_age: Duration::from_secs(3),
                desired: WorkerIntent::Drain {
                    reason: DrainReason::RollingRestart,
                    after_exit: ExitDisposition::FinishFleetShutdown,
                },
                desired_age: Duration::from_secs(12),
                operation: Some(WorkerOperation {
                    id: OperationId::from_raw(9).unwrap(),
                    kind: OperationKind::FleetShutdown,
                    origin: OperationOrigin::Signal,
                    age: Duration::from_secs(5),
                }),
                heartbeat_age: Some(Duration::from_millis(800)),
                block_reason: None,
            },
            // An adopted worker mid-resize.
            WorkerStatus {
                worker_id: 2,
                ownership: WorkerOwnership::Adopted,
                process: Some(ProcessIdentity::new(456, 0x0e0f_1011)),
                uptime: Some(Duration::from_secs(3_600)),
                recovery_attempts: 0,
                incarnation: 2,
                reload_revision: 0,
                phase: WorkerObservedPhase::Running,
                phase_age: Duration::from_secs(3_500),
                desired: WorkerIntent::EnsureRunning,
                desired_age: Duration::from_secs(3_600),
                operation: Some(WorkerOperation {
                    id: OperationId::from_raw(11).unwrap(),
                    kind: OperationKind::Resize { from: 3, to: 4 },
                    origin: OperationOrigin::Management {
                        via_worker: Some(WorkerId::new(1)),
                        command: CommandKey::from_raw(0x0100_0197_5c8d_2a01)
                            .unwrap(),
                    },
                    age: Duration::from_millis(250),
                }),
                heartbeat_age: None,
                block_reason: None,
            },
            // A slot boot could not reconcile: unowned, but the process
            // that blocks it is named.
            WorkerStatus {
                worker_id: 3,
                ownership: WorkerOwnership::Unowned,
                process: Some(ProcessIdentity::new(31_337, 0x1213_1415)),
                uptime: Some(Duration::from_secs(90)),
                recovery_attempts: 0,
                incarnation: 5,
                reload_revision: 0,
                phase: WorkerObservedPhase::ReconciliationBlocked,
                phase_age: Duration::from_secs(40),
                desired: WorkerIntent::EnsureRunning,
                desired_age: Duration::from_secs(40),
                operation: None,
                heartbeat_age: None,
                block_reason: Some(WorkerBlockReason::MasterStillAlive),
            },
        ],
    }
}

#[test]
fn wire_status_round_trips() {
    let snapshot = detailed_snapshot();
    let header = WireHeader::of(&snapshot, Duration::from_secs(5), 3);
    for (index, worker) in snapshot.workers.iter().enumerate() {
        let encoded = header.row(index as u16, worker).encode();
        assert_eq!(encoded.len(), WIRE_BYTES);
        let decoded = super::WireStatus::decode(&encoded).unwrap();
        assert_eq!(decoded.build_revision, 0x016f_f434);
        assert_ne!(decoded.flags & FLAG_BUILD_DIRTY, 0);
        assert_eq!(decoded.master_generation, 7);
        assert_eq!(decoded.master_runtime(), snapshot.master_runtime);
        assert_eq!(decoded.into_status().unwrap(), *worker);
    }
    let marker = header.empty_marker().encode();
    assert_eq!(marker.len(), WIRE_BYTES);
    assert_eq!(super::WireStatus::decode(&marker).unwrap().row_count, 0);
}

#[test]
fn master_runtime_is_part_of_the_coherent_batch_header() {
    let mut snapshot = detailed_snapshot();
    let header = WireHeader::of(&snapshot, Duration::from_secs(5), 3);
    let original = header.row(0, &snapshot.workers[0]);
    let runtime = snapshot.master_runtime.as_mut().unwrap();
    runtime.reload_revision += 1;
    runtime.uptime += Duration::from_millis(1);
    let next = WireHeader::of(&snapshot, Duration::from_secs(5), 3)
        .row(0, &snapshot.workers[0]);
    assert!(!original.same_header_as(&next));
    assert_eq!(
        original.master_runtime().unwrap().started_at_unix_ms,
        next.master_runtime().unwrap().started_at_unix_ms
    );

    snapshot.master_runtime = None;
    let absent =
        WireHeader::of(&snapshot, Duration::from_secs(5), 3).empty_marker();
    assert_eq!(absent.master_runtime(), None);
    snapshot.master_runtime = Some(super::MasterRuntimeSnapshot {
        started_at_unix_ms: 0,
        uptime: Duration::ZERO,
        reload_revision: 0,
    });
    let zero =
        WireHeader::of(&snapshot, Duration::from_secs(5), 3).empty_marker();
    assert!(!absent.same_header_as(&zero));
    assert_eq!(
        super::WireStatus::decode(&zero.encode()).unwrap().master_runtime(),
        snapshot.master_runtime
    );
}

#[test]
fn lifecycle_is_derived_from_phase_and_intent() {
    let snapshot = detailed_snapshot();
    assert_eq!(snapshot.workers[0].lifecycle(), WorkerLifecycle::Stopping);
    assert_eq!(snapshot.workers[1].lifecycle(), WorkerLifecycle::Running);
    assert_eq!(snapshot.workers[2].lifecycle(), WorkerLifecycle::Unclaimed);
    assert_eq!(
        snapshot.workers[2].pid(),
        Some(31_337),
        "the blocking process is named"
    );
}

#[test]
fn master_snapshot_batch_round_trips_and_expires() {
    let fleet =
        Fleet::join_as("worker-snapshot-test", 3, NodeId::ZERO).unwrap();
    fleet.reset_ring::<WorkerSnapshotRecord>().unwrap();
    let snapshot = detailed_snapshot();
    let captured_at = snapshot.captured_at;

    publish(&fleet, &snapshot, Duration::from_secs(5)).unwrap();
    assert_eq!(collect(&fleet, captured_at).unwrap(), snapshot);
    assert!(collect(&fleet, captured_at + Duration::from_secs(6)).is_err());

    // An empty fleet still publishes its header.
    let empty = WorkerListSnapshot {
        captured_at: captured_at + Duration::from_secs(1),
        workers_expected: 0,
        build: snapshot.build,
        master: None,
        master_runtime: None,
        fleet_mode: FleetMode::Running,
        workers: Vec::new(),
    };
    publish(&fleet, &empty, Duration::from_secs(5)).unwrap();
    assert_eq!(collect(&fleet, empty.captured_at).unwrap(), empty);
}

#[test]
fn a_row_with_a_bad_code_is_refused_not_defaulted() {
    let snapshot = detailed_snapshot();
    let header = WireHeader::of(&snapshot, Duration::from_secs(5), 3);
    let mut bytes = header.row(1, &snapshot.workers[1]).encode();
    // Find the phase byte by changing only the phase.
    let mut other = snapshot.workers[1].clone();
    other.phase = WorkerObservedPhase::Starting;
    let other_bytes = header.row(1, &other).encode();
    let differing: Vec<usize> = (0..bytes.len())
        .filter(|index| bytes[*index] != other_bytes[*index])
        .collect();
    assert_eq!(differing.len(), 1, "phase is one byte");
    bytes[differing[0]] = 200;
    assert!(super::WireStatus::decode(&bytes).unwrap().into_status().is_err());
}
