use claviron_orbit::lock::{LOCK_EVENT_RING_KIND, LOCK_STATE_KIND};
use claviron_orbit::{Fleet, NodeId, OrbitEpoch};
use claviron_runtime::WorkerId;
use claviron_sys::process::{self, ProcessIdentity};

use super::*;
use crate::lifecycle::MasterIdentity;

#[test]
fn lifecycle_ring_kinds_do_not_overlap_orbit_locks() {
    let lifecycle = [MASTER_GENERATION_RING_KIND, PRESENCE_RING_KIND];
    let locks = [LOCK_STATE_KIND, LOCK_EVENT_RING_KIND];

    assert_ne!(lifecycle[0], lifecycle[1]);
    assert!(lifecycle.into_iter().all(|kind| !locks.contains(&kind)));
}

fn presence_of(process: ProcessIdentity) -> WorkerPresence {
    let master =
        MasterIdentity::new(ProcessIdentity::new(4_242, 0x0102_0304), 9);
    WorkerPresence::new(
        WorkerIdentity::new(WorkerId::new(2), process, 3, master),
        1_757_780_000_123,
        PresencePhase::Draining,
        1,
        OperationId::from_raw(17),
        OrbitEpoch::from_unix_ms(1_757_780_100_000),
    )
}

#[test]
fn presence_round_trips_both_identities_and_runtime_start() {
    let worker = Fleet::join_as("presence-test", 3, NodeId::new(2)).unwrap();
    // In-memory fleets are process-local; a clone shares the same ring
    // registry and can inspect any node lane just as the master does over
    // the SHM-backed fleet at runtime.
    let reader = worker.clone();
    assert_eq!(latest_worker_presence(&reader, WorkerId::new(2)), None);

    let published = presence_of(ProcessIdentity::new(31_337, 0x0a0b_0c0d_0e0f));
    let encoded = published.encode();
    assert_eq!(encoded.len(), PRESENCE_BYTES);
    assert_eq!(
        WorkerPresence::decode(&encoded, published.captured_at(), 7),
        Some(published.with_counter(7))
    );
    let published = publish_presence(&worker, published).unwrap();
    let head = reader.lane_head::<PresenceRecord>(NodeId::new(2));
    assert_eq!(head, 1, "presence publication must advance worker lane 2");
    assert!(
        reader
            .read_lane_at::<PresenceRecord>(NodeId::new(2), head - 1)
            .is_some(),
        "presence publication must be readable from the master mapping"
    );
    let read = latest_worker_presence(&reader, WorkerId::new(2))
        .expect("presence on lane 2");
    assert_eq!(read, published);
    assert_eq!(read.counter(), 0);
    assert_eq!(read.identity().master().generation(), 9);
    assert_eq!(read.runtime_started_at_unix_ms(), 1_757_780_000_123);
    assert_ne!(
        read.runtime_started_at_unix_ms(),
        read.identity().process().started_at(),
        "the wall-clock start and the kernel start stamp are different facts"
    );
    assert_eq!(read.phase(), PresencePhase::Draining);
    assert_eq!(read.parent_pid(), 1);
    assert_eq!(read.active_operation_id(), Some(17));
    assert_eq!(
        latest_worker_presence(&reader, WorkerId::new(1)),
        None,
        "lanes do not bleed"
    );

    // A newer record on the same lane wins.
    let newer = WorkerPresence { phase: PresencePhase::Running, ..published };
    let newer = publish_presence(&worker, newer).unwrap();
    assert_eq!(newer.counter(), 1);
    assert_eq!(latest_worker_presence(&reader, WorkerId::new(2)), Some(newer));

    let wrong_lane =
        Fleet::join_as("wrong-presence-lane", 3, NodeId::new(1)).unwrap();
    let error = publish_presence(&wrong_lane, published).unwrap_err();
    assert!(
        error.to_string().contains("cannot publish presence on Orbit node 1")
    );
    wrong_lane.publish::<PresenceRecord>(
        PRESENCE_FRAME_KIND,
        published.captured_at().as_unix_ms(),
        Bytes::from(published.encode()),
    );
    assert_eq!(
        latest_worker_presence(&wrong_lane, WorkerId::new(1)),
        None,
        "a lane cannot claim the identity of another worker"
    );
}

#[test]
fn master_rejects_presence_whose_pid_matches_but_start_stamp_does_not() {
    let me = process::identity(std::process::id()).expect("own identity");
    assert!(presence_of(me).exact_process_alive());
    let reused_pid = ProcessIdentity::new(me.pid(), me.started_at() ^ 1);
    assert!(
        !presence_of(reused_pid).exact_process_alive(),
        "a matching PID with another start stamp is another process"
    );
}
