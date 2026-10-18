use std::sync::Arc;
#[cfg(unix)]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(unix)]
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(unix)]
use bytes::Bytes;
#[cfg(unix)]
use claviron_orbit::{Fleet, NodeId};
use claviron_runtime::Component;

use super::HeartbeatProvider;
#[cfg(unix)]
use super::{HEARTBEAT_FRAME_KIND, HeartbeatRecord, latest_heartbeats};

#[test]
fn heartbeat_is_registry_managed_runnable() {
    let provider = Arc::new(HeartbeatProvider::new());

    assert!(provider.as_runnable().is_some());
}

#[cfg(unix)]
#[test]
fn latest_heartbeats_reads_each_worker_lane() {
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let pid = std::process::id() & 0xffff;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .subsec_nanos();
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed) & 0xff;
    let fleet_name = Box::leak(
        format!("h{pid:04x}{nonce:08x}{counter:02x}").into_boxed_str(),
    );
    let fleet = Fleet::join_shm_as(fleet_name, 3, NodeId::ZERO)
        .expect("join heartbeat fleet");
    let ring =
        fleet.shm_ring::<HeartbeatRecord>().expect("open heartbeat ring");
    ring.reset();

    ring.write(NodeId::new(1), HEARTBEAT_FRAME_KIND, 10, Bytes::new())
        .expect("publish worker one heartbeat");
    ring.write(NodeId::new(2), HEARTBEAT_FRAME_KIND, 11, Bytes::new())
        .expect("publish worker two heartbeat");
    ring.write(NodeId::new(1), HEARTBEAT_FRAME_KIND, 12, Bytes::new())
        .expect("publish newer worker one heartbeat");

    let latest = latest_heartbeats(&fleet);
    assert_eq!(latest.len(), 2);
    assert_eq!(latest[0].node_id, NodeId::new(1));
    assert_eq!(latest[0].captured_at.as_unix_ms(), 12);
    assert_eq!(latest[0].counter(), 1);
    assert_eq!(latest[1].node_id, NodeId::new(2));
    assert_eq!(latest[1].captured_at.as_unix_ms(), 11);
    assert_eq!(latest[1].counter(), 0);

    ring.unlink().expect("unlink heartbeat ring");
}
