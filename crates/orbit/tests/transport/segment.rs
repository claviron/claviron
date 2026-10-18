use super::*;

/// A segment name nobody else in the fleet namespace will collide with, so the
/// test can run beside a live server.
fn scratch_fleet() -> String {
    format!("tx{}t{}", std::process::id(), unique())
}

fn unique() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

/// Unlinks its segment when the test ends. Shared memory outlives the process
/// that made it, so a test that forgot would leave an object behind for every
/// run after it.
struct Scratch(TransportSegment);

impl Scratch {
    fn open(
        fleet: &str,
        capacity: usize,
    ) -> Self {
        Self(TransportSegment::open(fleet, capacity).unwrap())
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = self.0.unlink();
    }
}

#[tokio::test]
async fn exited_process_closes_only_its_claimed_and_draining_streams() {
    use orbitive::link::LinkBodies;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let name = scratch_fleet();
    let held = Scratch::open(&name, 4);
    let live = held.0.join("edge", "edge", 10).unwrap();
    let dead = held.0.join("worker", "worker", 20).unwrap();
    let sibling = held.0.join("worker", "worker", 30).unwrap();
    let bodies = |lane, incarnation| {
        LinkBodies::for_name(&name, lane, incarnation, held.0.spec).unwrap()
    };
    let origin = bodies(live.lane, 10);
    origin.streams().reset_all();
    let remote = bodies(dead.lane, 20);
    let other = bodies(sibling.lane, 30);
    let (mut request, ticket) = origin.create().unwrap();
    let _response = remote.accept(ticket).unwrap();
    let (mut live_request, ticket) = origin.create().unwrap();
    let mut live_response = other.accept(ticket).unwrap();

    // Simulate the kernel releasing one participant's hold, not a runtime
    // boot. A withdrawn lane still owns its accepted streams until then.
    held.0.inbox().release_lane(dead.lane).unwrap();
    held.0.process_exited(std::process::id(), 20).unwrap();
    request.write_all(b"draining").await.unwrap();
    drop(dead.hold);
    held.0.process_exited(std::process::id(), 21).unwrap();
    request.write_all(b"still").await.unwrap();
    held.0.process_exited(std::process::id(), 20).unwrap();
    let mut byte = [0; 1];
    assert!(request.read(&mut byte).await.is_err());
    assert!(request.write_all(b"gone").await.is_err());

    live_request.write_all(b"alive").await.unwrap();
    let mut bytes = [0; 5];
    live_response.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"alive");
    assert_eq!(held.0.inbox().consumers(sibling.lane).unwrap(), 1);
    origin.unlink().unwrap();
}

/// The first process creates the table; the second maps the same bytes rather
/// than a copy. This is the whole point — two processes, one mailbox wall.
#[test]
fn a_second_open_attaches_to_the_same_table() {
    let fleet = scratch_fleet();
    let first = Scratch::open(&fleet, 4);
    assert!(first.0.created(), "the first open creates");

    let second = TransportSegment::open(&fleet, 4).unwrap();
    assert!(!second.created(), "the second attaches");

    first.0.inbox().claim_lane(2, "checkout", "asgi", 9).unwrap();
    second.inbox().write(2, b"across the boundary").unwrap();

    let mut out = Vec::new();
    assert!(first.0.inbox().read(2, &mut out).unwrap());
    assert_eq!(out, b"across the boundary");
}

/// Attaching must not clear what is already there: another process's lane may
/// be holding frames it has not read, and zeroing the table would take them.
#[test]
fn attaching_leaves_a_live_lane_alone() {
    let fleet = scratch_fleet();
    let first = Scratch::open(&fleet, 4);
    first.0.inbox().claim_lane(1, "catalog", "asgi", 3).unwrap();
    first.0.inbox().write(1, b"queued before the peer arrived").unwrap();

    let second = TransportSegment::open(&fleet, 4).unwrap();
    assert_eq!(
        second.inbox().depth(1).unwrap(),
        1,
        "the peer wiped a live lane"
    );
    assert_eq!(second.inbox().service(1).unwrap().as_deref(), Some("catalog"));
}

/// An edge chooses by reading, so what it reads must survive the process
/// boundary: identity, presence and depth are all written by one process and
/// read by another.
#[test]
fn targets_are_visible_across_the_boundary() {
    let fleet = scratch_fleet();
    let owner = Scratch::open(&fleet, 6);
    owner.0.inbox().claim_lane(0, "checkout", "asgi", 1).unwrap();
    owner.0.inbox().claim_lane(1, "checkout", "asgi", 1).unwrap();
    owner.0.inbox().claim_lane(2, "catalog", "asgi", 1).unwrap();

    let edge = TransportSegment::open(&fleet, 6).unwrap();
    edge.inbox().write(0, b"busy").unwrap();

    let targets = edge.inbox().targets("checkout");
    assert_eq!(targets.len(), 2);
    assert_eq!(targets[0], (1, 0), "the idle lane comes first");
    assert_eq!(targets[1], (0, 1));

    assert_eq!(edge.inbox().targets("catalog").len(), 1);
    assert!(edge.inbox().targets("nobody").is_empty());

    // A worker that leaves stops being a target immediately: nothing is
    // subscribed to, so nothing has to be told.
    owner.0.inbox().release_lane(1).unwrap();
    assert_eq!(edge.inbox().targets("checkout").len(), 1);
}

/// Two binaries that disagree about the geometry must not both open the
/// segment: one of them would read another shape's bytes as its own.
#[test]
fn a_differently_shaped_open_is_refused() {
    let fleet = scratch_fleet();
    let _held = Scratch::open(&fleet, 4);
    assert!(
        TransportSegment::open(&fleet, 8).is_err(),
        "a different fleet capacity is a different table"
    );
}

/// An application is a lane identity, not the physical SHM address.
#[test]
fn an_application_name_is_checked_independently_of_the_fleet() {
    assert!(check_application_name("fastapi-app").is_ok());
    assert!(check_application_name(&"a".repeat(47)).is_ok());
    assert!(matches!(
        check_application_name(""),
        Err(Error::InvalidName { .. })
    ));
    assert!(matches!(
        check_application_name("api/v2"),
        Err(Error::InvalidName { .. })
    ));
    // Past every host's limit: a lane holds 47 bytes.
    assert!(matches!(
        check_application_name(&"a".repeat(48)),
        Err(Error::InvalidName { .. })
    ));
}

#[test]
fn the_same_application_is_isolated_by_destination_fleet() {
    let first_name = scratch_fleet();
    let first = Scratch(TransportSegment::for_fleet(&first_name).unwrap());
    let second =
        Scratch(TransportSegment::for_fleet(&scratch_fleet()).unwrap());
    let caller = TransportSegment::for_fleet(&first_name).unwrap();
    first.0.inbox().claim_lane(0, "example", "php", 1).unwrap();
    assert_eq!(first.0.inbox().targets("example").len(), 1);
    assert!(second.0.inbox().targets("example").is_empty());
    second.0.inbox().claim_lane(1, "example", "php", 1).unwrap();
    assert_eq!(second.0.inbox().targets("example"), vec![(1, 0)]);
    assert_eq!(first.0.inbox().targets("example"), vec![(0, 0)]);
    assert_eq!(caller.inbox().targets("example"), vec![(0, 0)]);
}

/// A lane whose owner is gone — its lock let go, which is what the kernel does
/// when a process dies — is taken back by the next process to join, and the
/// body streams the gone process held end for whoever holds their other side.
/// A live lane is left alone.
#[test]
fn the_next_join_takes_back_a_gone_process_lane_and_ends_its_streams() {
    use std::sync::Arc;

    use orbitive::{Fleet, NodeId};

    use crate::transport::TransportBodies;

    let fleet: &'static str = Box::leak(scratch_fleet().into_boxed_str());
    let segment = Scratch::open(fleet, 4);
    let live = segment.0.join("checkout", "asgi", 1).unwrap();
    let gone = segment.0.join("checkout", "asgi", 7).unwrap();
    assert_eq!((live.lane, gone.lane), (0, 1));

    let bodies = |lane: usize, incarnation: u64| {
        let fleet =
            Fleet::join_shm_as(fleet, 4, NodeId::new(lane as u16)).unwrap();
        TransportBodies::open(Arc::new(fleet), incarnation).unwrap()
    };
    let live_bodies = bodies(0, 1);
    let gone_bodies = bodies(1, 7);
    let (held, ticket) = live_bodies.create().unwrap();
    let _far = gone_bodies.streams().open(ticket).unwrap();

    drop(gone.hold);
    assert_eq!(
        segment.0.inbox().consumers(1).unwrap(),
        1,
        "death alone leaves the lane claimed"
    );

    let next = segment.0.join("checkout", "asgi", 2).unwrap();
    assert_eq!(
        next.reclaimed,
        vec![Reclaimed { lane: 1, incarnation: 7, pid: std::process::id() }]
    );
    assert_eq!(next.lane, 1, "the lane taken back is free again");
    assert_eq!(
        segment.0.inbox().incarnation(0).unwrap(),
        1,
        "the live lane is untouched"
    );

    live_bodies.reclaimed(&next.reclaimed);
    let mut buffer = [0_u8; 4];
    assert!(matches!(
        held.try_read(&mut buffer),
        Err(orbitive::stream::Error::Reset)
    ));
    drop(live);
    let _ = live_bodies.unlink();
}
