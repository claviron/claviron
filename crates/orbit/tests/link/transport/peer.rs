use super::super::TransportSegment;
use super::*;

fn fleet_name(mut pid: u32) -> String {
    // Six base-64 digits retain all 32 PID bits and leave room for this fixture's
    // prefix. Every alphabet byte is allowed by FleetName; nothing is truncated.
    let alphabet =
        b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-_";
    let mut name = *b"px000000";
    for byte in name[2..].iter_mut().rev() {
        *byte = alphabet[(pid & 63) as usize];
        pid >>= 6;
    }
    std::str::from_utf8(&name).unwrap().to_owned()
}

#[test]
fn fleet_names_remain_valid_and_distinct_across_pid_boundaries() {
    let mut names = std::collections::BTreeSet::new();
    for pid in [
        0,
        1,
        63,
        64,
        999_998,
        999_999,
        1_000_000,
        1_000_001,
        9_999_999,
        10_000_000,
        0x3fff_ffff,
        0x4000_0000,
        0x7fff_ffff,
        0x8000_0000,
        u32::MAX - 1,
        u32::MAX,
    ] {
        let name = fleet_name(pid);
        assert_eq!(name.len(), FleetName::MAX_LEN);
        assert!(FleetName::try_new(&name).is_ok(), "PID {pid}: {name}");
        assert!(names.insert(name), "PID {pid} collided with another PID");
    }
}

/// An edge that restarts takes the same lane back under the same name, and
/// its release and claim can fall between two looks as one directory
/// generation. Only the whole identity tells the two processes apart, so the
/// look after it must report one going and one coming, not nothing.
#[test]
fn a_lane_retaken_between_two_looks_is_a_departure_and_an_arrival() {
    let fleet = fleet_name(std::process::id());
    let segment = TransportSegment::open(&fleet, 2).unwrap();
    let inbox = segment.inbox();
    let mut peers = Peers::new(FleetName::new(&fleet), |role| role == "edge");

    inbox.claim_lane(0, "web", "edge", 1).unwrap();
    inbox.claim_lane(1, "shop", "php", 1).unwrap();
    let (came, went) = peers.update(inbox);
    assert_eq!(came.len(), 1);
    assert_eq!((came[0].lane, came[0].incarnation), (0, 1));
    assert!(went.is_empty());

    inbox.release_lane(0).unwrap();
    inbox.claim_lane(0, "web", "edge", 2).unwrap();
    let (came, went) = peers.update(inbox);
    assert_eq!(
        went.iter().map(|peer| peer.incarnation).collect::<Vec<_>>(),
        [1]
    );
    assert_eq!(
        came.iter().map(|peer| peer.incarnation).collect::<Vec<_>>(),
        [2]
    );

    let _ = segment.unlink();
}
