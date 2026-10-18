use super::super::TransportSegment;
use super::*;

/// An edge that restarts takes the same lane back under the same name, and
/// its release and claim can fall between two looks as one directory
/// generation. Only the whole identity tells the two processes apart, so the
/// look after it must report one going and one coming, not nothing.
#[test]
fn a_lane_retaken_between_two_looks_is_a_departure_and_an_arrival() {
    let fleet = format!("px{}", std::process::id());
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
