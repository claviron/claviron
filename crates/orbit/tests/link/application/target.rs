use super::*;

#[repr(align(64))]
#[derive(Clone)]
struct Block([u8; 64]);

struct Memory {
    inbox: Inbox,
    _bytes: Vec<Block>,
}

fn memory() -> Memory {
    let geometry = orbitive::link::InboxGeometry::new(4, 128);
    let mut bytes =
        vec![Block([0; 64]); geometry.segment_bytes(4).div_ceil(64)];
    // SAFETY: zeroed, 64-byte aligned storage, sized for this geometry and
    // retained until after the inbox is dropped. No interprocess publisher.
    let inbox =
        unsafe { Inbox::initialize(bytes[0].0.as_mut_ptr(), 4, geometry) }
            .unwrap();
    Memory { inbox, _bytes: bytes }
}

#[test]
fn prepared_lanes_follow_join_release_and_reuse_without_retaining_an_old_name()
{
    let memory = memory();
    let inbox = &memory.inbox;
    let target = Lanes::new(inbox, "example-app");
    assert!(target.load(inbox, "example-app").lanes.is_empty());

    inbox.claim_lane(1, "example-app", "rust", 1).unwrap();
    inbox.claim_lane(2, "example-app", "rust", 1).unwrap();
    let joined = target.load(inbox, "example-app");
    assert_eq!(joined.lanes, [1, 2]);
    assert_eq!(joined.generation, inbox.directory());

    inbox.release_lane(1).unwrap();
    inbox.claim_lane(1, "other-app", "rust", 2).unwrap();
    assert_eq!(target.load(inbox, "example-app").lanes, [2]);

    inbox.release_lane(2).unwrap();
    assert!(target.load(inbox, "example-app").lanes.is_empty());
    inbox.claim_lane(2, "example-app", "rust", 3).unwrap();
    assert_eq!(target.load(inbox, "example-app").lanes, [2]);
    assert_eq!(joined.lanes, [1, 2], "in-flight snapshots remain immutable");
}

#[test]
fn prepared_membership_is_shared_while_depth_and_admission_stay_live() {
    let memory = memory();
    let inbox = &memory.inbox;
    inbox.claim_lane(1, "example-app", "rust", 1).unwrap();
    let target = Lanes::new(inbox, "example-app");
    let before = target.load(inbox, "example-app");
    assert_eq!(inbox.ready(&before.lanes), [(1, 0)]);

    for _ in 0..4 {
        inbox.write(1, b"request").unwrap();
    }
    let full = target.load(inbox, "example-app");
    assert!(Arc::ptr_eq(&before, &full));
    assert!(inbox.ready(&full.lanes).is_empty());

    assert!(inbox.read(1, &mut Vec::new()).unwrap());
    let ready = target.load(inbox, "example-app");
    assert!(Arc::ptr_eq(&before, &ready));
    assert_eq!(inbox.ready(&ready.lanes), [(1, 3)]);
}
