use super::*;
use crate::link::application::{Access, Exchange};

fn request() -> Vec<u8> {
    Exchange::default().encode().unwrap()
}

/// A receiver with a window of 4 that runs 2, and one caller.
fn receiver(tag: &str) -> (std::sync::Arc<ApplicationSegment>, usize, usize) {
    let name = format!("wq{tag}{:x}", std::process::id());
    let spec = orbitive::link::LinkSpec::new(
        4,
        crate::link::application::LINK_SPEC.inbox_kind,
        orbitive::link::InboxGeometry::new(8, 1024),
        orbitive::stream::StreamSpec::new(
            crate::link::application::LINK_SPEC.streams.kind,
            4,
            1024,
        ),
    );
    let segment = std::sync::Arc::new(
        ApplicationSegment::for_fleet_with_spec(
            &name,
            spec,
            &Access::OwnerOnly,
        )
        .unwrap(),
    );
    let worker =
        segment.join_with_capacity("example", "rust", 1, Some(4)).unwrap();
    segment.set_work_execution(worker.lane, 2).unwrap();
    let caller = segment.join("caller", "edge", 2).unwrap();
    std::mem::forget(worker.hold);
    std::mem::forget(caller.hold);
    (segment, worker.lane, caller.lane)
}

#[test]
fn a_receiver_runs_its_execution_queues_the_rest_and_passes_withdrawn_entries()
{
    let (segment, lane, origin) = receiver("a");
    let held: Vec<_> = (0..4)
        .map(|_| segment.reserve_work(lane, origin).unwrap().unwrap())
        .collect();
    let tickets: Vec<_> =
        held.iter().map(|reservation| reservation.ticket()).collect();
    let mut queue = WorkQueue::new();
    let mut running = Vec::new();
    for ticket in &tickets {
        match queue
            .offer(&segment, lane, Received::Work(*ticket), request())
            .unwrap()
        {
            Offered::Start(start) => running.push(start),
            Offered::Held => {}
        }
    }
    assert_eq!((queue.running(), queue.waiting()), (2, 2));

    // The third's sender withdraws it at its deadline; it never starts.
    assert!(segment.inbox().withdraw_work(tickets[2]));
    running.pop();
    let next = queue.finished(&segment, lane).expect("the fourth starts");
    assert!(next.1.is_some());
    assert_eq!((queue.running(), queue.waiting()), (2, 0));
    // A started exchange can no longer be withdrawn.
    assert!(!segment.inbox().withdraw_work(tickets[3]));
    assert!(queue.finished(&segment, lane).is_none());
    assert_eq!(queue.running(), 1);
    let _ = segment.unlink();
}
