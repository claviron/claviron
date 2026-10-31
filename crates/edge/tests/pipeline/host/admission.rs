use std::cell::Cell;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use super::*;

#[test]
fn admission_without_capacity_or_deadline_needs_no_runtime_or_backend_probe() {
    let admission = Admission::default();
    let entering = {
        let policy = AdmissionPolicy::new(RequestPolicy {
            queue_timeout: Some(Duration::from_secs(1)),
            ..RequestPolicy::unlimited()
        });
        assert_eq!(policy.head_timeout_for(|| panic!("no head timing probe")), None);
        policy.enter(&admission)
    };
    let mut entering = pin!(entering);
    let Poll::Ready(Some(_place)) = entering.as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("unlimited capacity must not wait or create a queue timer");
    };
}

#[test]
fn prepared_head_deadline_keeps_backend_timing_ownership() {
    let deadline = Duration::from_secs(1);
    let policy = AdmissionPolicy::new(RequestPolicy {
        head_timeout: Some(deadline),
        ..RequestPolicy::unlimited()
    });
    let probes = Cell::new(0);
    for owns in [false, true] {
        assert_eq!(policy.head_timeout_for(|| {
            probes.set(probes.get() + 1);
            owns
        }), if owns { None } else { Some(deadline) });
    }
    assert_eq!(probes.get(), 2);
}

#[test]
fn cancelled_queue_waiter_does_not_retain_the_applications_capacity() {
    let policy = AdmissionPolicy::new(RequestPolicy::unlimited());
    let admission = Admission::limited(Some(1));
    let mut cx = Context::from_waker(Waker::noop());
    let mut first = pin!(policy.enter(&admission));
    let Poll::Ready(Some(place)) = first.as_mut().poll(&mut cx) else {
        panic!("the first request must acquire capacity");
    };
    let mut queued = Box::pin(policy.enter(&admission));
    assert!(queued.as_mut().poll(&mut cx).is_pending());
    drop(queued);
    drop(place);
    let mut next = pin!(policy.enter(&admission));
    assert!(matches!(next.as_mut().poll(&mut cx), Poll::Ready(Some(_))));
}
