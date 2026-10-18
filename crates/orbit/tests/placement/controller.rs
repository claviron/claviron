use super::*;
use crate::placement::Scope;
use std::num::NonZeroU16;
use std::time::Duration;

fn policy() -> DispatchPolicy {
    DispatchPolicy {
        capacity: NonZeroU16::new(3).unwrap(),
        lease_ttl: Duration::from_secs(20),
        renewal_interval: Duration::from_secs(5),
    }
}

fn request(
    dispatch: Dispatch,
    policy: Option<DispatchPolicy>,
) -> DispatchRequest {
    DispatchRequest {
        name: "ExampleService",
        generation: 1,
        scope: Scope::Worker,
        dispatch,
        policy,
    }
}

#[test]
fn service_policy_needs_no_named_or_shared_configuration() {
    let dispatcher = OrbitDispatcher::new();
    for selection in [Dispatch::One, Dispatch::Pool] {
        let request = request(selection, Some(policy()));
        assert_eq!(dispatcher.policy(&request).unwrap(), policy());
        request.dispatch.validate_policy(request.policy).unwrap();
    }
}

#[test]
fn generation_keeps_its_declared_policy_and_invalid_timing_is_rejected() {
    let dispatcher = OrbitDispatcher::new();
    let original = request(Dispatch::Pool, Some(policy()));
    let mut next = original;
    next.policy.as_mut().unwrap().capacity = NonZeroU16::new(4).unwrap();
    assert_eq!(dispatcher.policy(&original).unwrap().capacity.get(), 3);
    assert_eq!(dispatcher.policy(&next).unwrap().capacity.get(), 4);

    for renewal in [Duration::ZERO, policy().lease_ttl] {
        next.policy.as_mut().unwrap().renewal_interval = renewal;
        assert!(next.dispatch.validate_policy(next.policy).is_err());
    }
}

#[test]
fn coordinated_selection_requires_policy_but_each_does_not() {
    let dispatcher = OrbitDispatcher::new();
    for selection in [Dispatch::One, Dispatch::Pool] {
        let request = request(selection, None);
        assert!(dispatcher.policy(&request).is_err());
        assert!(request.dispatch.validate_policy(request.policy).is_err());
    }
    Dispatch::Each.validate_policy(None).unwrap();
}
