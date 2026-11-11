use super::*;

#[test]
fn omitted_and_null_bounds_do_not_inherit_the_physical_inbox_capacity() {
    for yaml in ["{}", "max_in_flight: null"] {
        let config: LinkConfig = serde_yaml::from_str(yaml).unwrap();
        let control = LinkCapacity::new(None, None);
        control.prepare(&config, None).unwrap();
        assert_eq!(control.limit().unwrap(), None);
        assert_eq!(control.geometry().unwrap(), config.geometry);
    }
}

#[test]
fn explicit_bound_overrides_the_engine_fallback() {
    let mut config = LinkConfig::default();
    config.geometry.inbox_capacity = 4;
    config.geometry.lane_capacity = 2;
    let control = LinkCapacity::new(None, None);
    // A bound from the engine needs a declared max_waiting like any other.
    assert!(control.prepare(&config, Some(2)).is_err());
    config.max_waiting = Some(3);
    control.prepare(&config, Some(2)).unwrap();
    assert_eq!(control.limit().unwrap(), Some(2));
    config.max_in_flight = Some(3);
    control.prepare(&config, Some(2)).unwrap();
    assert_eq!(control.limit().unwrap(), Some(3));
    config.max_in_flight = None;
    assert!(control.prepare(&config, Some(4)).is_err());
    assert_eq!(control.limit().unwrap(), Some(3));
    assert!(control.prepare(&config, Some(0)).is_err());
}

#[test]
fn physical_shape_and_bound_are_validated_before_join() {
    let mut config = LinkConfig::default();
    config.geometry.inbox_capacity = 4;
    config.geometry.lane_capacity = 2;
    config.geometry.buffer_bytes = 1024;
    config.max_in_flight = Some(3);
    assert!(config.validate().is_err(), "a bound without max_waiting");
    config.max_waiting = Some(3);
    config.validate().unwrap();
    assert_eq!(config.geometry.spec().inbox.capacity, 4);
    assert_eq!(config.geometry.spec().streams.lane_capacity, 2);
    // The inbox also carries control frames: a bound equal to it is refused.
    config.max_in_flight = Some(4);
    config.max_waiting = Some(4);
    assert!(config.validate().is_err());
    // What is admitted in all stays below the inbox and covers what runs.
    config.max_in_flight = Some(2);
    assert!(config.validate().is_err());
    config.max_waiting = Some(1);
    assert!(config.validate().is_err());
    config.max_waiting = Some(3);
    config.validate().unwrap();
    config.max_in_flight = Some(1);
    config.max_waiting = Some(1);
    config.geometry.lane_capacity = 3;
    assert!(config.validate().is_err());
    // A sender's inbox must take the replies to all its open exchanges.
    config.geometry.lane_capacity = 4;
    assert!(config.validate().is_err());
}

#[test]
fn live_control_preserves_existing_work_and_owns_no_join() {
    let control = LinkCapacity::new(None, None);
    let config = LinkConfig {
        max_in_flight: Some(1),
        max_waiting: Some(3),
        geometry: LinkGeometry {
            fleet_capacity: 4,
            inbox_capacity: 4,
            inbox_payload_capacity: 1024,
            lane_capacity: 2,
            buffer_bytes: 1024,
        },
        workers: None,
    };
    control.prepare(&config, None).unwrap();
    let fleet = format!("lc{:x}", std::process::id());
    let (segment, joined) = control.join(&fleet, "example", "rust", 1).unwrap();
    let origin = segment.join("caller", "edge", 2).unwrap();
    let reservation =
        segment.reserve_work(joined.lane, origin.lane).unwrap().unwrap();
    let execution =
        segment.accept_work(joined.lane, reservation.ticket()).unwrap();
    // A live bound moves how many run at once; the admitted window stays.
    control.set_max_in_flight(2).unwrap();
    assert_eq!(segment.inbox().work_execution(joined.lane).unwrap(), Some(2));
    assert_eq!(segment.inbox().work_capacity(joined.lane).unwrap(), Some(3));
    assert!(control.set_max_in_flight(4).is_err(), "beyond max_waiting");
    let next = segment.reserve_work(joined.lane, origin.lane).unwrap().unwrap();
    let last = segment.reserve_work(joined.lane, origin.lane).unwrap().unwrap();
    assert!(segment.reserve_work(joined.lane, origin.lane).is_err());
    drop((next, last));
    assert!(segment.reserve_work(joined.lane, origin.lane).is_ok());
    drop(execution);
    let weak = Arc::downgrade(&joined);
    segment.inbox().release_lane(joined.lane).unwrap();
    drop(joined);
    assert!(weak.upgrade().is_none());
    segment.inbox().release_lane(origin.lane).unwrap();
    drop(origin);
    segment.unlink().unwrap();
}

#[test]
fn a_growth_policy_is_declared_whole_and_fits_its_fleet() {
    let yaml = "geometry: { fleet_capacity: 8, inbox_capacity: 16, inbox_payload_capacity: 1024, lane_capacity: 8, buffer_bytes: 1024 }\n\
                workers: { policy: dynamic, min: 2, max: 6, step: 1, retire_idle: off }";
    let config: LinkConfig = serde_yaml::from_str(yaml).unwrap();
    config.validate().unwrap();
    let growth = config.workers.unwrap().growth(40).unwrap();
    assert_eq!((growth.min, growth.max, growth.step), (2, 6, 1));

    // `on_pressure` is the earlier spelling of `dynamic`, still accepted.
    let named = "workers: { policy: on_pressure, min: 2, max: available, step: 1, retire_idle: off }";
    let named: LinkConfig = serde_yaml::from_str(named).unwrap();
    named.validate().unwrap();
    assert_eq!(named.workers.unwrap().growth(40).unwrap().max, 40);

    let mut stepped = config.clone();
    let Workers::Dynamic(policy) = stepped.workers.as_mut().unwrap() else {
        panic!("growth")
    };
    policy.step = 2;
    assert!(stepped.validate().is_err(), "one worker per episode");

    let mut crowded = config.clone();
    let Workers::Dynamic(policy) = crowded.workers.as_mut().unwrap() else {
        panic!("growth")
    };
    policy.max = WorkerMax::Count(8);
    assert!(crowded.validate().is_err(), "a caller needs a lane too");

    let partial = "workers: { policy: dynamic, min: 2, max: 6 }";
    assert!(serde_yaml::from_str::<LinkConfig>(partial).is_err());
}

#[test]
fn worker_count_has_one_owner_and_retirement_reserves_its_control_lane() {
    let fixed: Workers =
        serde_yaml::from_str("{policy: static, count: 4, retire_idle: off}")
            .unwrap();
    assert_eq!(fixed, Workers::Static { count: 4 });
    assert_eq!(fixed.initial_count(), 4);
    assert_eq!(fixed.for_owner(40, 128, 4).unwrap(), None);
    assert!(fixed.for_owner(40, 128, 3).is_err());
    assert!(
        serde_yaml::from_str::<Workers>("{policy: static, count: 0}").is_err()
    );
    assert!(
        serde_yaml::from_str::<Workers>("{policy: static, min: 4, max: 8}")
            .is_err()
    );
    let growing: Workers = serde_yaml::from_str("{policy: dynamic, min: 2, max: 6, step: 1, retire_idle: {idle_timeout: 7s}}").unwrap();
    assert_eq!(growing.initial_count(), 2);
    assert!(
        growing.for_owner(40, 7, 2).is_err(),
        "caller and idle observer need lanes"
    );
    assert!(growing.for_owner(40, 8, 2).is_ok());
    let encoded = serde_yaml::to_string(&growing).unwrap();
    assert!(encoded.contains("policy: dynamic"), "{encoded}");
    assert_eq!(serde_yaml::from_str::<Workers>(&encoded).unwrap(), growing);
    assert!(serde_yaml::from_str::<Workers>("{policy: dynamic, min: 2, max: 6, step: 1, retire_idle: {idle_timeout: 0s}}").is_err());
}
