use super::*;

#[test]
fn physical_shape_and_bound_are_validated_before_join() {
    let mut config = LinkConfig::default();
    config.geometry.inbox_capacity = 4;
    config.geometry.lane_capacity = 2;
    config.geometry.buffer_bytes = 1024;
    config.max_in_flight = Some(4);
    config.validate().unwrap();
    assert_eq!(config.geometry.spec().inbox.capacity, 4);
    assert_eq!(config.geometry.spec().streams.lane_capacity, 2);
    config.max_in_flight = Some(5);
    assert!(config.validate().is_err());
    config.max_in_flight = Some(1);
    config.geometry.lane_capacity = 3;
    assert!(config.validate().is_err());
}

#[test]
fn live_control_preserves_existing_work_and_owns_no_join() {
    let control = LinkCapacity::new(None);
    let config = LinkConfig {
        max_in_flight: Some(1),
        geometry: LinkGeometry {
            fleet_capacity: 4,
            inbox_capacity: 4,
            inbox_payload_capacity: 1024,
            lane_capacity: 4,
            buffer_bytes: 1024,
        },
    };
    control.prepare(&config, None).unwrap();
    let fleet = format!("lc{:x}", std::process::id());
    let (segment, joined) = control.join(&fleet, "example", "rust", 1).unwrap();
    let origin = segment.join("caller", "edge", 2).unwrap();
    let reservation =
        segment.reserve_work(joined.lane, origin.lane).unwrap().unwrap();
    let execution =
        segment.accept_work(joined.lane, reservation.ticket()).unwrap();
    control.set_max_in_flight(2).unwrap();
    let next = segment.reserve_work(joined.lane, origin.lane).unwrap().unwrap();
    control.set_max_in_flight(1).unwrap();
    assert!(segment.reserve_work(joined.lane, origin.lane).is_err());
    drop(next);
    assert!(segment.reserve_work(joined.lane, origin.lane).is_err());
    drop(execution);
    assert!(segment.reserve_work(joined.lane, origin.lane).is_ok());
    let weak = Arc::downgrade(&joined);
    segment.inbox().release_lane(joined.lane).unwrap();
    drop(joined);
    assert!(weak.upgrade().is_none());
    segment.inbox().release_lane(origin.lane).unwrap();
    drop(origin);
    segment.unlink().unwrap();
}
