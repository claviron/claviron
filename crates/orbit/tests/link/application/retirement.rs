use super::*;

#[test]
fn only_the_declared_idle_period_can_expire_and_activity_restarts_it() {
    let start = Instant::now();
    let timeout = Duration::from_secs(7);
    let mut clock = IdleClock::default();
    assert_eq!(clock.observe(start, 1, false, timeout), None);
    assert_eq!(clock.observe(start, 1, true, timeout), Some(start + timeout));
    assert_eq!(
        clock.observe(start + Duration::from_secs(2), 1, true, timeout),
        Some(start + timeout)
    );
    // Work can arrive and complete between observations: its release advances
    // the capacity generation even when the observed occupancy is zero again.
    let later = start + Duration::from_secs(3);
    assert_eq!(clock.observe(later, 2, true, timeout), Some(later + timeout));
    assert_eq!(clock.observe(later, 2, false, timeout), None);
}
