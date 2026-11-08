use super::*;

use crate::proxy::test_profile as fixture;

#[test]
fn audit_upstream_warm_h1_lazy_floor_starts_at_real_demand() {
    let profile = fixture::profile("h1", "{max_live: 3, min_idle: 2}");
    let mut pool = TargetPool::new(profile, PoolGeneration(1));
    assert!(!pool.needs_replenish());
    assert!(matches!(pool.take(), Ok(Take::Connect(_))));
    assert_eq!(pool.live(), 1);
    assert!(pool.needs_replenish());
    assert!(matches!(pool.take(), Ok(Take::Connect(_))));
    assert_eq!(pool.live(), 2);
    assert!(!pool.needs_replenish());
    pool.close();
    assert!(!pool.needs_replenish());
    assert!(matches!(pool.take(), Err(RequestFailure::Closed)));
}

#[test]
fn audit_upstream_warm_h1_opt_in_prepares_only_the_declared_floor() {
    let profile =
        fixture::profile("h1", "{max_live: 3, min_idle: 2, warmup: true}");
    let pool = TargetPool::new(profile, PoolGeneration(1));
    assert!(pool.needs_replenish());
    let profile = fixture::profile("h1", "{max_live: 3, warmup: true}");
    let pool = TargetPool::new(profile, PoolGeneration(1));
    assert!(!pool.needs_replenish());
}
