use super::*;

#[test]
fn audit_upstream_warm_h2_job_is_single_flight_and_releases_on_drop() {
    let profile = crate::proxy::test_profile::profile(
        "h2",
        "{max_live: 4, max_idle: 2, warmup: true}",
    );
    let pool = Arc::new(H2TargetPool::new(
        profile,
        crate::origin::PoolGeneration(1),
        crate::origin::OriginId(0),
    ));
    let job = WarmJob::claim(&pool).unwrap();
    assert!(WarmJob::claim(&pool).is_none());
    drop(job);
    let job = WarmJob::claim(&pool).unwrap();
    pool.close();
    drop(job);
    assert!(WarmJob::claim(&pool).is_none());
}
