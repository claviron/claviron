use super::*;

#[test]
fn recovery_caps_growth_and_warning_transitions_and_resets_on_success() {
    let policy = AcceptRetryConfig {
        initial: Duration::from_millis(50),
        max: Duration::from_secs(1),
    };
    let mut recovery = Recovery::default();
    let now = Instant::now();
    let mut warnings = 0;
    for attempt in 0..10_000 {
        warnings += usize::from(recovery.failed(&policy, now));
        assert!(
            recovery.delay >= policy.initial && recovery.delay <= policy.max
        );
        if attempt > 5 {
            assert_eq!(recovery.delay, policy.max);
        }
    }
    assert_eq!(warnings, 1, "no warning or retained history per retry");
    assert_eq!(recovery.recovered().unwrap().0, 10_000);
    assert!(recovery.recovered().is_none(), "one summary per episode");
    assert!(recovery.failed(&policy, now));
    assert_eq!(recovery.delay, policy.initial);

    recovery.failures = u64::MAX;
    recovery.delay = Duration::MAX;
    assert!(!recovery.failed(&policy, now));
    assert_eq!(recovery.failures, u64::MAX);
    assert_eq!(recovery.delay, policy.max);
}
