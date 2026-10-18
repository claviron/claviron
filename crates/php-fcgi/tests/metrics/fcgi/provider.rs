use super::*;

/// A freshness bound below the cadence the rows are written at
/// declares every sample stale for most of every period, and the
/// family renders empty. Measured on a running fleet before this:
/// `interval: 15s` with the default `max_age: 5s` showed the
/// `claviron_fpm_*` rows for about six seconds in every fifteen.
#[test]
fn the_bound_never_falls_below_the_cadence_it_judges() {
    // The case that was live: a 5s bound against a 15s scrape.
    let bound =
        freshness_bound(Duration::from_secs(5), Duration::from_secs(15));
    assert!(
        bound >= Duration::from_secs(15),
        "a sample is a whole period old by construction"
    );
    assert_eq!(bound, Duration::from_secs(30));

    // The FCGI family, whose cadence is already well under max_age:
    // the operator's floor stands.
    assert_eq!(
        freshness_bound(Duration::from_secs(5), Duration::from_secs(1)),
        Duration::from_secs(5)
    );

    // A generous operator floor is never narrowed to the cadence.
    assert_eq!(
        freshness_bound(Duration::from_secs(120), Duration::from_secs(15)),
        Duration::from_secs(120)
    );
}
