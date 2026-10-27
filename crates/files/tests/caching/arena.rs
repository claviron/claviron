use super::*;

#[test]
fn regression_arena_validators_retain_the_same_timestamp_precision() {
    let modified = UNIX_EPOCH
        + Duration::from_secs(1_700_000_000)
        + Duration::from_nanos(100);
    let first = etag(4, stamp(modified).unwrap());
    assert_eq!(
        first,
        crate::mime::make_etag(4, modified.duration_since(UNIX_EPOCH).unwrap())
    );
    assert_ne!(
        first,
        etag(4, stamp(modified + Duration::from_nanos(1)).unwrap())
    );
    assert_eq!(stamp(UNIX_EPOCH + Duration::from_secs(1 << 35)), None);
}
