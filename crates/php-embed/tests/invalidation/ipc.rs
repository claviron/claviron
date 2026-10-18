use super::*;

#[test]
fn cache_invalidation_codec_preserves_operation_and_rejects_invalid_frames() {
    for operation in [
        OwnedInvalEvent::Delete("key".into()),
        OwnedInvalEvent::DeletePattern("pattern".into()),
        OwnedInvalEvent::ApcuClearCache,
        OwnedInvalEvent::OpcacheReset,
        OwnedInvalEvent::OpcacheInvalidate("file.php".into(), false),
        OwnedInvalEvent::OpcacheInvalidate("file.php".into(), true),
    ] {
        let event =
            PhpCacheInvalidationObserved { source_worker: 2, operation };
        assert_eq!(
            PhpCacheInvalidationObserved::decode_orbit(&event.encode_orbit()),
            Ok(event)
        );
    }
    for frame in [
        &[][..],
        &[1][..],
        &[0, 1][..],
        &[1, 99][..],
        &[1, 3, 42][..],
        &[1, 1, 255][..],
    ] {
        assert!(PhpCacheInvalidationObserved::decode_orbit(frame).is_err());
    }
}
