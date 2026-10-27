use super::*;
use crate::state::layout::OriginSlot;
use std::sync::atomic::{AtomicU8, AtomicU32};

#[test]
fn audit_health_wire_bytes_and_legacy_offsets_are_preserved() {
    #[repr(C)]
    struct Legacy {
        key_hash: AtomicU64,
        key: [AtomicU8; 96],
        config_generation: AtomicU64,
        health: AtomicU8,
        serving: AtomicU8,
        lifecycle: AtomicU8,
        failures: AtomicU32,
        healthy_since: AtomicU64,
        in_flight: AtomicU32,
        ewma_micros: AtomicU32,
        reserved: [u8; 24],
    }
    assert_eq!(size_of::<OriginSlot>(), size_of::<Legacy>());
    assert_eq!(align_of::<OriginSlot>(), align_of::<Legacy>());
    assert_eq!(
        std::mem::offset_of!(OriginSlot, verdict),
        std::mem::offset_of!(Legacy, health)
    );
    assert_eq!(
        std::mem::offset_of!(Legacy, failures),
        std::mem::offset_of!(OriginSlot, verdict) + 4
    );
    assert_eq!(
        std::mem::offset_of!(OriginSlot, healthy_since),
        std::mem::offset_of!(Legacy, healthy_since)
    );
    assert_eq!(
        std::mem::offset_of!(OriginSlot, in_flight),
        std::mem::offset_of!(Legacy, in_flight)
    );
    assert_eq!(
        std::mem::offset_of!(OriginSlot, ewma_micros),
        std::mem::offset_of!(Legacy, ewma_micros)
    );
    let verdict = Verdict::new();
    let before = verdict.load();
    assert_eq!(before.0.to_ne_bytes(), [0, 1, 1, 0, 0, 0, 0, 0]);
    let after = before.with_health(2, 0x01020304);
    verdict.compare_exchange(before, after).unwrap();
    verdict.serving_bits(2, 1);
    let bytes = verdict.load().0.to_ne_bytes();
    assert_eq!(&bytes[..4], &[2, 2, 1, 0]);
    assert_eq!(&bytes[4..], &0x01020304u32.to_ne_bytes());
    assert_eq!(verdict.load().failures(), 0x01020304);
}
