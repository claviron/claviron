//! Frozen kind-222 ABI; keys are installed once, all concurrent fields atomic.
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};

// A distinct physical ABI is never selected by runtime policy.
pub(crate) const UPSTREAM_STATE_KIND: u8 = 222;
/// Fleet wire geometry: every peer must be built with the same power of two.
pub(crate) const CAPACITY: usize = orbitive::core::compile::usize_from_env(
    option_env!("ORBIT_UPSTREAM_STATE_CAPACITY"),
    1024,
);
// The 96-byte key region is part of kind 222's frozen slot ABI.
pub(crate) const KEY_MAX: usize = 96;
// Header magic distinguishes this table from rings and unrelated tables.
const MAGIC: u32 = 0x4355_5354;
// Version changes require an explicitly migrated physical kind.
const VERSION: u16 = 1;
#[repr(C, align(64))]
pub(super) struct Header {
    magic: u32,
    version: u16,
    header_size: u16,
    capacity: u32,
    slot_size: u32,
    reserved: [u8; 48],
}
impl Header {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            header_size: size_of::<Self>() as u16,
            capacity: capacity as u32,
            slot_size: size_of::<OriginSlot>() as u32,
            reserved: [0; 48],
        }
    }
    pub(super) fn validate(
        &self,
        capacity: usize,
    ) -> std::io::Result<()> {
        if self.magic != MAGIC
            || self.version != VERSION
            || self.header_size as usize != size_of::<Self>()
            || self.capacity as usize != capacity
            || self.slot_size as usize != size_of::<OriginSlot>()
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "incompatible upstream state layout: magic={:#x} version={} header={} capacity={} slot={}; expected magic={MAGIC:#x} version={VERSION} header=64 capacity={capacity} slot=160",
                    self.magic,
                    self.version,
                    self.header_size,
                    self.capacity,
                    self.slot_size
                ),
            ));
        }
        Ok(())
    }
}
#[repr(C)]
pub(crate) struct OriginSlot {
    // Zero means empty; release publishes all key bytes, acquire makes them visible.
    pub(super) key_hash: AtomicU64,
    pub(super) key: [AtomicU8; KEY_MAX],
    pub(crate) config_generation: AtomicU64,
    pub(crate) health: AtomicU8,
    pub(crate) serving: AtomicU8,
    pub(crate) lifecycle: AtomicU8,
    pub(crate) failures: AtomicU32,
    pub(crate) healthy_since: AtomicU64,
    pub(crate) in_flight: AtomicU32,
    pub(crate) ewma_micros: AtomicU32,
    // The fields above occupy 136 bytes, not the plan's stated 160. Reserve
    // the remaining bytes explicitly so the installed kind's ABI is 160 bytes.
    reserved: [u8; 24],
}
impl OriginSlot {
    pub(crate) fn empty() -> Self {
        Self {
            key_hash: AtomicU64::new(0),
            key: std::array::from_fn(|_| AtomicU8::new(0)),
            config_generation: AtomicU64::new(0),
            health: AtomicU8::new(0),
            serving: AtomicU8::new(1),
            lifecycle: AtomicU8::new(1),
            failures: AtomicU32::new(0),
            healthy_since: AtomicU64::new(0),
            in_flight: AtomicU32::new(0),
            ewma_micros: AtomicU32::new(0),
            reserved: [0; 24],
        }
    }
    pub(super) fn matches(
        &self,
        hash: u64,
        key: &[u8],
    ) -> bool {
        self.key_hash.load(Ordering::Acquire) == hash
            && self.key.iter().enumerate().all(|(i, b)| {
                b.load(Ordering::Relaxed) == key.get(i).copied().unwrap_or(0)
            })
    }
    pub(crate) fn install(
        &self,
        hash: u64,
        key: &[u8],
        generation: u64,
    ) {
        for (i, b) in self.key.iter().enumerate() {
            b.store(key.get(i).copied().unwrap_or(0), Ordering::Relaxed);
        }
        self.config_generation.store(generation, Ordering::Relaxed);
        self.serving.store(1, Ordering::Relaxed);
        self.lifecycle.store(1, Ordering::Relaxed);
        self.healthy_since.store(now_millis(), Ordering::Relaxed);
        self.key_hash.store(hash, Ordering::Release);
    }
    pub(crate) fn reset(&self) {
        self.health.store(0, Ordering::Relaxed);
        self.serving.store(1, Ordering::Relaxed);
        self.lifecycle.store(1, Ordering::Relaxed);
        self.failures.store(0, Ordering::Relaxed);
        self.config_generation.store(0, Ordering::Relaxed);
        self.healthy_since.store(0, Ordering::Relaxed);
        self.in_flight.store(0, Ordering::Relaxed);
        self.ewma_micros.store(0, Ordering::Relaxed);
        for byte in &self.key {
            byte.store(0, Ordering::Relaxed);
        }
        self.key_hash.store(0, Ordering::Release);
    }
}
pub(crate) fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}
const _: () =
    assert!(CAPACITY.is_power_of_two() && CAPACITY <= u32::MAX as usize);
const _: () =
    assert!(size_of::<Header>() == 64 && size_of::<OriginSlot>() == 160);
