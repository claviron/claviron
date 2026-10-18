//! Unified DNS publication and reusable health; incompatible with retained 245.
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64};

// Membership eligibility and snapshot bytes must share one commit selector.
// Adding health/native registration changes the stride, so old 245 is untouched.
pub(crate) const RESOLUTION_KIND: u8 = 248;
// Snapshot-only v1 has deployed bytes. This reservation prevents incompatible
// reuse; no new provider opens, migrates or cleans its physical identity.
pub(crate) const RETIRED_RESOLUTION_KIND: u8 = 245;
/// Every peer agrees on these physical row offsets; runtime growth is unsafe.
pub(crate) const CAPACITY: usize = orbitive::core::compile::usize_from_env(
    option_env!("ORBIT_UPSTREAM_RESOLUTION_CAPACITY"),
    16,
);
/// Each fixed bank bounds one serialized membership; peers must agree.
pub(crate) const PAYLOAD: usize = orbitive::core::compile::usize_from_env(
    option_env!("ORBIT_UPSTREAM_RESOLUTION_PAYLOAD_CAPACITY"),
    8192,
);
/// Canonical slots per named binding, including retiring assignments. Fixed
/// inline geometry must match before mapping; config may request a smaller range.
pub(crate) const ADDRESSES: usize = orbitive::core::compile::usize_from_env(
    option_env!("ORBIT_UPSTREAM_RESOLUTION_ADDRESS_CAPACITY"),
    16,
);
/// Concurrent native epochs per canonical slot, across all processes. Full
/// registration refuses new IO; changing this inline stride needs matching builds.
pub(crate) const OWNERS: usize = orbitive::core::compile::usize_from_env(
    option_env!("ORBIT_UPSTREAM_RESOLUTION_OWNER_CAPACITY"),
    64,
);
// Escaped full identities retain the established bounded key representation.
pub(super) const KEY_MAX: usize = 96;
// Lower 32 bits encode closed/count; upper 32 bits fence native reuse in the
// SAME CAS. A post-CAS generation check cannot safely refund a foreign owner's
// counter after death cleanup resets it. This partition is the gate protocol,
// not an operational limit; generation exhaustion retires that record forever.
pub(super) const GENERATION_SHIFT: u32 = 32;
// Bit 31 closes admission while bits 0..30 count scopes; carry into the native
// generation is forbidden. No callback may overflow this atomic scope field.
pub(super) const CLOSED: u64 = 1 << (GENERATION_SHIFT - 1);
// Isolates closed/count from the generation tag for last-scope join completion.
pub(super) const GATE_MASK: u64 = (1 << GENERATION_SHIFT) - 1;
// Distinguishes this unified geometry from snapshot-only and literal state.
const MAGIC: u32 = 0x4355_4432;
// First unified ABI; incompatible changes require another physical kind.
const VERSION: u16 = 1;

#[repr(C, align(64))]
pub(super) struct Header {
    magic: u32,
    version: u16,
    header_size: u16,
    capacity: u32,
    slot_size: u32,
    payload: u32,
    addresses: u32,
    owners: u32,
    cell_size: u32,
    native_size: u32,
    reserved: [u8; 28],
}
impl Header {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            header_size: size_of::<Self>() as u16,
            capacity: capacity as u32,
            slot_size: size_of::<Row>() as u32,
            payload: PAYLOAD as u32,
            addresses: ADDRESSES as u32,
            owners: OWNERS as u32,
            cell_size: size_of::<Cell>() as u32,
            native_size: size_of::<Native>() as u32,
            reserved: [0; 28],
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
            || self.slot_size as usize != size_of::<Row>()
            || self.payload as usize != PAYLOAD
            || self.addresses as usize != ADDRESSES
            || self.owners as usize != OWNERS
            || self.cell_size as usize != size_of::<Cell>()
            || self.native_size as usize != size_of::<Native>()
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "incompatible upstream resolution layout: magic={:#x} version={} header={} capacity={} slot={} payload={} addresses={} owners={} cell={} native={}; expected magic={MAGIC:#x} version={VERSION} header=64 capacity={capacity} slot={} payload={PAYLOAD} addresses={ADDRESSES} owners={OWNERS} cell={} native={}",
                    self.magic,
                    self.version,
                    self.header_size,
                    self.capacity,
                    self.slot_size,
                    self.payload,
                    self.addresses,
                    self.owners,
                    self.cell_size,
                    self.native_size,
                    size_of::<Row>(),
                    size_of::<Cell>(),
                    size_of::<Native>()
                ),
            ));
        }
        Ok(())
    }
}
#[repr(C)]
pub(super) struct Bank {
    pub(super) len: AtomicU32,
    pub(super) bytes: [AtomicU8; PAYLOAD],
    // Zero denies new scopes/registration. An active assignment's full stamp
    // is committed with the JSON by the same revision/bank selector.
    pub(super) eligible: [AtomicU64; ADDRESSES],
}
#[repr(C, align(64))]
pub(super) struct Native {
    pub(super) generation: AtomicU64,
    pub(super) pid: AtomicU32,
    pub(super) started_at: AtomicU64,
    pub(super) scopes: AtomicU64,
    pub(super) pressure: AtomicU32,
    pub(super) phase: AtomicU64,
}
#[repr(C, align(64))]
pub(super) struct Cell {
    pub(super) state: crate::state::layout::OriginSlot,
    pub(super) stamp: AtomicU64,
    pub(super) natives: [Native; OWNERS],
}
#[repr(C, align(64))]
pub(super) struct Row {
    pub(super) hash: AtomicU64,
    pub(super) key: [AtomicU8; KEY_MAX],
    pub(super) fence: AtomicU64,
    // Revision occupies the high bits, bank the low bit. Comparing the full
    // selector avoids ABA when two publications reuse the same bank.
    pub(super) active: AtomicU64,
    pub(super) banks: [Bank; 2],
    pub(super) cells: [Cell; ADDRESSES],
}
impl Row {
    pub(super) fn empty() -> Self {
        Self {
            hash: AtomicU64::new(0),
            key: std::array::from_fn(|_| AtomicU8::new(0)),
            fence: AtomicU64::new(0),
            active: AtomicU64::new(0),
            banks: std::array::from_fn(|_| Bank {
                len: AtomicU32::new(0),
                bytes: std::array::from_fn(|_| AtomicU8::new(0)),
                eligible: std::array::from_fn(|_| AtomicU64::new(0)),
            }),
            cells: std::array::from_fn(|_| Cell {
                state: crate::state::layout::OriginSlot::empty(),
                stamp: AtomicU64::new(0),
                natives: std::array::from_fn(|_| Native {
                    generation: AtomicU64::new(0),
                    pid: AtomicU32::new(0),
                    started_at: AtomicU64::new(0),
                    scopes: AtomicU64::new(CLOSED),
                    pressure: AtomicU32::new(0),
                    phase: AtomicU64::new(0),
                }),
            }),
        }
    }
}
const _: () = assert!(
    RESOLUTION_KIND != RETIRED_RESOLUTION_KIND
        && RESOLUTION_KIND != crate::state::layout::UPSTREAM_STATE_KIND
        && size_of::<Header>() == 64
        && size_of::<Native>() == 64
        && CAPACITY > 0
        && CAPACITY <= u32::MAX as usize
        && PAYLOAD > 0
        && PAYLOAD <= u32::MAX as usize
        && ADDRESSES > 0
        && ADDRESSES <= u16::MAX as usize + 1
        && OWNERS > 0
        && OWNERS <= u32::MAX as usize
        && size_of::<Row>() <= u32::MAX as usize
);
