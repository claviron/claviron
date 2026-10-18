//! The buffer: kind 253, one lane per fleet node.
//!
//! Geometry comes from the build configuration (`ORBIT_LOG_RING_CAPACITY`,
//! `ORBIT_LOG_RING_PAYLOAD_CAPACITY`), the way every Orbit kind's does: a
//! segment's shape is fixed when it is created, and a process whose shape
//! differs from the segment's cannot open it, so it is a build decision, not
//! a runtime one.

use orbitive::{OrbitTyped, RingSpec};

/// Registered in the canonical SHM Kind ID Index (`crates/orbit`).
pub(crate) const LOG_RING_KIND: u8 = 253;

/// A whole record in one frame.
pub(crate) const FRAME_KIND_RECORD: u8 = 1;

/// One part of a record longer than a frame. Its payload starts with the
/// part's index and the record's part count (one byte each); the parts of a
/// record are published as one batch, so a per-node lane holds them
/// consecutively and a reader sees all of them or none.
pub(crate) const FRAME_KIND_PART: u8 = 2;

/// The index and count before a part's bytes.
pub(crate) const PART_HEADER: usize = 2;

/// The most frames one record may span; a longer record is truncated and
/// marked. Bounded so one record cannot overwrite a lane on its own, and so
/// the count fits the part header's byte.
pub(crate) const MAX_FRAMES: usize = orbitive::core::compile::usize_from_env(
    option_env!("ORBIT_LOG_RECORD_MAX_FRAMES"),
    32,
);

const _: () = assert!(
    MAX_FRAMES >= 1 && MAX_FRAMES <= u8::MAX as usize && MAX_FRAMES <= CAPACITY
);

/// Records retained per node lane before the oldest is overwritten.
const CAPACITY: usize = orbitive::core::compile::usize_from_env(
    option_env!("ORBIT_LOG_RING_CAPACITY"),
    1024,
);

/// One frame's bytes. A record longer than a frame spans several, up to
/// [`MAX_FRAMES`].
pub(crate) const PAYLOAD_CAPACITY: usize =
    orbitive::core::compile::usize_from_env(
        option_env!("ORBIT_LOG_RING_PAYLOAD_CAPACITY"),
        2048,
    );

#[derive(Clone)]
pub(crate) struct LogFrame;

impl OrbitTyped for LogFrame {
    const KIND: u8 = LOG_RING_KIND;
    const RING_SPEC: RingSpec = RingSpec::per_node(CAPACITY, PAYLOAD_CAPACITY);
}
