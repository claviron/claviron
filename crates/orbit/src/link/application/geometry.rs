//! The application Link's compiled geometry: inbox frames and duplex body
//! streams. All participants in a fleet must use the same compiled geometry.

use orbitive::link::{InboxGeometry, LinkSpec};
use orbitive::stream::StreamSpec;

/// Maximum participant lanes per link fleet, fixed at build time.
pub const LINK_FLEET_CAPACITY: usize = orbitive::core::compile::usize_from_env(
    option_env!("ORBIT_LINK_FLEET_CAPACITY"),
    32,
);

/// SHM kind for the inbox layout, including directory watchers and interests.
pub const LINK_INBOX_KIND: u8 = 195;

/// Frames per inbox lane, fixed at build time. Full lanes reject new frames.
pub const LINK_INBOX_CAPACITY: usize = orbitive::core::compile::usize_from_env(
    option_env!("ORBIT_LINK_INBOX_CAPACITY"),
    128,
);

/// Maximum frame bytes, including HTTP headers and link metadata.
/// Fixed at build time; body bytes use the duplex stream.
pub const LINK_INBOX_PAYLOAD_CAPACITY: usize =
    orbitive::core::compile::usize_from_env(
        option_env!("ORBIT_LINK_INBOX_PAYLOAD_CAPACITY"),
        17_408,
    );

/// SHM kind for duplex request/response body streams.
pub const LINK_STREAM_KIND: u8 = 211;

/// Concurrent exchanges a node may originate, fixed at build time.
/// Each exchange occupies one slot in its creator's lane.
pub const LINK_LANE_CAPACITY: usize = orbitive::core::compile::usize_from_env(
    option_env!("ORBIT_LINK_LANE_CAPACITY"),
    128,
);

/// Buffered bytes per stream direction before the writer waits.
/// Fixed at build time; total body size is not limited by this window.
pub const LINK_BUFFER_BYTES: usize = orbitive::core::compile::usize_from_env(
    option_env!("ORBIT_LINK_BUFFER_BYTES"),
    65_536,
);

/// Body stream table geometry, independent of other Orbit stream tables.
pub const LINK_STREAM_SPEC: StreamSpec =
    StreamSpec::new(LINK_STREAM_KIND, LINK_LANE_CAPACITY, LINK_BUFFER_BYTES);

const _: () = assert!(LINK_FLEET_CAPACITY <= u16::MAX as usize);

// This geometry is compiled in, and a deployment that still sets a former
// name would silently build with the defaults and refuse the fleet's
// segments at run time. Stop the build instead.
const _: () = {
    assert!(
        option_env!("ORBIT_TRANSPORT_FLEET_CAPACITY").is_none(),
        "ORBIT_TRANSPORT_FLEET_CAPACITY is now ORBIT_LINK_FLEET_CAPACITY"
    );
    assert!(
        option_env!("ORBIT_TRANSPORT_INBOX_CAPACITY").is_none(),
        "ORBIT_TRANSPORT_INBOX_CAPACITY is now ORBIT_LINK_INBOX_CAPACITY"
    );
    assert!(
        option_env!("ORBIT_TRANSPORT_INBOX_PAYLOAD_CAPACITY").is_none(),
        "ORBIT_TRANSPORT_INBOX_PAYLOAD_CAPACITY is now ORBIT_LINK_INBOX_PAYLOAD_CAPACITY"
    );
    assert!(
        option_env!("ORBIT_TRANSPORT_LANE_CAPACITY").is_none(),
        "ORBIT_TRANSPORT_LANE_CAPACITY is now ORBIT_LINK_LANE_CAPACITY"
    );
    assert!(
        option_env!("ORBIT_TRANSPORT_BUFFER_BYTES").is_none(),
        "ORBIT_TRANSPORT_BUFFER_BYTES is now ORBIT_LINK_BUFFER_BYTES"
    );
};

/// Combined fleet, inbox and body stream geometry for the application link.
pub const LINK_SPEC: LinkSpec = link_spec(LINK_FLEET_CAPACITY as u16);

pub(super) const fn link_spec(fleet_capacity: u16) -> LinkSpec {
    LinkSpec::new(
        fleet_capacity,
        LINK_INBOX_KIND,
        InboxGeometry::new(LINK_INBOX_CAPACITY, LINK_INBOX_PAYLOAD_CAPACITY),
        LINK_STREAM_SPEC,
    )
}

/// Whether this host supports notified shared-memory reader waits.
pub fn parking_supported() -> bool {
    orbitive::link::parking_supported()
}

#[cfg(test)]
#[path = "../../../tests/link/application/geometry.rs"]
mod tests;
