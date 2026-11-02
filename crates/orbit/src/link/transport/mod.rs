//! Shared-memory application transport: inbox frames and duplex body streams.
//! All participants in a fleet must use the same compiled geometry.

use orbitive::link::{InboxGeometry, LinkSpec};
use orbitive::stream::StreamSpec;

pub mod app;
pub mod body;
pub mod client;
pub mod frame;
pub use super::interest;
pub use super::{Access, GroupAccess};
pub mod message;
pub mod peer;
pub mod segment;
pub mod service;
mod target;

pub use app::{ApplicationLink, ApplicationLinkSetup, LinkEventRouting};
pub use body::TransportBodies;
pub use client::{EDGE_RUNTIME, Pending, ReplyWait, TransportClient};
pub use frame::{
    ApplicationStateNotice, ControlCommand, ControlTarget, Exchange,
    ExchangeReply, FrameKind, Headers, HttpVersion, ShutdownNotice, frame_kind,
};
pub use interest::{InterestGuard, LinkInterest};
pub use message::{Message, MessageReader, read_message, write_message};
pub use orbitive::link::{
    Admission, DirectoryWatcher, Inbox, Joined, LANE_NAME_MAX, Received,
    Reclaimed, Routing,
};
// Stream slot ownership types.
pub use orbitive::stream::{Endpoint, StreamId, Ticket};
pub use peer::{
    ApplicationConnected, ApplicationDisconnected, EdgeConnected,
    EdgeDisconnected, LinkJoined, Peer, Peers,
};
pub use segment::{INBOX_GEOMETRY, TransportSegment};
pub use service::{LinkedApplication, TransportService};
pub use target::TransportTarget;

/// Maximum participant lanes per transport fleet, fixed at build time.
pub const TRANSPORT_FLEET_CAPACITY: usize =
    orbitive::core::compile::usize_from_env(
        option_env!("ORBIT_TRANSPORT_FLEET_CAPACITY"),
        32,
    );

/// SHM kind for the inbox layout, including directory watchers and interests.
pub const TRANSPORT_INBOX_KIND: u8 = 195;

/// Frames per inbox lane, fixed at build time. Full lanes reject new frames.
pub const TRANSPORT_INBOX_CAPACITY: usize =
    orbitive::core::compile::usize_from_env(
        option_env!("ORBIT_TRANSPORT_INBOX_CAPACITY"),
        128,
    );

/// Maximum frame bytes, including HTTP headers and transport metadata.
/// Fixed at build time; body bytes use the duplex stream.
pub const TRANSPORT_INBOX_PAYLOAD_CAPACITY: usize =
    orbitive::core::compile::usize_from_env(
        option_env!("ORBIT_TRANSPORT_INBOX_PAYLOAD_CAPACITY"),
        17_408,
    );

/// SHM kind for duplex request/response body streams.
pub const TRANSPORT_STREAM_KIND: u8 = 211;

/// Concurrent exchanges a node may originate, fixed at build time.
/// Each exchange occupies one slot in its creator's lane.
pub const TRANSPORT_LANE_CAPACITY: usize =
    orbitive::core::compile::usize_from_env(
        option_env!("ORBIT_TRANSPORT_LANE_CAPACITY"),
        128,
    );

/// Buffered bytes per stream direction before the writer waits.
/// Fixed at build time; total body size is not limited by this window.
pub const TRANSPORT_BUFFER_BYTES: usize =
    orbitive::core::compile::usize_from_env(
        option_env!("ORBIT_TRANSPORT_BUFFER_BYTES"),
        65_536,
    );

/// Body stream table geometry, independent of other Orbit stream tables.
pub const TRANSPORT_STREAM_SPEC: StreamSpec = StreamSpec::new(
    TRANSPORT_STREAM_KIND,
    TRANSPORT_LANE_CAPACITY,
    TRANSPORT_BUFFER_BYTES,
);

const _: () = assert!(TRANSPORT_FLEET_CAPACITY <= u16::MAX as usize);

/// Combined fleet, inbox and body stream geometry for application transport.
pub const TRANSPORT_LINK_SPEC: LinkSpec =
    transport_link_spec(TRANSPORT_FLEET_CAPACITY as u16);

const fn transport_link_spec(fleet_capacity: u16) -> LinkSpec {
    LinkSpec::new(
        fleet_capacity,
        TRANSPORT_INBOX_KIND,
        InboxGeometry::new(
            TRANSPORT_INBOX_CAPACITY,
            TRANSPORT_INBOX_PAYLOAD_CAPACITY,
        ),
        TRANSPORT_STREAM_SPEC,
    )
}

/// Whether this host supports notified shared-memory reader waits.
pub fn parking_supported() -> bool {
    orbitive::link::parking_supported()
}

/// Application transport errors.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("the target's inbox lane is full")]
    InboxFull,
    #[error("this node holds no free stream slot")]
    LaneFull,
    #[error("no live consumer serves application '{0}'")]
    NoConsumer(String),
    #[error("every lane in the destination transport fleet is taken")]
    NoLane,
    #[error("every directory watcher slot in the transport fleet is taken")]
    NoWatcher,
    #[error(
        "exchange frame does not fit in {TRANSPORT_INBOX_PAYLOAD_CAPACITY} bytes"
    )]
    FrameTooLarge,
    #[error("exchange frame is malformed: {0}")]
    Malformed(&'static str),
    #[error("invalid Orbit name '{application}': {reason}")]
    InvalidName { application: String, reason: String },
    #[error(
        "'{application}' is already served by {held_by} (lane {lane}, pid {pid}); a {runtime} \
         process cannot join it under the same name"
    )]
    RuntimeConflict {
        application: String,
        runtime: String,
        held_by: String,
        lane: usize,
        pid: u32,
    },
    #[error(transparent)]
    Stream(#[from] orbitive::stream::Error),
    #[error(transparent)]
    Orbit(#[from] orbitive::core::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

pub(crate) fn map_link_error(error: orbitive::link::Error) -> Error {
    match error {
        orbitive::link::Error::InboxFull => Error::InboxFull,
        orbitive::link::Error::NoLane => Error::NoLane,
        orbitive::link::Error::NoWatcher => Error::NoWatcher,
        orbitive::link::Error::FrameTooLarge => Error::FrameTooLarge,
        orbitive::link::Error::Malformed(message) => Error::Malformed(message),
        orbitive::link::Error::InvalidName { name, reason } => {
            Error::InvalidName { application: name, reason }
        }
        orbitive::link::Error::RoleConflict {
            name,
            role,
            held_by,
            lane,
            pid,
        } => Error::RuntimeConflict {
            application: name,
            runtime: role,
            held_by,
            lane,
            pid,
        },
        orbitive::link::Error::Stream(error) => Error::Stream(error),
        orbitive::link::Error::Core(error) => Error::Orbit(error),
        orbitive::link::Error::Io(error) => {
            Error::Orbit(orbitive::core::Error::Io(error))
        }
        _ => Error::Malformed("unrecognized Orbit link error"),
    }
}

#[cfg(test)]
#[path = "../../../tests/link/transport.rs"]
mod tests;

#[cfg(test)]
#[path = "../../../tests/link/transport/exchange.rs"]
mod exchange_tests;
