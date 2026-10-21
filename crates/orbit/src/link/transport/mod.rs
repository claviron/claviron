//! An HTTP exchange between an edge worker and an application process on the
//! same host, carried over shared memory.
//!
//! No socket, no HTTP parsing on the wire, no connection to pool. The edge
//! writes a header frame into the target's inbox lane and the body into a
//! duplex stream slot; the application answers the same way. Both ends read
//! the geometry from here so they cannot disagree about it — Orbit refuses to
//! open a segment whose shape differs from the code's, and two constants in
//! two crates eventually drift.
//!
//! ## What is addressed, and what crosses
//!
//! The addressing is an application name in a lane, and it carries frames
//! whose kind says what they are. HTTP exchanges, application commands,
//! application snapshot rows, and edge shutdown notices share the inbox.
//! The lane and name are transport addressing; the hosted application's
//! runtime-qualified identity is carried separately on management commands.
//!
//! `TRANSPORT_PLAN.md` beside this crate records the shape, what is
//! deliberately absent from it, and why.

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
// The stream types a participant needs to hold a slot open between exchanges,
// so neither end has to depend on `orbitive` to name what it is keeping.
pub use orbitive::stream::{Endpoint, StreamId, Ticket};
pub use peer::{
    ApplicationConnected, ApplicationDisconnected, EdgeConnected,
    EdgeDisconnected, LinkJoined, Peer, Peers,
};
pub use segment::{INBOX_GEOMETRY, TransportSegment};
pub use service::{LinkedApplication, TransportService};

/// Lanes one transport fleet holds.
///
/// Applications meet their callers in the explicitly selected `fleet_name`:
/// segments are `/orbit-<fleet_name>-195-<uid>` and
/// `/orbit-<fleet_name>-211-<uid>`. Application names select lanes within
/// that namespace. Capacity, isolation and segment lifetime are per fleet;
/// callers reuse one client for all applications in the same fleet.
///
/// This is that fleet's geometry, so it is the ceiling rather than the current
/// count: every edge worker plus every application worker in this namespace that may
/// run at once. Both ends read it from here, and a process that disagreed would
/// be refused at open rather than read a table of a different shape. Masters
/// take no lane — they serve no traffic. Eight edge workers and an application
/// of eight workers is sixteen; 32 leaves room. It is address space, reserved
/// rather than occupied: a lane nobody claims costs no memory.
///
/// Only the fleet namespace is part of a POSIX SHM name. An application
/// identity need only fit the lane's advertised-name field. Other kinds in
/// the runner's process-family fleet retain their own independent geometry.
pub const TRANSPORT_FLEET_CAPACITY: usize =
    orbitive::core::compile::usize_from_env(
        option_env!("ORBIT_TRANSPORT_FLEET_CAPACITY"),
        32,
    );

/// Inbox ring: one lane per fleet node, fixed-size frames, many writers and
/// the lane's owner reading, followed by one directory-watcher slot per lane
/// and one interest entry per lane and per watcher. Kind `195`; the layout
/// without interest entries was kind `196`, the one without watcher slots
/// kind `202`. A segment that grows takes a new kind rather than a bigger
/// shape under the old one.
pub const TRANSPORT_INBOX_KIND: u8 = 195;

/// Frames one node's lane holds before a writer is refused. That refusal is a
/// 503: a full lane is never overwritten, because an overwritten request is a
/// request that vanished.
///
/// It must hold what one caller may have outstanding to one target, which is
/// the same question `TRANSPORT_LANE_CAPACITY` answers from the other end —
/// so it is the same number.
///
/// A shallower lane was tried on 2026-09-24 and reverted the same day. The
/// argument for it was that depth is what a caller chooses a target by, and a
/// long queue makes that reading stale. True, and beside the point: choosing
/// needs somewhere else to go. With one process serving a name there is no
/// second target, so a shallow lane does not redirect work, it refuses it —
/// sixty-four clients against one process filled thirty-two slots and the
/// rest were answered `503` by an edge with a healthy application in front of
/// it.
///
/// Depth stays honest for a different reason: the far side must not hold a
/// backlog the caller cannot see. That is a property of what the application
/// buffers internally, not of how deep this is.
pub const TRANSPORT_INBOX_CAPACITY: usize =
    orbitive::core::compile::usize_from_env(
        option_env!("ORBIT_TRANSPORT_INBOX_CAPACITY"),
        128,
    );

/// Bytes one frame holds: an exchange's head — the HTTP request's own header
/// block, and a few dozen bytes of ours around it (correlation, the lane to
/// answer on, the stream ticket). Never a body: bodies take the stream.
///
/// **A frame is read in one piece, so this is the header limit.** It is not
/// ours to choose: the host that puts this transport behind its listeners
/// already decided how large a header block it accepts, and a frame smaller
/// than that refuses, with a 503, requests the host has let in. The edge takes
/// 16 KiB of HTTP/2 header list by default (`max_header_list_size`), so this is
/// 16 KiB and 1 KiB for our own fields. A host that raises its limit raises
/// this with it, in every binary that maps the table.
pub const TRANSPORT_INBOX_PAYLOAD_CAPACITY: usize =
    orbitive::core::compile::usize_from_env(
        option_env!("ORBIT_TRANSPORT_INBOX_PAYLOAD_CAPACITY"),
        17_408,
    );

/// Body streams: per-node lanes of duplex slots, one slot per exchange —
/// request body one way, response body the other. Kind `211`.
pub const TRANSPORT_STREAM_KIND: u8 = 211;

/// Concurrent exchanges a node may *originate*.
///
/// A slot comes from the creator's lane, and the edge creates; an application
/// worker opens the other side and consumes none of its own.
///
/// 128, which is what a Unix socket profile in `upstream.yaml` gives itself
/// as `max_live`. The number is the same question on both transports — how
/// many exchanges may be in flight from one worker at once — so answering it
/// differently would put a thumb on the comparison.
pub const TRANSPORT_LANE_CAPACITY: usize =
    orbitive::core::compile::usize_from_env(
        option_env!("ORBIT_TRANSPORT_LANE_CAPACITY"),
        128,
    );

/// Bytes each direction holds before its writer waits.
///
/// A window, not a body limit: a larger body only makes the writer wait more
/// often, and nothing here caps what an application may send.
///
/// 64 KiB, sized for what actually crosses. A request's headers are under a
/// kilobyte and its body is usually absent; a reply is a JSON document of a
/// few kilobytes or an HTML page whose median is tens. All of those cross in
/// one window with the writer never waiting, and the address space the table
/// reserves stays in the hundreds of megabytes rather than the gigabytes.
///
/// The window does bound bulk throughput at `buffer / wake latency`, and an
/// earlier value of 256 KiB was chosen from a ~55 µs wake measured on the
/// `xd01` guest. That measurement was withdrawn on 2026-09-24 along with the
/// rest of that run — see the `edge-server` project's results — so it is not
/// a reason for
/// anything until it is taken again on a host. A deployment that moves large
/// bodies raises this, in every binary that maps the table.
pub const TRANSPORT_BUFFER_BYTES: usize =
    orbitive::core::compile::usize_from_env(
        option_env!("ORBIT_TRANSPORT_BUFFER_BYTES"),
        65_536,
    );

/// The body stream table, separate from `orbit-stream`'s default kind `246`.
///
/// Worker-to-worker coordination wants many narrow lanes; this wants fewer and
/// wider. Independent specs are independent tables: separate segments, separate
/// epochs, and a reset on one leaves the other alone.
pub const TRANSPORT_STREAM_SPEC: StreamSpec = StreamSpec::new(
    TRANSPORT_STREAM_KIND,
    TRANSPORT_LANE_CAPACITY,
    TRANSPORT_BUFFER_BYTES,
);

const _: () = assert!(TRANSPORT_FLEET_CAPACITY <= u16::MAX as usize);

/// The two generic physical resources Claviron assigns to its application
/// transport. HTTP framing and application routing remain in this crate.
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

/// Whether this host can park a reader on a lane.
///
/// A lane's reader waits on a word and is woken by whoever writes. That needs
/// a shared-memory wait primitive — futex, `_umtx_op`, or
/// `os_sync_wait_on_address` from macOS 14.4. Where there is none, this
/// transport has no way to sleep that is not a poll, so a process refuses at
/// boot rather than burning a core pretending to wait.
pub fn parking_supported() -> bool {
    orbitive::link::parking_supported()
}

/// What can go wrong before an exchange is anyone's to answer.
///
/// Every variant here is something the caller turns into a status: the first
/// three are a 503 — capacity, not time — and a reply that no longer has a
/// caller is dropped rather than raised.
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
