//! Claviron's web application over a Link: HTTP exchanges addressed to a named
//! application, as inbox frames and duplex body streams. All participants in
//! a fleet must use the same compiled geometry.

pub mod app;
pub mod body;
pub mod client;
mod creator;
mod error;
pub mod frame;
mod geometry;
mod growth;
pub use super::interest;
pub use super::{Access, GroupAccess};
pub mod message;
pub mod peer;
mod queue;
mod receiver;
mod retirement;
pub mod segment;
mod selection;
pub mod service;
mod target;

pub use app::{ApplicationLink, ApplicationLinkSetup, LinkEventRouting};
pub use body::ApplicationBodies;
pub use claviron_types::link::LinkBalance;
pub use client::{ApplicationClient, EDGE_RUNTIME, Pending, ReplyWait};
pub(crate) use error::map_link_error;
pub use error::{Error, Result};
pub use frame::{
    ApplicationStateNotice, ControlCommand, ControlTarget, Exchange,
    ExchangeBody, ExchangeReply, FrameKind, Headers, HttpVersion,
    ShutdownNotice, fit_inline, frame_kind,
};
use geometry::link_spec;
pub use geometry::*;
pub use interest::{InterestGuard, LinkInterest};
pub use message::{Message, MessageReader, read_message, write_message};
pub use orbitive::link::{
    Admission, DirectoryWatcher, Inbox, Joined, LANE_NAME_MAX, Received,
    Reclaimed, Routing, WorkExecution, WorkReservation, WorkTicket,
};
// Stream slot ownership types.
pub use growth::FleetGrowth;
pub use orbitive::link::{GrowthPolicy, Watched};
pub use orbitive::stream::{Endpoint, StreamId, Ticket};
pub use peer::{
    ApplicationConnected, ApplicationDisconnected, EdgeConnected,
    EdgeDisconnected, LinkJoined, LinkPressure, LinkWorkerIdle, Peer, Peers,
};
pub use queue::{Offered, Start, WorkQueue};
pub use receiver::ApplicationReceiver;
pub use retirement::{IdleRetirement, RetirementSource, RetirementTarget};
pub use segment::{ApplicationSegment, INBOX_GEOMETRY};
pub use service::{ApplicationService, LinkApplication};
pub use target::ApplicationTarget;

#[cfg(test)]
#[path = "../../../tests/link/application/exchange.rs"]
mod exchange_tests;
