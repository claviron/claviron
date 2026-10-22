//! Protocol-neutral Orbit Link connections owned by their calling service.
//!
//! A Rust application can join a Link and exchange frames or topic/payload
//! events without an HTTP route, an application runner or `ApplicationEvent`.
//! Orbitive owns the inbox, admission and death recovery. This module owns
//! the connection lifetime and its single reader. [`transport`] adds the
//! existing Claviron application/HTTP protocol over that substrate.
//! [`invoke`] carries HTTP-independent commands with reserved result channels;
//! callers enter through [`crate::invoke::Client`], not transport addressing.
//!
//! Opening is explicit: do it in the owning service's execution/preparation,
//! not during registration. All geometry and identities come from the caller.
//! Dropping a connection releases its lane, never resets or unlinks the Link.
//! Connections default to the caller's UID namespace. Explicit [`Access::Group`]
//! shares only a dedicated Link and its streams with trusted OS-group members.

mod access;
mod connection;
mod events;
pub mod interest;
pub mod invoke;
pub mod transport;

pub use access::Access;
pub use connection::Connection;
pub use events::{EventConnection, Incoming};
pub use interest::{InterestGuard, LinkInterest};
pub use orbitive::events::{DefaultEventLayout, EventLayout};
pub use orbitive::link::{
    Admission, Error, GroupAccess, InboxGeometry, LinkSegment, LinkSpec,
    Received, Reclaimed, Result, Routing,
};
pub use orbitive::stream::StreamSpec;
