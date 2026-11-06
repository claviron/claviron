//! HTTP/3 over QUIC: bind, admission, drain, H3 request and response framing
//! and WebTransport session routing.
//!
//! The listener's owner publishes a [`Quic`] snapshot. After the handshake it
//! selects the connection's [`QuicHost`] from the server name; the host returns
//! the connection's [`QuicRequests`]. Which host that is, if hosts exist at
//! all, is the snapshot's business. [`Single`] selects nothing.

pub mod config;
pub mod drain;
pub mod error;
pub mod listener;
pub mod request;
pub mod single;
pub mod wt;

pub use listener::{Quic, QuicHost, QuicRequests, Refusal, SessionScope};
pub use single::Single;

#[cfg(test)]
#[path = "../../tests/quic/client.rs"]
pub(crate) mod test_client;
