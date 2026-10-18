//! Owns prepared backend origin identity; connections carry only OriginId.
mod generation;
mod health;
mod target;
use std::sync::Arc;

use claviron_orbit::lock::LockKey;
pub use generation::{PoolGeneration, ResolutionGeneration};
pub(crate) use health::{OriginHealth, Serving};
use http::HeaderValue;
use http::uri::Authority;
pub(crate) use target::ConnectTarget;
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Deserialize,
    serde::Serialize,
)]
/// Dense index in one immutable service member view, never a DNS reservation.
pub struct OriginId(pub u16);
impl OriginId {
    pub(crate) fn index(self) -> usize {
        usize::from(self.0)
    }
}
/// Stable canonical slot reserved for a named DNS binding. Retirement and
/// assignment stamps use this identity even when dense serving indexes move.
/// Materialization must choose a fresh dense OriginId separately; there is no
/// implicit conversion from a reservation into a native pool/view index.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Deserialize,
    serde::Serialize,
)]
pub struct ReservedOriginId(pub u16);
impl ReservedOriginId {
    pub(crate) fn index(self) -> usize {
        usize::from(self.0)
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Origin {
    pub(crate) id: OriginId,
    pub(crate) label: Arc<str>,
    pub(crate) authority: Authority,
    pub(crate) host: HeaderValue,
    pub(crate) source: Arc<str>,
    pub(crate) state_key: Arc<str>,
    pub(crate) slot: crate::state::StateSlot,
    pub(crate) tls_name: Option<rustls::pki_types::ServerName<'static>>,
    pub(crate) connect: ConnectTarget,
    pub(crate) weight: u32,
    /// See `DetailedOriginConfig::max_live`.
    pub(crate) max_live: Option<usize>,
    pub(crate) priority: u16,
    pub(crate) lock: [LockKey; 5],
    pub(crate) metrics: crate::metrics::OriginMetrics,
}
