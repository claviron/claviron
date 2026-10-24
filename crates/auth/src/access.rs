//! Host-owned service admission, independent of user and role management.
//!
//! Transports provide verified peer facts and the actual operation they route.
//! The host decides identity binding, current account state and access policy.
//! No database, user schema, role vocabulary or certificate issuance is implied.
use async_trait::async_trait;
pub use claviron_types::PeerIdentity;

use crate::AnyAuthenticated;

/// Adapter-owned service and operation names, never a client-supplied scope.
/// For example, `grpc` with `/example.Admin/Reload`, or `vpn` with `connect`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccessTarget {
    pub service: String,
    pub operation: String,
}

/// Safe admission failures; infrastructure diagnostics remain host-owned.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AccessError {
    #[error("peer identity is not accepted")]
    Unauthenticated,
    #[error("access is forbidden")]
    Forbidden,
    #[error("access policy is unavailable")]
    Unavailable,
    #[error("access target is not supported")]
    Unsupported,
}

/// Register this capability only when the service needs additional admission.
/// A registered policy is always consulted; failure never falls back to mTLS.
///
/// `Ok(None)` grants service access to the verified peer without binding a user.
/// `Ok(Some(actor))` also supplies the host's authenticated application subject.
/// Operations requiring a subject must reject a peer-only grant. A grant applies
/// only to the supplied target; adapters must not cache it across operations.
/// Managers still check current resource authority and scope on every call.
#[async_trait]
pub trait AccessPolicy: Send + Sync + 'static {
    async fn authorize(
        &self,
        peer: &PeerIdentity,
        target: &AccessTarget,
    ) -> Result<Option<AnyAuthenticated>, AccessError>;
}

#[cfg(test)]
#[path = "../tests/access.rs"]
mod tests;
