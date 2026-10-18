//! Outbound identity: a name in the shared resolver, read at each handshake.
//!
//! A client that authenticates itself does not own certificate material; it
//! owns a *name*. The resolver that already holds every certificate this
//! deployment loaded answers that name, so a renewal reaches the next
//! handshake without the consumer rebuilding its TLS configuration, and an
//! operator moving a file changes nothing here. It is the client-side twin of
//! [`crate::auth::derive_resolver_for_hosts`]: both take a slice of the one
//! resolver rather than a second copy of the certificates.

use std::sync::Arc;

use rustls::SignatureScheme;
use rustls::client::ResolvesClientCert;
use rustls::sign::CertifiedKey;

use crate::resolver::CertResolver;

#[derive(Debug)]
pub struct ClientIdentity {
    resolver: Arc<CertResolver>,
    name: String,
}

impl ClientIdentity {
    pub fn new(
        resolver: Arc<CertResolver>,
        name: impl Into<String>,
    ) -> Self {
        Self { resolver, name: name.into() }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// What this identity presents right now, or `None` once the resolver no
    /// longer holds that name.
    pub fn current(&self) -> Option<Arc<CertifiedKey>> {
        self.resolver.get_cert(&self.name)
    }
}

impl ResolvesClientCert for ClientIdentity {
    /// The server's acceptable-CA hints are deliberately ignored. The operator
    /// named one identity for this leg; offering a different one would be
    /// presenting a different principal, which is a configuration decision
    /// rather than a handshake-time negotiation.
    fn resolve(
        &self,
        _root_hint_subjects: &[&[u8]],
        _sigschemes: &[SignatureScheme],
    ) -> Option<Arc<CertifiedKey>> {
        self.current()
    }

    fn has_certs(&self) -> bool {
        true
    }
}
