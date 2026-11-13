//! What a server requires of a TLS handshake beyond certificates and
//! versions: a client certificate, and a negotiated ALPN protocol. A virtual
//! host borrows this for its `tls:` block; the process-wide versions and
//! suites are [`crate::policy::TlsPolicy`].

use serde::Deserialize;
use std::{io, sync::Arc};

/// An optional, listener-scoped handshake with no application protocol driver.
/// Return `None` to continue normal host selection or an error to refuse TLS.
pub trait HandshakeSelector: Send + Sync {
    fn select(
        &self,
        listener: &str,
        hello: &rustls::server::ClientHello<'_>,
    ) -> io::Result<Option<Arc<rustls::ServerConfig>>>;
}

use crate::auth::ClientAuthPolicy;

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HandshakePolicy {
    #[serde(default)]
    pub client_auth: ClientAuthPolicy,
    /// Refuse TLS connections without negotiated ALPN. Does not affect non-SSL serving.
    #[serde(default)]
    pub require_alpn: bool,
}
