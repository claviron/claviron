//! What a server requires of a TLS handshake beyond certificates and
//! versions: a client certificate, and a negotiated ALPN protocol. A virtual
//! host borrows this for its `tls:` block; the process-wide versions and
//! suites are [`crate::policy::TlsPolicy`].

use serde::Deserialize;

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
