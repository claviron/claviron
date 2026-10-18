//! Host transport declarations, independent of listener/socket ownership.

use claviron_helpers::de::one_or_many;
use serde::{Deserialize, Deserializer};

use crate::listener::HttpProtocol;

/// Action on a non-SSL listener, independent of its HTTP version.
/// Serving is always explicit; omission retains HTTPS redirection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NonSslPolicy {
    Serve,
    #[default]
    Redirect,
    Reject,
}

/// Host policy narrows listener capabilities; it never enables a listener or
/// a protocol that the selected listener cannot serve. Protocol option blocks
/// do not implicitly enable their protocol.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
pub struct TransportPolicy {
    #[serde(default)]
    pub non_ssl: NonSslPolicy,
    /// Omitted = inherit listener protocols and preference; explicit [] = none.
    /// An explicit list supplies host ALPN preference, limited by the listener.
    #[serde(default, deserialize_with = "deserialize_protocols")]
    pub protocols: Option<Vec<HttpProtocol>>,
    #[serde(default)]
    pub h1: H1Policy,
    #[serde(default)]
    pub h2: H2Policy,
    #[cfg(feature = "http3")]
    #[serde(default)]
    pub h3: H3Policy,
}

impl TransportPolicy {
    pub fn allows_protocol(
        &self,
        protocol: HttpProtocol,
    ) -> bool {
        self.protocols
            .as_ref()
            .is_none_or(|protocols| protocols.contains(&protocol))
    }
}

fn deserialize_protocols<'de, D>(
    deserializer: D
) -> Result<Option<Vec<HttpProtocol>>, D::Error>
where
    D: Deserializer<'de>,
{
    let protocols: Vec<HttpProtocol> = one_or_many(deserializer)?;
    for (index, protocol) in protocols.iter().enumerate() {
        if protocols[..index].contains(protocol) {
            return Err(serde::de::Error::custom(
                "protocols contains duplicate entries",
            ));
        }
    }
    Ok(Some(protocols))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct H1Policy {
    /// Permit Upgrade requests. A registered handler must still accept and own
    /// the upgraded IO; this declaration does not create a handler.
    #[serde(default)]
    pub upgrade: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct H2Policy {
    /// Permit extended CONNECT and advertise ENABLE_CONNECT_PROTOCOL on the
    /// selected host's H2 connection. Handler registration is independent.
    #[serde(default)]
    pub connect_protocol: bool,
}

#[cfg(feature = "http3")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(deny_unknown_fields)]
#[expect(
    clippy::manual_non_exhaustive,
    reason = "the private serde field rejects a removed config key with a migration diagnostic"
)]
pub struct H3Policy {
    /// Request replay policy; only relevant when the QUIC listener accepts 0-RTT.
    #[serde(default)]
    pub early_data: EarlyDataPolicy,
    /// Removed; read only to say so. HTTP/3 replies stream like any other:
    /// a client that dies unseen holds one until the listener's
    /// `quic.idle_timeout`, the length the operator declares.
    #[serde(
        default,
        rename = "streaming",
        deserialize_with = "removed_streaming"
    )]
    removed_streaming: (),
}

#[cfg(feature = "http3")]
fn removed_streaming<'de, D: Deserializer<'de>>(_: D) -> Result<(), D::Error> {
    Err(serde::de::Error::custom(
        "transport.h3.streaming is gone: HTTP/3 replies stream; a client that dies unseen holds one until the HTTP/3 listener's quic.idle_timeout",
    ))
}

/// Per-host HTTP/3 early-data policy, separate from listener 0-RTT support.
#[cfg(feature = "http3")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EarlyDataPolicy {
    /// Historical allow-list: GET, HEAD and OPTIONS only (not every idempotent method).
    #[default]
    #[serde(alias = "idempotent")]
    AllowIdempotent,
    #[serde(alias = "deny")]
    DenyAll,
    #[serde(alias = "allow")]
    AllowAll,
}
