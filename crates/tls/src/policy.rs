//! What this server will negotiate, rather than what its crypto library
//! happens to offer.
//!
//! Until now the answer was `with_safe_default_protocol_versions()` and an
//! unconditional `aws_lc_rs::default_provider()`: sensible defaults, and no way
//! to say otherwise. That is a gap for anyone who has to *demonstrate* what
//! their server negotiates rather than argue that the library's defaults are
//! reasonable — a compliance regime does not accept "rustls picked it".
//!
//! The policy is process-wide because `tls.yaml` is one file and describes one
//! server. It is also deliberately read through a single provider function
//! rather than passed as a parameter: there are five places in this workspace
//! that build a rustls config — the TCP server, the mTLS server, the mTLS
//! client verifier, the H3 server and the H3 alias client — and a parameter is
//! something the sixth one can forget. A function they all already call is not.
//!
//! Unknown names fail the boot. An operator who misspells a cipher suite must
//! not quietly get the defaults back: that is the exact outcome the setting
//! exists to prevent.

use std::sync::OnceLock;

use rustls::crypto::{CryptoProvider, aws_lc_rs};
use rustls::{SupportedProtocolVersion, version};
use serde::Deserialize;

use crate::error::Error;

/// A TLS protocol version, spelled the way an operator writes it.
#[derive(
    Clone, Copy, Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd,
)]
pub enum TlsVersion {
    #[serde(rename = "1.2")]
    Tls12,
    #[default]
    #[serde(rename = "1.3")]
    Tls13,
}

impl TlsVersion {
    fn supported(self) -> &'static SupportedProtocolVersion {
        match self {
            Self::Tls12 => &version::TLS12,
            Self::Tls13 => &version::TLS13,
        }
    }
}

/// Negotiation policy from `tls.yaml`.
///
/// The defaults reproduce `with_safe_default_protocol_versions()` and the
/// provider's own suite list, so a file that says nothing behaves exactly as
/// before.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TlsPolicy {
    /// Oldest version this server will accept.
    #[serde(default = "default_min_version")]
    pub min_version: TlsVersion,

    /// Newest version this server will offer. Lower than `min_version` is a
    /// configuration error rather than an empty version set at handshake time.
    #[serde(default)]
    pub max_version: TlsVersion,

    /// Cipher suites by IANA name, e.g. `TLS13_AES_256_GCM_SHA384`. Empty
    /// keeps the provider's own list.
    #[serde(default)]
    pub cipher_suites: Vec<String>,

    /// Key exchange groups by name, e.g. `X25519`. Empty keeps the provider's
    /// own list.
    #[serde(default)]
    pub key_exchange_groups: Vec<String>,
}

impl Default for TlsPolicy {
    fn default() -> Self {
        Self {
            min_version: default_min_version(),
            max_version: TlsVersion::default(),
            cipher_suites: Vec::new(),
            key_exchange_groups: Vec::new(),
        }
    }
}

fn default_min_version() -> TlsVersion {
    TlsVersion::Tls12
}

static POLICY: OnceLock<TlsPolicy> = OnceLock::new();

/// Publishes the policy. First call wins, so a second TLS provider in the same
/// process cannot quietly renegotiate what the first one promised.
pub fn configure(policy: TlsPolicy) -> Result<(), Error> {
    policy.validate()?;
    let _ = POLICY.set(policy);

    Ok(())
}

fn current() -> &'static TlsPolicy {
    static FALLBACK: OnceLock<TlsPolicy> = OnceLock::new();

    POLICY.get().unwrap_or_else(|| FALLBACK.get_or_init(TlsPolicy::default))
}

/// The protocol versions every rustls config in this process is built with.
pub fn versions() -> Vec<&'static SupportedProtocolVersion> {
    let policy = current();

    [TlsVersion::Tls12, TlsVersion::Tls13]
        .into_iter()
        .filter(|version| {
            *version >= policy.min_version && *version <= policy.max_version
        })
        .map(TlsVersion::supported)
        .collect()
}

/// The crypto provider every rustls config in this process is built with.
///
/// Call this rather than `aws_lc_rs::default_provider()`, which is the same
/// thing with the policy left out.
pub fn provider() -> CryptoProvider {
    let policy = current();
    let mut provider = aws_lc_rs::default_provider();

    if !policy.cipher_suites.is_empty() {
        provider
            .cipher_suites
            .retain(|suite| named(&policy.cipher_suites, &suite.suite()));
    }
    if !policy.key_exchange_groups.is_empty() {
        provider
            .kx_groups
            .retain(|group| named(&policy.key_exchange_groups, &group.name()));
    }

    provider
}

impl TlsPolicy {
    /// Rejects a policy that would leave nothing to negotiate, at boot rather
    /// than at the first handshake.
    fn validate(&self) -> Result<(), Error> {
        if self.min_version > self.max_version {
            return Err(Error::Other(
                "tls.policy.min_version is newer than max_version".to_owned(),
            ));
        }

        let available = aws_lc_rs::default_provider();
        unknown(
            &self.cipher_suites,
            available.cipher_suites.iter().map(|s| s.suite()),
        )
        .map_or(Ok(()), |name| {
            Err(Error::Other(format!(
                "tls.policy: unknown cipher suite '{name}'"
            )))
        })?;
        unknown(
            &self.key_exchange_groups,
            available.kx_groups.iter().map(|g| g.name()),
        )
        .map_or(Ok(()), |name| {
            Err(Error::Other(format!(
                "tls.policy: unknown key exchange group '{name}'"
            )))
        })?;

        Ok(())
    }
}

/// rustls spells these as enum variants, and their `Debug` is the IANA name —
/// `TLS13_AES_256_GCM_SHA384`, `X25519`. Matching on that keeps the config
/// readable without a table of our own to keep in step with the library.
fn named<T: std::fmt::Debug>(
    configured: &[String],
    candidate: &T,
) -> bool {
    let name = format!("{candidate:?}");

    configured.iter().any(|wanted| wanted.eq_ignore_ascii_case(&name))
}

fn unknown<T: std::fmt::Debug>(
    configured: &[String],
    available: impl Iterator<Item = T>,
) -> Option<String> {
    let available: Vec<String> =
        available.map(|item| format!("{item:?}")).collect();

    configured
        .iter()
        .find(|wanted| {
            !available.iter().any(|name| name.eq_ignore_ascii_case(wanted))
        })
        .cloned()
}

#[cfg(test)]
#[path = "../tests/policy.rs"]
mod tests;
