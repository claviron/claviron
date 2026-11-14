//! What a server requires of a TLS handshake beyond certificates and
//! versions: a client certificate, and a negotiated ALPN protocol. A virtual
//! host borrows this for its `tls:` block; the process-wide versions and
//! suites are [`crate::policy::TlsPolicy`].

use serde::Deserialize;
use std::{
    fmt, io,
    sync::{Arc, RwLock},
};

/// A TLS handshake that an ALPN protocol a client offers selects, ahead of
/// ordinary host selection. A profile negotiates; it grants no trust. The
/// certificate and client authentication it applies are its own handshake's,
/// and a profile that does not answer leaves the connection to its host.
pub trait AlpnProfile: Send + Sync {
    /// The ALPN protocol ids this profile answers. No two profiles in a
    /// process answer the same one.
    fn protocols(&self) -> &[&'static [u8]];

    /// The handshake for `hello`, which offered one of [`Self::protocols`].
    /// `None` continues ordinary host selection; an error refuses TLS.
    fn handshake(
        &self,
        listener: &str,
        hello: &rustls::server::ClientHello<'_>,
    ) -> io::Result<Option<Arc<rustls::ServerConfig>>>;
}

/// A second profile asked for an ALPN protocol id another one answers.
#[derive(Debug)]
pub struct AlpnConflict {
    pub protocol: &'static [u8],
}

impl fmt::Display for AlpnConflict {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        write!(
            f,
            "ALPN protocol '{}' already has a profile",
            String::from_utf8_lossy(self.protocol)
        )
    }
}

impl std::error::Error for AlpnConflict {}

/// The ALPN profiles a process registers, at most one per protocol id. A
/// host's listeners read them as an [`AlpnSelection`] when they are prepared.
#[derive(Default)]
pub struct AlpnProfiles {
    profiles: RwLock<Vec<Arc<dyn AlpnProfile>>>,
}

impl AlpnProfiles {
    /// Add `profile`, refusing it whole if any of its ids is taken.
    pub fn register(
        &self,
        profile: Arc<dyn AlpnProfile>,
    ) -> Result<(), AlpnConflict> {
        let mut profiles =
            self.profiles.write().unwrap_or_else(|error| error.into_inner());
        if let Some(protocol) = profile.protocols().iter().find(|protocol| {
            profiles
                .iter()
                .any(|existing| existing.protocols().contains(protocol))
        }) {
            return Err(AlpnConflict { protocol });
        }
        profiles.push(profile);
        Ok(())
    }

    /// What is registered now, fixed for the listeners prepared from it.
    pub fn selection(&self) -> AlpnSelection {
        let profiles =
            self.profiles.read().unwrap_or_else(|error| error.into_inner());
        AlpnSelection(profiles.iter().cloned().collect())
    }
}

/// The ALPN profiles a listener consults, fixed when it is prepared.
#[derive(Clone, Default)]
pub struct AlpnSelection(Arc<[Arc<dyn AlpnProfile>]>);

impl AlpnSelection {
    /// The handshake of the profile answering the first protocol `hello`
    /// offers that any profile answers, in the client's order. `None` when no
    /// offered protocol has a profile, or when that profile does not answer.
    pub fn select(
        &self,
        listener: &str,
        hello: &rustls::server::ClientHello<'_>,
    ) -> io::Result<Option<Arc<rustls::ServerConfig>>> {
        if self.0.is_empty() {
            return Ok(None);
        }
        let Some(offered) = hello.alpn() else { return Ok(None) };
        for protocol in offered {
            if let Some(profile) = self
                .0
                .iter()
                .find(|profile| profile.protocols().contains(&protocol))
            {
                return profile.handshake(listener, hello);
            }
        }
        Ok(None)
    }
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
    /// Session profiles this host offers, by ALPN protocol id. Each id is
    /// registered by the package that speaks it; enabling it here is setup.
    #[serde(default)]
    pub alpn: Vec<Box<str>>,
}

#[cfg(test)]
#[path = "../tests/handshake.rs"]
mod tests;
