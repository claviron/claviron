//! Sessions a host offers by ALPN: a protocol a client names in its
//! ClientHello selects a handshake of its own and a prepared execution of its
//! own, before any request. See `docs/design/ALPN_ROUTING.md`.

use std::fmt;
use std::sync::{Arc, RwLock};

use claviron_http::config::HttpPolicy;
use claviron_pipeline::plan::HostExecution;
use claviron_runtime::AppState;
use claviron_runtime::registry::Result;
use claviron_tls::Proto;
use claviron_types::Host;

use crate::config::HostConfig;

/// A protocol a host can offer by ALPN, speaking HTTP to services of its own.
///
/// A session authenticates its callers itself, so its handshake asks for no
/// client certificate whatever the host's own policy; in return, a connection
/// that negotiated it reaches only its execution. The package that speaks
/// the protocol registers the profile; a host enables it by id in its
/// `tls.alpn` list.
pub trait SessionProfile: Send + Sync {
    /// The ALPN protocol id. One profile per id in a process.
    fn protocol(&self) -> &'static [u8];

    /// The HTTP version spoken once the id is negotiated.
    fn wire(&self) -> Proto;

    /// What a connection that negotiated this profile reaches on `host`,
    /// prepared with the host at boot or reload. `policy` is the host's
    /// effective HTTP policy on this listener; a profile that serves a Rust
    /// handler gives it to `HostExecution::router`.
    fn execution(
        &self,
        state: &AppState,
        host: &Host,
        config: &Arc<HostConfig>,
        policy: &HttpPolicy,
    ) -> Result<HostExecution>;
}

/// A second profile asked for a protocol id another one answers.
#[derive(Debug)]
pub struct SessionConflict {
    pub protocol: &'static [u8],
}

impl fmt::Display for SessionConflict {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        write!(
            f,
            "ALPN protocol '{}' already has a session profile",
            String::from_utf8_lossy(self.protocol)
        )
    }
}

impl std::error::Error for SessionConflict {}

/// The session profiles a process registers, at most one per protocol id.
#[derive(Default)]
pub struct SessionProfiles {
    profiles: RwLock<Vec<Arc<dyn SessionProfile>>>,
}

impl SessionProfiles {
    pub fn register(
        &self,
        profile: Arc<dyn SessionProfile>,
    ) -> std::result::Result<(), SessionConflict> {
        let mut profiles =
            self.profiles.write().unwrap_or_else(|error| error.into_inner());
        let protocol = profile.protocol();
        if profiles.iter().any(|existing| existing.protocol() == protocol) {
            return Err(SessionConflict { protocol });
        }
        profiles.push(profile);
        Ok(())
    }

    /// The profile registered for `protocol`, read when a host is prepared.
    pub(crate) fn get(
        &self,
        protocol: &[u8],
    ) -> Option<Arc<dyn SessionProfile>> {
        self.profiles
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .find(|profile| profile.protocol() == protocol)
            .cloned()
    }
}
