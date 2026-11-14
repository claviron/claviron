//! Sessions a host offers by ALPN: a protocol a client names in its
//! ClientHello selects a handshake of its own and an application of its own,
//! before any request. See `docs/design/ALPN_ROUTING.md`.

use std::fmt;
use std::sync::{Arc, RwLock};

use claviron_http::config::{HttpPolicy, HttpSnapshot, ListenerConfig};
use claviron_pipeline::HostHandler;
use claviron_pipeline::compile::{SessionInput, compile_session};
use claviron_pipeline::extension::RouteLayer;
use claviron_pipeline::plan::HostExecution;
use claviron_runtime::AppState;
use claviron_runtime::registry::{Error, Result};
use claviron_tls::Proto;
use claviron_types::Host;
use serde::de::DeserializeOwned;

pub use claviron_pipeline::compile::Inherit;

use crate::config::HostConfig;

/// A protocol a host can offer by ALPN, speaking HTTP to an application of
/// its own.
///
/// A session authenticates its callers itself, so its handshake asks for no
/// client certificate whatever the host's own policy; in return, a connection
/// that negotiated it reaches only its application. The package that speaks
/// the protocol registers the profile against this public API alone; a host
/// enables it by id, with its parameters, in `tls.alpn`.
pub trait SessionProfile: Send + Sync {
    /// The ALPN protocol id. One profile per id in a process.
    fn protocol(&self) -> &'static [u8];

    /// The HTTP version spoken once the id is negotiated.
    fn wire(&self) -> Proto;

    /// What the profile takes from its host's admission, applied where the
    /// host declares it. Nothing unless named.
    fn inherits(&self) -> Inherit {
        Inherit::NONE
    }

    /// Build what a connection that negotiated this profile reaches on the
    /// plan's host, prepared with the host at boot or reload.
    fn execution(
        &self,
        plan: SessionPlan<'_>,
    ) -> Result<HostExecution>;
}

/// One host's session for a profile, being built: the host's admission the
/// profile inherits, its parameters, and the layers and owner the profile adds.
pub struct SessionPlan<'a> {
    state: &'a AppState,
    host: &'a Host,
    config: &'a Arc<HostConfig>,
    listener: &'a ListenerConfig,
    http: Option<&'a HttpSnapshot>,
    params: &'a serde_yaml::Value,
    inherit: Inherit,
    policy: HttpPolicy,
    layers: Vec<Arc<dyn RouteLayer>>,
}

impl<'a> SessionPlan<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        state: &'a AppState,
        host: &'a Host,
        config: &'a Arc<HostConfig>,
        listener: &'a ListenerConfig,
        http: Option<&'a HttpSnapshot>,
        params: &'a serde_yaml::Value,
        inherit: Inherit,
    ) -> Self {
        Self {
            state,
            host,
            config,
            listener,
            http,
            params,
            inherit,
            policy: listener.policy().narrow(&config.policy()),
            layers: Vec::new(),
        }
    }

    pub fn state(&self) -> &AppState {
        self.state
    }

    pub fn host(&self) -> &Host {
        self.host
    }

    pub fn config(&self) -> &Arc<HostConfig> {
        self.config
    }

    /// The host's effective HTTP policy on this listener.
    pub fn policy(&self) -> &HttpPolicy {
        &self.policy
    }

    /// This host's parameters for the profile, decoded into the profile's own
    /// type. An id listed without a value reads as an empty mapping.
    pub fn params<T: DeserializeOwned>(&self) -> Result<T> {
        let empty = serde_yaml::Value::Mapping(Default::default());
        let params = if self.params.is_null() { &empty } else { self.params };
        serde_yaml::from_value(params.clone()).map_err(|error| {
            Error::msg(format!(
                "ALPN parameters of host {}: {error}",
                self.host
            ))
        })
    }

    /// Add a layer of the profile's own, beside what it inherits.
    pub fn layer(
        mut self,
        layer: Arc<dyn RouteLayer>,
    ) -> Self {
        self.layers.push(layer);
        self
    }

    /// Finish the plan with the profile's owner, the only one the session
    /// reaches.
    pub fn backend(
        self,
        owner: impl HostHandler,
    ) -> Result<HostExecution> {
        compile_session(SessionInput {
            state: self.state,
            host: self.host,
            site: self.config.site().clone(),
            transport: &self.config.transport,
            listener: Some(self.listener),
            http: self.http,
            inherit: self.inherit,
            layers: &self.layers,
            owner: Arc::new(owner),
            policy: self.policy,
        })
        .map_err(|error| Error::msg(error.to_string()))
    }
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
    #[cfg(all(feature = "tcp", feature = "tls"))]
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
