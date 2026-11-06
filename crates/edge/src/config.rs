//! Host and route declarations, independent of provider boot and backend preparation.

use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;

use claviron_helpers::de::opt_one_or_many;
pub use claviron_http::config::host::{
    DEFAULT_SERVER_HEADER, HostHeadersConfig,
};
#[cfg(feature = "orbit")]
pub use claviron_http::config::routes::OrbitRouteConfig;
pub use claviron_http::config::routes::RouteRule;
pub use claviron_http::config::statics::{
    DYNAMIC_FILE_EXTS, StaticConfig, is_dynamic_file,
};
pub use claviron_http::config::{
    acl, caching as policy, cors, response, transport,
};
pub use claviron_http::policy::{
    HttpPolicyOverride, RequestPolicyOverride, ResponsePolicyOverride,
};
pub use claviron_tls::auth::{ClientAuthMode, ClientAuthPolicy};
pub use claviron_tls::handshake::HandshakePolicy;
use claviron_types::Aliases;
pub use policy::{
    CachingConfig, CompressionAlgorithm, CompressionConfig, RulesConfig,
};
pub use response::{LocationTemplate, RedirectConfig, RedirectStatus};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer};
use serde_yaml::{Mapping, Value};

#[cfg(feature = "rate-limit")]
pub use claviron_pipeline::site::quota;
#[cfg(feature = "rate-limit")]
pub use claviron_pipeline::site::quota::RateLimitConfig;
pub use claviron_pipeline::site::{RouteConfig, Site};

/// A virtual host: how a connection finds a [`Site`] and on what terms. Its
/// names and listeners select it; its handshake and protocol policy hold before
/// any request exists.
pub struct HostConfig {
    site: Arc<Site>,
    /// Missing = compatible non-explicit listeners. A list restricts all
    /// listener membership; an empty list exposes this host nowhere.
    pub listeners: Option<Vec<claviron_http::ListenerId>>,
    aliases: Aliases,
    /// TLS handshake policy, separate from HTTP transport/request policy.
    pub tls: HandshakePolicy,
    /// HTTP transport/request boundary policy.
    pub transport: TransportPolicy,
}

impl Deref for HostConfig {
    type Target = Site;

    fn deref(&self) -> &Self::Target {
        &self.site
    }
}

/// A host's site is mutable only while it is being declared, before it is
/// shared with anything that prepares from it.
impl std::ops::DerefMut for HostConfig {
    fn deref_mut(&mut self) -> &mut Self::Target {
        Arc::get_mut(&mut self.site)
            .expect("a host's site is not shared while it is being declared")
    }
}

#[cfg(feature = "http3")]
pub use transport::{EarlyDataPolicy, H3Policy};
pub use transport::{H1Policy, H2Policy, NonSslPolicy, TransportPolicy};

/// What a declaration says about the connection rather than what it serves.
#[derive(Deserialize)]
struct HostTerms {
    #[serde(default, deserialize_with = "opt_one_or_many")]
    listeners: Option<Vec<claviron_http::ListenerId>>,
    #[serde(default, alias = "names", alias = "name", alias = "alias")]
    aliases: Aliases,
    #[serde(default)]
    tls: HandshakePolicy,
    /// The old `https:` config key is accepted only as a deprecated
    /// deserialize alias.
    #[serde(default, alias = "https")]
    transport: TransportPolicy,
}

const TERMS: [&str; 8] = [
    "listeners",
    "aliases",
    "names",
    "name",
    "alias",
    "tls",
    "transport",
    "https",
];

/// Take the connection's terms out of a flat declaration and its explicit `/`
/// route, which name the same scope. What remains is the site.
fn take_terms(source: &mut Mapping) -> Result<Mapping, String> {
    let mut terms = Mapping::new();
    for name in TERMS {
        if let Some(value) = source.remove(name) {
            terms.insert(Value::String(name.to_owned()), value);
        }
    }
    let root = source
        .get_mut("routes")
        .and_then(Value::as_mapping_mut)
        .and_then(|routes| routes.get_mut("/"))
        .and_then(Value::as_mapping_mut);
    if let Some(root) = root {
        for name in TERMS {
            if let Some(value) = root.remove(name) {
                let name = Value::String(name.to_owned());
                if terms.contains_key(&name) {
                    return Err(format!(
                        "flat host field {name:?} conflicts with route '/'"
                    ));
                }
                terms.insert(name, value);
            }
        }
    }
    Ok(terms)
}

impl<'de> Deserialize<'de> for HostConfig {
    fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        let mut source = Mapping::deserialize(de)?;
        let terms = take_terms(&mut source).map_err(D::Error::custom)?;
        let terms: HostTerms = serde_yaml::from_value(Value::Mapping(terms))
            .map_err(D::Error::custom)?;
        let site: Site = serde_yaml::from_value(Value::Mapping(source))
            .map_err(D::Error::custom)?;
        let HostTerms { listeners, aliases, tls, transport } = terms;
        Ok(Self { site: Arc::new(site), listeners, aliases, tls, transport })
    }
}

impl HostConfig {
    /// The site alone: a route scope has no connection terms of its own.
    #[cfg(test)]
    pub(crate) fn into_site(self) -> Site {
        Arc::into_inner(self.site)
            .expect("a site is not shared while it is being declared")
    }

    /// Equality of handshake inputs, excluding request and application policy.
    pub fn same_tls_policy(
        &self,
        other: &Self,
    ) -> bool {
        self.listeners == other.listeners
            && self.transport.protocols == other.transport.protocols
            && self.tls.require_alpn == other.tls.require_alpn
            && self.tls.client_auth == other.tls.client_auth
    }

    /// An empty host declaration with an explicit file root and the same
    /// policy defaults as a deserialized host. Register it with
    /// `claviron::edge::register_host`; routes still belong to `Routable`.
    /// For a consumer-owned configuration file, deserialize `HostConfig`
    /// directly instead, including its normal routes and directives.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            site: Arc::new(Site::new(root)),
            listeners: None,
            aliases: Aliases::default(),
            tls: HandshakePolicy::default(),
            transport: TransportPolicy::default(),
        }
    }

    /// The site this host serves, shared with whatever prepares from it.
    pub fn site(&self) -> &Arc<Site> {
        &self.site
    }

    pub fn allows_listener(
        &self,
        id: &claviron_http::ListenerId,
    ) -> bool {
        self.listeners.as_ref().is_none_or(|listeners| listeners.contains(id))
    }

    /// Host narrowing only; a driver must also enforce its listener's capabilities.
    pub fn allows_transport(
        &self,
        listener: &claviron_http::ListenerId,
        protocol: claviron_http::listener::HttpProtocol,
    ) -> bool {
        self.allows_listener(listener)
            && self.transport.allows_protocol(protocol)
    }

    pub fn aliases(&self) -> &Aliases {
        &self.aliases
    }

    /// Effective per-host transport policy (falls back to defaults if not configured).
    pub fn transport_policy(&self) -> &TransportPolicy {
        &self.transport
    }

    /// Effective per-host TLS handshake policy.
    pub fn tls_policy(&self) -> HandshakePolicy {
        self.tls.clone()
    }
}

/// File/application view already selected by Edge's route table. Native Rust
/// intercepts use this instead of reselecting a route or reusing the original
/// host-wide file policy. Canonical host metadata remains a separate handle.
#[derive(Clone)]
pub struct SelectedRoute(pub Arc<HostConfig>);

#[cfg(test)]
#[path = "../tests/config/routing.rs"]
mod routing_tests;
