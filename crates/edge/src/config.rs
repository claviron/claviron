//! Host and route declarations, independent of provider boot and backend preparation.

use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;

use claviron_helpers::de::opt_one_or_many;
use claviron_http::config::cors::CorsConfig;
pub use claviron_http::config::host::{
    DEFAULT_SERVER_HEADER, HostHeadersConfig,
};
#[cfg(feature = "lua")]
pub use claviron_http::config::routes::JavaScriptWebSocketConfig;
#[cfg(feature = "lua")]
pub use claviron_http::config::routes::LuaRouteConfig;
#[cfg(feature = "orbit")]
pub use claviron_http::config::routes::OrbitRouteConfig;
pub use claviron_http::config::routes::RouteRule;
use claviron_http::config::routes::RouteTable;
pub use claviron_http::config::statics::{
    DYNAMIC_FILE_EXTS, StaticConfig, is_dynamic_file,
};
pub use claviron_http::config::{
    acl, caching as policy, cors, response, transport,
};
use claviron_http::ext::PathPrefix;
pub use claviron_http::policy::{
    HttpPolicyOverride, RequestPolicyOverride, ResponsePolicyOverride,
};
#[cfg(feature = "php")]
use claviron_php::PhpConfig;
#[cfg(feature = "broadcast")]
use claviron_pusher::PusherConfig;
pub use claviron_tls::auth::{ClientAuthMode, ClientAuthPolicy};
pub use claviron_tls::handshake::HandshakePolicy;
use claviron_types::Aliases;
pub use policy::{
    CachingConfig, CompressionAlgorithm, CompressionConfig, RulesConfig,
};
pub use response::{LocationTemplate, RedirectConfig, RedirectStatus};
use serde::Deserialize;

mod routing;
pub use routing::RouteConfig;
#[cfg(feature = "rate-limit")]
pub mod quota;
#[cfg(feature = "rate-limit")]
pub use quota::RateLimitConfig;

/// Host identity/transport plus normalized application route declarations.
pub struct HostConfig {
    fields: HostFields,
    pub routes: RouteTable<Arc<HostConfig>>,
    entry_rules: Vec<PathPrefix>,
    fallback: Option<Arc<HostConfig>>,
    pub(crate) extensions:
        std::collections::BTreeMap<String, serde_yaml::Value>,
    pub(crate) owns_handler: bool,
}

impl Deref for HostConfig {
    type Target = HostFields;

    fn deref(&self) -> &Self::Target {
        &self.fields
    }
}

impl std::ops::DerefMut for HostConfig {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.fields
    }
}
#[cfg(feature = "http3")]
pub use transport::{EarlyDataPolicy, H3Policy};
pub use transport::{H1Policy, H2Policy, NonSslPolicy, TransportPolicy};

/// Feature-dependent composition container: inactive sections and unknown
/// top-level keys are retained as extension directives; fixed nested policies
/// retain strict parsing. Directive resolution validates names at boot.
#[derive(Deserialize)]
pub struct HostFields {
    /// Request/file declarations shared by the root and every route scope.
    #[serde(flatten)]
    pub routing: RouteConfig,
    /// Missing = compatible non-explicit listeners. A list restricts all
    /// listener membership; an empty list exposes this host nowhere.
    #[serde(default, deserialize_with = "opt_one_or_many")]
    pub listeners: Option<Vec<claviron_http::ListenerId>>,
    #[serde(default, alias = "names", alias = "name", alias = "alias")]
    aliases: Aliases,
    /// TLS handshake policy, separate from HTTP transport/request policy.
    #[serde(default)]
    pub tls: HandshakePolicy,
    /// HTTP transport/request boundary policy. The old `https:`
    /// config key is accepted only as a deprecated deserialize alias.
    #[serde(default, alias = "https")]
    pub transport: TransportPolicy,
    /// Provider-defined root directives, decoded only during route preparation.
    #[serde(flatten)]
    pub(super) directives:
        std::collections::BTreeMap<String, serde_yaml::Value>,
}

impl Deref for HostFields {
    type Target = RouteConfig;

    fn deref(&self) -> &Self::Target {
        &self.routing
    }
}

impl std::ops::DerefMut for HostFields {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.routing
    }
}

impl HostConfig {
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
            fields: HostFields {
                routing: RouteConfig {
                    files: claviron_http::config::files::FilesConfig {
                        root: root.into(),
                        mapping: None,
                        index: Vec::new(),
                        try_files: false,
                        spa: None,
                        statics: None,
                    },
                    access_log: None,
                    redirect: None,
                    acl: None,
                    #[cfg(feature = "rate-limit")]
                    rate_limit: None,
                    requests: RequestPolicyOverride::default(),
                    responses: ResponsePolicyOverride::default(),
                    cors: None,
                    headers: None,
                    #[cfg(feature = "php")]
                    php: None,
                    #[cfg(feature = "lua")]
                    lua: None,
                    #[cfg(feature = "orbit")]
                    orbit: None,
                    #[cfg(feature = "broadcast")]
                    broadcast: None,
                },
                listeners: None,
                aliases: Aliases::default(),
                tls: HandshakePolicy::default(),
                transport: TransportPolicy::default(),
                directives: Default::default(),
            },
            routes: RouteTable::default(),
            entry_rules: Vec::new(),
            fallback: None,
            extensions: Default::default(),
            owns_handler: false,
        }
    }

    /// A custom directive declared on this host or route, before inheritance.
    pub fn directive(
        &self,
        name: &str,
    ) -> Option<&serde_yaml::Value> {
        self.extensions.get(name)
    }

    pub fn entry_rules(&self) -> &[PathPrefix] {
        &self.entry_rules
    }

    pub(crate) fn fallback(&self) -> Option<&Arc<HostConfig>> {
        self.fallback.as_ref()
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

    /// One declaration view: flat fields are the root; explicit children keep
    /// their route-table prefix. An explicit `/` is already merged into self.
    pub fn scopes(
        &self
    ) -> impl Iterator<Item = (Option<&PathPrefix>, &HostConfig)> {
        std::iter::once((None, self)).chain(
            self.routes
                .iter()
                .filter(|(prefix, _)| prefix.as_str() != "/")
                .map(|(prefix, route)| (Some(prefix), route.as_ref())),
        )
    }

    /// Boot/reload lookup over declared ancestors; values retain one owner.
    pub fn setting_at<'a, T: ?Sized>(
        &'a self,
        prefix: &PathPrefix,
        setting: impl Fn(&'a RouteConfig) -> Option<&'a T>,
    ) -> Option<&'a T> {
        self.routes
            .iter()
            .filter(|(ancestor, _)| ancestor.has_path_segment(prefix.as_str()))
            .find_map(|(_, scope)| setting(&scope.fields.routing))
            .or_else(|| setting(&self.fields.routing))
    }

    /// Narrow one listener's request/reply policy through root and ancestors
    /// at boot/reload. Limits never widen in a more specific scope.
    pub(crate) fn policy_at(
        &self,
        prefix: &PathPrefix,
        listener: claviron_http::policy::HttpPolicy,
    ) -> claviron_http::policy::HttpPolicy {
        self.scopes_at(prefix)
            .fold(listener, |policy, scope| policy.narrow(&scope.policy()))
    }

    /// Borrow root and matching prefix declarations from outermost to nearest.
    pub(crate) fn scopes_at<'a>(
        &'a self,
        prefix: &'a PathPrefix,
    ) -> impl DoubleEndedIterator<Item = &'a RouteConfig> {
        std::iter::once(&self.fields.routing).chain(
            self.routes.iter().rev().filter_map(move |(ancestor, scope)| {
                (ancestor.as_str() != "/"
                    && ancestor.has_path_segment(prefix.as_str()))
                .then_some(&scope.fields.routing)
            }),
        )
    }

    /// Every Link destination needed by host-level lifecycle coordination.
    #[cfg(feature = "orbit")]
    pub(crate) fn link_routes(
        &self
    ) -> impl Iterator<Item = (Option<&PathPrefix>, &OrbitRouteConfig)> {
        self.scopes().flat_map(|(prefix, scope)| {
            scope.orbit().iter().map(move |route| (prefix, route))
        })
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

/// The file service reads a host through this view; it does not know hosts.
impl claviron_files::FileSite for HostConfig {
    fn root(&self) -> PathBuf {
        self.files.mapping.as_ref().map_or_else(
            || self.routing.root(),
            |mapping| mapping.directory().to_path_buf(),
        )
    }
    fn mapping(&self) -> Option<&claviron_http::config::mapping::RouteMapping> {
        self.files.mapping.as_ref()
    }
    fn route_for(
        &self,
        path: &str,
    ) -> Option<&dyn claviron_files::FileSite> {
        self.routes
            .find(path)
            .map(|route| route.as_ref() as &dyn claviron_files::FileSite)
    }
    fn indexes(&self) -> &[String] {
        self.routing.indexes()
    }
    fn try_files(&self) -> bool {
        self.try_files
    }
    fn spa(&self) -> Option<&str> {
        self.spa.as_ref().and_then(|spa| spa.entry.to_str())
    }
    fn spa_config(&self) -> Option<&claviron_http::config::spa::SpaConfig> {
        self.spa.as_ref()
    }
    fn statics(&self) -> Option<&[StaticConfig]> {
        self.routing.statics()
    }
    fn has_static_index(&self) -> bool {
        self.routing.has_static_index()
    }
}

/// File/application view already selected by Edge's route table. Native Rust
/// intercepts use this instead of reselecting a route or reusing the original
/// host-wide file policy. Canonical host metadata remains a separate handle.
#[derive(Clone)]
pub struct SelectedRoute(pub Arc<HostConfig>);
