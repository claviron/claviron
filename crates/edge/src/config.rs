//! Host and route declarations, independent of provider boot and backend preparation.

use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;

use claviron_helpers::de::opt_one_or_many;
use claviron_http::config::cors::CorsConfig;
pub use claviron_http::config::host::{
    DEFAULT_SERVER_HEADER, HostHeadersConfig, HostUpstreamConfig,
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
pub use claviron_http::tuning::RouterConfig;
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
    pub(crate) route_acl: Option<acl::AccessLimiter>,
    #[cfg(feature = "rate-limit")]
    pub(crate) route_rate_limit: Option<quota::RateLimitConfig>,
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
    /// Flat declarations are the compatibility view of the implicit root route.
    #[serde(flatten)]
    pub routing: RouteConfig,
    /// Missing = all listeners; an explicit empty list exposes this host nowhere.
    #[serde(default, deserialize_with = "opt_one_or_many")]
    pub listeners: Option<Vec<claviron_http::ListenerId>>,
    #[serde(default, alias = "names", alias = "name", alias = "alias")]
    aliases: Aliases,
    /// Where this host's access records go: a channel `logging.yaml`
    /// declares, or `off` for none. Unwritten, `logging.yaml`'s own
    /// `access_log`. Several hosts may name one channel; each record carries
    /// its host.
    #[serde(default)]
    access_log: Option<String>,
    /// Requests this host serves at once in a worker, replacing
    /// `defaults.request.max_concurrent_requests`: a count, or `off` for none.
    /// Left out, the default holds.
    #[serde(
        default,
        deserialize_with = "claviron_http::tuning::optional_count_limit"
    )]
    pub max_concurrent_requests: Option<claviron_http::policy::Limit<usize>>,
    /// Removed; read only to say where each field went.
    #[allow(dead_code, reason = "read only to refuse the removed key")]
    #[serde(
        default,
        rename = "tuning",
        deserialize_with = "claviron_http::tuning::removed_tuning"
    )]
    removed_tuning: (),
    #[serde(default, deserialize_with = "opt_one_or_many")]
    pub redirect: Option<Vec<RedirectConfig>>,
    /// Opt-in segment-prefix ACL. `/` is the default; longest match wins.
    /// Runtime compilation and live admission belong to claviron-edge.
    acl: Option<acl::AclConfig>,
    /// Bounded IP admission before native sessions, routing and Other dispatch.
    #[cfg(feature = "rate-limit")]
    pub rate_limit: Option<quota::RateLimitConfig>,
    /// TLS handshake policy, separate from HTTP transport/request policy.
    #[serde(default)]
    pub tls: HandshakePolicy,
    /// HTTP transport/request boundary policy. The old `https:`
    /// config key is accepted only as a deprecated deserialize alias.
    #[serde(default, alias = "https")]
    pub transport: TransportPolicy,
    /// This host's request policy, under its listener's: a limit or a time
    /// written here can only narrow the listener's (`RequestPolicy::narrow`),
    /// and a host that needs more belongs on another listener. Unwritten
    /// fields leave the listener's as they are.
    ///
    /// Separate from `transport:` because that block is admission policy —
    /// which protocols, which handshakes — and this is how a request is
    /// accepted and run.
    #[serde(default)]
    pub requests: RequestPolicyOverride,
    /// This host's reply policy, under its listener's, narrowing the same
    /// way (`ResponsePolicy::narrow`).
    #[serde(default)]
    pub responses: ResponsePolicyOverride,
    /// Edge HTTP policy, outside the application routing boundary. Flat
    /// declarations cover `/`; scoped declarations are compiled at boot.
    pub cors: Option<CorsConfig>,
    #[serde(default)]
    pub headers: HostHeadersConfig,
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
        let left = &self.tls.client_auth;
        let right = &other.tls.client_auth;
        self.listeners == other.listeners
            && self.transport.protocols == other.transport.protocols
            && self.tls.require_alpn == other.tls.require_alpn
            && left.mode == right.mode
            && left.ca_bundle == right.ca_bundle
            && left.organization == right.organization
            && left.unit == right.unit
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
                    #[cfg(feature = "php")]
                    php: None,
                    #[cfg(feature = "lua")]
                    lua: None,
                    #[cfg(feature = "orbit")]
                    orbit: None,
                    #[cfg(feature = "broadcast")]
                    broadcast: None,
                    upstream: None,
                },
                listeners: None,
                aliases: Aliases::default(),
                access_log: None,
                max_concurrent_requests: None,
                removed_tuning: (),
                redirect: None,
                acl: None,
                #[cfg(feature = "rate-limit")]
                rate_limit: None,
                tls: HandshakePolicy::default(),
                transport: TransportPolicy::default(),
                requests: RequestPolicyOverride::default(),
                responses: ResponsePolicyOverride::default(),
                cors: None,
                headers: HostHeadersConfig::default(),
                directives: Default::default(),
            },
            routes: RouteTable::default(),
            entry_rules: Vec::new(),
            fallback: None,
            extensions: Default::default(),
            owns_handler: false,
            route_acl: None,
            #[cfg(feature = "rate-limit")]
            route_rate_limit: None,
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
    /// Both of this host's policy blocks, for narrowing its listener's.
    pub fn policy(&self) -> HttpPolicyOverride {
        HttpPolicyOverride {
            requests: self.requests,
            responses: self.responses,
        }
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

    pub fn indexes(&self) -> &[String] {
        &self.index
    }

    pub fn is_indexed(&self) -> bool {
        !self.index.is_empty()
    }

    pub fn has_static_index(&self) -> bool {
        self.is_indexed()
            && self.indexes().iter().any(|idx| !is_dynamic_file(idx))
    }

    /// Mounts compiled from route declarations for direct file consumers.
    pub fn statics(&self) -> Option<&[StaticConfig]> {
        self.statics.as_deref()
    }

    /// The document root as a path on disk.
    ///
    /// A relative root is relative to the working directory the server was
    /// started in — the application's own directory — not to the file that
    /// declared it. Includes are the other way round: an include names
    /// configuration, so it is found beside the file that names it.
    pub fn root(&self) -> PathBuf {
        let p = self.root.as_path();
        if p.is_absolute() {
            return p.to_path_buf();
        }
        std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")).join(p)
    }

    #[cfg(feature = "broadcast")]
    pub fn broadcast_cfg(&self) -> Option<Arc<PusherConfig>> {
        self.broadcast.clone()
    }

    /// This host's `access_log`, as written.
    pub fn access_log(&self) -> Option<&str> {
        self.access_log.as_deref()
    }

    #[cfg(feature = "php")]
    pub fn php(&self) -> &[PhpConfig] {
        self.php.as_deref().unwrap_or(&[])
    }

    #[cfg(feature = "lua")]
    pub fn lua(&self) -> &[LuaRouteConfig] {
        self.lua.as_deref().unwrap_or(&[])
    }

    #[cfg(feature = "orbit")]
    pub fn orbit(&self) -> &[OrbitRouteConfig] {
        self.orbit.as_deref().unwrap_or(&[])
    }

    pub fn acl(&self) -> Option<&acl::AclConfig> {
        self.acl.as_ref()
    }

    pub fn aliases(&self) -> &Aliases {
        &self.aliases
    }

    pub fn upstream_name(&self) -> Option<&str> {
        self.upstream.as_ref().map(HostUpstreamConfig::name)
    }

    /// Effective per-host transport policy (falls back to defaults if not configured).
    pub fn transport_policy(&self) -> &TransportPolicy {
        &self.transport
    }

    /// Effective per-host TLS handshake policy.
    pub fn tls_policy(&self) -> HandshakePolicy {
        self.tls.clone()
    }

    /// Effective `Server:` header value for this vhost. Returns the
    /// per-host override when configured, otherwise [`DEFAULT_SERVER_HEADER`].
    /// Always returns a value — the response layer never has to fabricate
    /// a fallback.
    pub fn server_name(&self) -> &str {
        self.headers.server.as_deref().unwrap_or(DEFAULT_SERVER_HEADER)
    }
}

/// The file service reads a host through this view; it does not know hosts.
impl claviron_files::FileSite for HostConfig {
    fn root(&self) -> PathBuf {
        self.files.mapping.as_ref().map_or_else(
            || HostConfig::root(self),
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
        HostConfig::indexes(self)
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
        HostConfig::statics(self)
    }
    fn has_static_index(&self) -> bool {
        HostConfig::has_static_index(self)
    }
}

/// File/application view already selected by Edge's route table. Native Rust
/// intercepts use this instead of reselecting a route or reusing the original
/// host-wide file policy. Canonical host metadata remains a separate handle.
#[derive(Clone)]
pub struct SelectedRoute(pub Arc<HostConfig>);

/// Shared handle to the active host configuration. Axum extraction is supplied
/// by the optional application adapter.
#[derive(Clone)]
pub struct HostConfigExt(pub Arc<HostConfig>);

impl Deref for HostConfigExt {
    type Target = HostConfig;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
