//! Host and route declarations, independent of provider boot and backend preparation.

use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;

use claviron_helpers::de::opt_one_or_many;
use claviron_http::config::cors::CorsConfig;
pub use claviron_http::config::host::{
    DEFAULT_SERVER_HEADER, HostHeadersConfig,
};
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
pub(crate) use routing::is_builtin_directive;
#[cfg(feature = "rate-limit")]
pub mod quota;
#[cfg(feature = "rate-limit")]
pub use quota::RateLimitConfig;

/// What one application serves: its request and file declarations, its route
/// scopes and the directives its owners decode. A route is a `Site` of its own,
/// and so is a virtual host's root; nothing here knows how a request found it.
pub struct Site {
    pub routing: RouteConfig,
    pub routes: RouteTable<Arc<Site>>,
    entry_rules: Vec<PathPrefix>,
    fallback: Option<Arc<Site>>,
    pub(crate) extensions:
        std::collections::BTreeMap<String, serde_yaml::Value>,
    /// The static mounts here were taken from the closest declaring ancestor,
    /// not declared on this scope. A scope with an application owner is
    /// prepared without them.
    pub(crate) statics_inherited: bool,
}

impl Deref for Site {
    type Target = RouteConfig;

    fn deref(&self) -> &Self::Target {
        &self.routing
    }
}

impl std::ops::DerefMut for Site {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.routing
    }
}

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

/// One flat declaration as written: inactive sections and unknown top-level
/// keys are retained as extension directives; fixed nested policies retain
/// strict parsing. Directive resolution validates names at boot.
#[derive(Deserialize)]
pub(crate) struct HostFields {
    /// Request/file declarations shared by the root and every route scope.
    #[serde(flatten)]
    pub(crate) routing: RouteConfig,
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
    /// Provider-defined root directives, decoded only during route preparation.
    #[serde(flatten)]
    pub(super) directives:
        std::collections::BTreeMap<String, serde_yaml::Value>,
}

impl Site {
    /// An empty site with an explicit file root and the same policy defaults
    /// as a deserialized one.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
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
                #[cfg(feature = "orbit")]
                orbit: None,
            },
            routes: RouteTable::default(),
            entry_rules: Vec::new(),
            fallback: None,
            extensions: Default::default(),
            statics_inherited: false,
        }
    }

    /// A custom directive declared on this site or route, before inheritance.
    pub fn directive(
        &self,
        name: &str,
    ) -> Option<&serde_yaml::Value> {
        self.extensions.get(name)
    }

    pub fn entry_rules(&self) -> &[PathPrefix] {
        &self.entry_rules
    }

    pub(crate) fn fallback(&self) -> Option<&Arc<Site>> {
        self.fallback.as_ref()
    }

    /// One declaration view: flat fields are the root; explicit children keep
    /// their route-table prefix. An explicit `/` is already merged into self.
    pub fn scopes(&self) -> impl Iterator<Item = (Option<&PathPrefix>, &Site)> {
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
            .find_map(|(_, scope)| setting(&scope.routing))
            .or_else(|| setting(&self.routing))
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
        std::iter::once(&self.routing).chain(
            self.routes.iter().rev().filter_map(move |(ancestor, scope)| {
                (ancestor.as_str() != "/"
                    && ancestor.has_path_segment(prefix.as_str()))
                .then_some(&scope.routing)
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
}

impl HostConfig {
    /// One normalized declaration, split into what connects and what serves.
    pub(crate) fn from_fields(
        fields: HostFields,
        entry_rules: Vec<PathPrefix>,
        extensions: std::collections::BTreeMap<String, serde_yaml::Value>,
    ) -> Self {
        let HostFields { routing, listeners, aliases, tls, transport, .. } =
            fields;
        Self {
            site: Arc::new(Site {
                routing,
                routes: RouteTable::default(),
                entry_rules,
                fallback: None,
                extensions,
                statics_inherited: false,
            }),
            listeners,
            aliases,
            tls,
            transport,
        }
    }

    /// The site alone: a route scope has no connection terms of its own.
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

/// The file service reads a host through this view; it does not know hosts.
impl claviron_files::FileSite for Site {
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
