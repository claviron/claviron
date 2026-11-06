//! What one application serves, independent of how a request found it.

use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;

#[cfg(feature = "orbit")]
use claviron_http::config::routes::OrbitRouteConfig;
use claviron_http::config::routes::RouteTable;
use claviron_http::config::statics::StaticConfig;
use claviron_http::ext::PathPrefix;
use claviron_http::policy::{RequestPolicyOverride, ResponsePolicyOverride};

#[cfg(feature = "rate-limit")]
pub mod quota;
mod routing;

pub use routing::RouteConfig;
pub(crate) use routing::is_builtin_directive;

/// What one application serves: its request and file declarations, its route
/// scopes and the directives its owners decode. A route is a `Site` of its own,
/// and so is a virtual host's root; nothing here knows how a request found it.
pub struct Site {
    pub routing: RouteConfig,
    pub routes: RouteTable<Arc<Site>>,
    entry_rules: Vec<PathPrefix>,
    fallback: Option<Arc<Site>>,
    pub extensions: std::collections::BTreeMap<String, serde_yaml::Value>,
    /// The static mounts here were taken from the closest declaring ancestor,
    /// not declared on this scope. A scope with an application owner is
    /// prepared without them.
    pub statics_inherited: bool,
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

impl Site {
    pub(crate) fn from_parts(
        routing: RouteConfig,
        entry_rules: Vec<PathPrefix>,
        extensions: std::collections::BTreeMap<String, serde_yaml::Value>,
    ) -> Self {
        Self {
            routing,
            routes: RouteTable::default(),
            entry_rules,
            fallback: None,
            extensions,
            statics_inherited: false,
        }
    }

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

    pub(crate) fn entry_rules(&self) -> &[PathPrefix] {
        &self.entry_rules
    }

    pub fn fallback(&self) -> Option<&Arc<Site>> {
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
    pub fn policy_at(
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
    pub fn link_routes(
        &self
    ) -> impl Iterator<Item = (Option<&PathPrefix>, &OrbitRouteConfig)> {
        self.scopes().flat_map(|(prefix, scope)| {
            scope.orbit().iter().map(move |route| (prefix, route))
        })
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
