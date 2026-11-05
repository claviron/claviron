//! Boot-time normalization of flat host applications and prefix-keyed routes.

#[cfg(feature = "orbit")]
use super::OrbitRouteConfig;
#[cfg(feature = "rate-limit")]
use super::quota;
use super::{
    CorsConfig, DEFAULT_SERVER_HEADER, HostHeadersConfig, HttpPolicyOverride,
    RedirectConfig, RequestPolicyOverride, ResponsePolicyOverride,
    StaticConfig, acl, is_dynamic_file,
};
use super::{HostConfig, HostFields};
use claviron_helpers::de::opt_one_or_many;
use claviron_http::config::mapping::RouteMapping;
use claviron_http::config::routes::RouteTable;
use claviron_http::ext::PathPrefix;
use serde::de::Error;
use serde::{Deserialize, Deserializer};
use serde_yaml::{Mapping, Value};
use std::path::{Path, PathBuf};
use std::sync::Arc;

mod statics;

/// The same declaration schema at the host root and under every route key.
/// URL scope comes from the enclosing route; effective settings are borrowed
/// during boot/reload, rather than copied into child declarations.
#[derive(Deserialize)]
pub struct RouteConfig {
    #[serde(flatten)]
    pub files: claviron_http::config::files::FilesConfig,
    #[cfg(feature = "orbit")]
    #[serde(
        default,
        deserialize_with = "claviron_helpers::de::opt_one_or_many"
    )]
    pub(super) orbit: Option<Vec<super::OrbitRouteConfig>>,
    /// Where this host's access records go: a channel `logging.yaml`
    /// declares, or `off` for none. Unwritten, `logging.yaml`'s own
    /// `access_log`. Several hosts may name one channel; each record carries
    /// its host.
    #[serde(default)]
    pub(super) access_log: Option<String>,
    #[serde(default, deserialize_with = "opt_one_or_many")]
    pub redirect: Option<Vec<RedirectConfig>>,
    /// Access rule for this scope. Prefix selection belongs to the host plan.
    pub acl: Option<acl::AccessLimiter>,
    /// Bounded IP admission before native sessions, routing and Other dispatch.
    #[cfg(feature = "rate-limit")]
    pub rate_limit: Option<quota::RateLimitConfig>,
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
    pub headers: Option<HostHeadersConfig>,
}

impl RouteConfig {
    /// This scope's request and response overrides.
    pub fn policy(&self) -> HttpPolicyOverride {
        HttpPolicyOverride {
            requests: self.requests,
            responses: self.responses,
        }
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

    /// Resolve this scope's document root against the process working directory.
    pub fn root(&self) -> PathBuf {
        let p = self.root.as_path();
        if p.is_absolute() {
            return p.to_path_buf();
        }
        std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")).join(p)
    }

    /// This host's `access_log`, as written.
    pub fn access_log(&self) -> Option<&str> {
        self.access_log.as_deref()
    }

    #[cfg(feature = "orbit")]
    pub fn orbit(&self) -> &[OrbitRouteConfig] {
        self.orbit.as_deref().unwrap_or(&[])
    }

    pub fn acl(&self) -> Option<&acl::AccessLimiter> {
        self.acl.as_ref()
    }

    /// Declared server label, or the library identity.
    pub fn server_name(&self) -> &str {
        self.headers
            .as_ref()
            .and_then(|headers| headers.server.as_deref())
            .unwrap_or(DEFAULT_SERVER_HEADER)
    }
}

impl std::ops::Deref for RouteConfig {
    type Target = claviron_http::config::files::FilesConfig;
    fn deref(&self) -> &Self::Target {
        &self.files
    }
}

impl std::ops::DerefMut for RouteConfig {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.files
    }
}

impl<'de> Deserialize<'de> for HostConfig {
    fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        let source = Mapping::deserialize(de)?;
        normalize(source).map_err(D::Error::custom)
    }
}

fn key(name: &str) -> Value {
    Value::String(name.to_owned())
}

fn normalize(mut source: Mapping) -> Result<HostConfig, String> {
    if source.contains_key(key("path")) {
        return Err("path is not a directory setting; use root on the host or directory in a route".into());
    }
    let root = PathPrefix::root();
    validate_scope(&root, &source)?;
    for name in source.keys().filter_map(Value::as_str) {
        validate_feature(&root, name)?;
    }
    // Every leaf, including an explicit `/`, needs the same host default.
    source.entry(key("root")).or_insert_with(|| Value::String(".".into()));
    let routes = source.remove(key("routes"));
    let routes: RouteTable<Value> = match routes {
        None => RouteTable::default(),
        Some(value) => {
            serde_yaml::from_value(value).map_err(|error| error.to_string())?
        }
    };
    for (prefix, value) in routes.iter() {
        let route = value
            .as_mapping()
            .ok_or_else(|| format!("route '{prefix}' must be a mapping"))?;
        if route.contains_key(key("root")) {
            return Err(format!(
                "route '{prefix}' cannot declare root; root belongs to the host"
            ));
        }
    }
    let mut entries = Vec::new();
    let mut entry_rules = Vec::new();
    let mut fallback = source.clone();
    // Resolve the root route before children, without redefining the host root.
    if let Some(value) = routes.find("/") {
        let root = value.as_mapping().ok_or("route '/' must be a mapping")?;
        for (name, value) in root {
            if ["rules", "directory", "statics"]
                .iter()
                .any(|field| name == &key(field))
            {
                continue;
            }
            if source.contains_key(name) {
                return Err(format!(
                    "flat host field {name:?} conflicts with route '/'"
                ));
            }
            source.insert(name.clone(), value.clone());
        }
        // Navigation fallback may use files/SPA, never the rejected owner.
        for name in ["index", "try_files", "spa", "cors", "headers"] {
            if let Some(value) = source.get(key(name)) {
                fallback.insert(key(name), value.clone());
            }
        }
    }
    let host_root: PathBuf = serde_yaml::from_value(
        source
            .get(key("root"))
            .cloned()
            .unwrap_or_else(|| Value::String(".".into())),
    )
    .map_err(|error| error.to_string())?;
    let root_mapping = file_mapping(&host_root, &root, &routes)?;
    mount(&root, &mut source, &root_mapping)?;
    mount(&root, &mut fallback, &root_mapping)?;
    for (prefix, value) in routes.iter() {
        let mut route = value
            .as_mapping()
            .cloned()
            .ok_or_else(|| format!("route '{prefix}' must be a mapping"))?;
        validate_scope(prefix, &route)?;
        let mapping = file_mapping(&host_root, prefix, &routes)?;
        route.remove(key("directory"));
        let conditions = match route.remove(key("rules")) {
            Some(Value::Mapping(map)) => map,
            None => Mapping::new(),
            Some(_) => {
                return Err(format!(
                    "route '{prefix}' rules must be a mapping"
                ));
            }
        };
        let mut extensions = validate(prefix, &route)?;
        // Normalize the former rules wrapper into ordinary route directives.
        for (name, value) in conditions {
            let name = name.as_str().ok_or_else(|| {
                format!("route '{prefix}' directive name must be a string")
            })?;
            if extensions.insert(name.to_owned(), value).is_some() {
                return Err(format!(
                    "duplicate route directive '{name}' at '{prefix}'"
                ));
            }
        }
        for name in extensions.keys() {
            route.remove(key(name));
        }
        // Filesystem defaults are inherited, application owners are not.
        for name in ["root", "index"] {
            if !route.contains_key(key(name))
                && let Some(value) = source.get(key(name))
            {
                route.insert(key(name), value.clone());
            }
        }
        route.entry(key("root")).or_insert_with(|| Value::String(".".into()));
        let selected_static = route.contains_key(key("static"));
        mount(prefix, &mut route, &mapping)?;
        if prefix.as_str() == "/" {
            if selected_static {
                source.remove(key("static"));
            }
            if let Some(mounts) = route.get(key("statics")) {
                source.insert(key("statics"), mounts.clone());
            }
        }
        // Explicit `/` and flat declarations name the same root scope,
        // not a new files pipeline that discards flat mounts and SPA settings.
        let mut config = if prefix.as_str() == "/" {
            leaf(source.clone(), Vec::new())?
        } else {
            leaf(route, Vec::new())?
        };
        config.extensions.extend(extensions);
        config.files.mapping = Some(mapping);
        entries.push((prefix.clone(), Arc::new(config)));
        entry_rules.push(prefix.clone());
    }
    let mut host = leaf(source.clone(), entry_rules)?;
    // File-only consumers, including axum-free Other, receive the same mounts
    // compiled from route keys. This is not a second configuration surface.
    let declared_mounts: Vec<_> = entries
        .iter()
        .filter_map(|(prefix, route)| {
            route
                .statics
                .as_ref()
                .filter(|mounts| !mounts.is_empty())
                .map(|mounts| (prefix.clone(), mounts.clone()))
        })
        .collect();
    for (prefix, config) in &mut entries {
        // Inherit only the closest declaring scope, never mounts owned by a
        // descendant or sibling route. Keep directory-group prefixes intact.
        // Whether this scope has an application owner is not known here; its
        // preparation leaves inherited mounts out when it has one.
        if config.statics.as_ref().is_none_or(Vec::is_empty)
            && let Some((_, mounts)) =
                declared_mounts.iter().find(|(ancestor, _)| {
                    ancestor.has_path_segment(prefix.as_str())
                })
        {
            let config = Arc::get_mut(config)
                .expect("route configuration has not been published");
            let mapping =
                config.files.mapping.as_ref().expect("prepared route mapping");
            let mut inherited = Vec::new();
            for mount in mounts {
                let mut mount = mount.clone();
                if let Some(suffix) = mapping.relative_path(mount.path.as_str())
                {
                    // The directory group is below the selected route.
                    mount.dir = mapping.directory().join(suffix);
                } else if mount.path.has_path_segment(prefix.as_str()) {
                    let suffix = prefix
                        .as_str()
                        .strip_prefix(mount.path.as_str().trim_end_matches('/'))
                        .expect("matching static ancestor")
                        .trim_start_matches('/');
                    // A policy-only child does not change the mount-relative
                    // name seen by cache rules. An explicit disk remap still
                    // establishes a new directory/prefix pair.
                    if mount.dir.join(suffix) != mapping.directory() {
                        mount.path = prefix.clone();
                        mount.dir = mapping.directory().to_path_buf();
                    }
                } else {
                    continue;
                }
                inherited.push(mount);
            }
            if !inherited.is_empty() {
                config.files.statics = Some(inherited);
                config.statics_inherited = true;
            }
        }
    }
    let mounts: Vec<_> =
        declared_mounts.into_iter().flat_map(|(_, mounts)| mounts).collect();
    if !mounts.is_empty() {
        host.files.statics = Some(mounts);
    }
    if !host.extensions.is_empty()
        && !entries.iter().any(|(prefix, _)| prefix.as_str() == "/")
    {
        let root = leaf(source, Vec::new())?;
        host.entry_rules.push(PathPrefix::root());
        entries.push((PathPrefix::root(), Arc::new(root)));
    }
    host.routes = RouteTable::new(entries);
    if !host.routes.is_empty() {
        fallback
            .entry(key("root"))
            .or_insert_with(|| Value::String(".".into()));
        let mut fallback = leaf(fallback, Vec::new())?;
        if routes.find("/").is_some() {
            fallback.files.mapping =
                Some(file_mapping(&host_root, &PathPrefix::root(), &routes)?);
        }
        host.fallback = Some(Arc::new(fallback));
    }
    Ok(host)
}

/// A directive the edge decodes and executes itself. Every other name is kept
/// as written for whoever registered it: a backend or a route extension. The
/// edge does not know those names, so what they mean, and whether a scope has
/// one application owner, is settled when the scope is prepared.
pub(crate) fn is_builtin_directive(name: &str) -> bool {
    name == "orbit"
        || matches!(
            name,
            "root"
                | "static"
                | "directory"
                | "index"
                | "try_files"
                | "spa"
                | "statics"
                | "cors"
                | "headers"
                | "acl"
                | "rate_limit"
                | "redirect"
                | "access_log"
                | "requests"
                | "responses"
        )
}

fn validate_feature(
    prefix: &PathPrefix,
    name: &str,
) -> Result<(), String> {
    let supported = match name {
        "orbit" => cfg!(feature = "orbit"),
        "rate_limit" => cfg!(feature = "rate-limit"),
        _ => true,
    };
    if !supported {
        return Err(format!(
            "directive '{name}' on route '{prefix}' is not enabled"
        ));
    }
    Ok(())
}

fn leaf(
    source: Mapping,
    entry_rules: Vec<PathPrefix>,
) -> Result<HostConfig, String> {
    #[cfg(not(feature = "rate-limit"))]
    if source.contains_key(key("rate_limit")) {
        return Err("rate_limit requires the rate-limit feature".into());
    }
    let mut fields: HostFields = serde_yaml::from_value(Value::Mapping(source))
        .map_err(|error| error.to_string())?;
    let mut extensions = std::mem::take(&mut fields.directives);
    #[cfg(feature = "rate-limit")]
    if let Some(rate_limit) = &fields.rate_limit {
        rate_limit.validate()?;
    }
    // Nested flattened structs may leave their recognized keys visible to a
    // sibling flattened map. Only provider directives belong to that map.
    extensions.retain(|name, _| !is_builtin_directive(name));
    Ok(HostConfig {
        fields,
        routes: RouteTable::default(),
        entry_rules,
        fallback: None,
        extensions,
        statics_inherited: false,
    })
}

fn validate(
    prefix: &PathPrefix,
    route: &Mapping,
) -> Result<std::collections::BTreeMap<String, Value>, String> {
    let mut extensions = std::collections::BTreeMap::new();
    for (name, value) in route {
        let name = name.as_str().ok_or("route directive must be a string")?;
        if !is_builtin_directive(name) {
            extensions.insert(name.to_owned(), value.clone());
        }
        validate_feature(prefix, name)?;
    }
    Ok(extensions)
}

/// URL scope is owned by the routes key, never an individual feature.
fn validate_scope(
    prefix: &PathPrefix,
    scope: &Mapping,
) -> Result<(), String> {
    if scope.contains_key(key("path")) {
        return Err(format!(
            "route '{prefix}' uses its key as the URL prefix; use directory for a disk mapping"
        ));
    }
    for name in ["orbit", "redirect"] {
        let Some(value) = scope.get(key(name)) else {
            continue;
        };
        let entries = match value {
            Value::Sequence(entries) => entries.as_slice(),
            _ => std::slice::from_ref(value),
        };
        for entry in entries {
            if entry
                .as_mapping()
                .is_some_and(|map| map.contains_key(key("path")))
            {
                return Err(format!(
                    "'{name}.path' is not supported; declare its scope under routes"
                ));
            }
        }
    }
    Ok(())
}

fn mount(
    prefix: &PathPrefix,
    route: &mut Mapping,
    mapping: &RouteMapping,
) -> Result<(), String> {
    let mut mounts = match route.remove(key("statics")) {
        Some(source) => statics::compile(prefix, mapping, source)?,
        None => Vec::new(),
    };
    if let Some(mut config) = route.remove(key("static")) {
        let map = config.as_mapping_mut().ok_or_else(|| {
            format!("route '{prefix}' static must be a mapping")
        })?;
        if map.contains_key(key("path")) {
            return Err(format!(
                "route '{prefix}' owns its path; remove 'static.path'"
            ));
        }
        if map.contains_key(key("root")) {
            return Err(format!(
                "route '{prefix}' static cannot declare root; use route.directory"
            ));
        }
        if map.contains_key(key("dir")) {
            return Err(format!(
                "route '{prefix}' static.dir is replaced by route.directory"
            ));
        }
        map.insert(key("path"), Value::String(prefix.as_str().into()));
        map.insert(
            key("dir"),
            serde_yaml::to_value(mapping.directory())
                .map_err(|error| error.to_string())?,
        );
        mounts.push(config);
    }
    if !mounts.is_empty() {
        // Directory policies are selected by their longest derived prefix.
        mounts.sort_by_key(|mount| {
            std::cmp::Reverse(
                mount
                    .as_mapping()
                    .and_then(|mount| mount.get(key("path")))
                    .and_then(Value::as_str)
                    .map_or(0, str::len),
            )
        });
        route.insert(key("statics"), Value::Sequence(mounts));
    }
    Ok(())
}

/// Resolve the nearest declared directory mapping once during preparation.
/// With no override, URL components name directories below the host root.
fn file_mapping(
    host_root: &Path,
    prefix: &PathPrefix,
    routes: &RouteTable<Value>,
) -> Result<RouteMapping, String> {
    let root = if host_root.is_absolute() {
        host_root.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|error| error.to_string())?
            .join(host_root)
    };
    let directory = match routes.iter().find_map(|(ancestor, value)| {
        ancestor
            .has_path_segment(prefix.as_str())
            .then(|| {
                value
                    .as_mapping()?
                    .get(key("directory"))
                    .map(|path| (ancestor, path))
            })
            .flatten()
    }) {
        Some((ancestor, value)) => {
            let path: PathBuf =
                serde_yaml::from_value(value.clone()).map_err(|error| {
                    format!("route '{ancestor}' directory: {error}")
                })?;
            if path.as_os_str().is_empty() {
                return Err(format!(
                    "route '{ancestor}' directory must not be empty"
                ));
            }
            let suffix = prefix
                .as_str()
                .strip_prefix(ancestor.as_str().trim_end_matches('/'))
                .expect("matching route ancestor");
            root.join(path).join(directory_suffix(prefix, suffix)?)
        }
        None => root.join(directory_suffix(prefix, prefix.as_str())?),
    };
    Ok(RouteMapping::new(prefix.clone(), directory))
}

fn directory_suffix<'a>(
    prefix: &PathPrefix,
    suffix: &'a str,
) -> Result<&'a str, String> {
    let suffix = suffix.trim_start_matches('/');
    if suffix.split('/').any(|segment| matches!(segment, "." | "..")) {
        return Err(format!(
            "route '{prefix}' cannot derive a directory from dot segments; declare directory explicitly"
        ));
    }
    Ok(suffix)
}

#[cfg(test)]
#[path = "../../tests/config/routing.rs"]
mod tests;
