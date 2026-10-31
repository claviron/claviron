//! Boot-time normalization of flat host applications and prefix-keyed routes.

use super::{HostConfig, HostFields, RouteRule};
use claviron_http::config::mapping::RouteMapping;
use claviron_http::config::routes::RouteTable;
use claviron_http::ext::PathPrefix;
use serde::de::Error;
use serde::{Deserialize, Deserializer};
use serde_yaml::{Mapping, Value};
use std::path::{Path, PathBuf};
use std::sync::Arc;

mod statics;

/// Prepared application/file view, independent of host identity and transport.
/// Host parsing supplies the single host root and compiles each static mount
/// from its route key. The internal file view is not a second YAML surface.
#[derive(Deserialize)]
pub struct RouteConfig {
    #[serde(flatten)]
    pub files: claviron_http::config::files::FilesConfig,
    #[cfg(feature = "php")]
    #[serde(
        default,
        deserialize_with = "claviron_helpers::de::opt_one_or_many"
    )]
    pub(super) php: Option<Vec<super::PhpConfig>>,
    #[cfg(feature = "lua")]
    #[serde(
        default,
        deserialize_with = "claviron_helpers::de::opt_one_or_many"
    )]
    pub(super) lua: Option<Vec<super::LuaRouteConfig>>,
    #[cfg(feature = "orbit")]
    #[serde(
        default,
        deserialize_with = "claviron_helpers::de::opt_one_or_many"
    )]
    pub(super) orbit: Option<Vec<super::OrbitRouteConfig>>,
    #[cfg(feature = "broadcast")]
    #[serde(alias = "pusher")]
    pub(super) broadcast: Option<Arc<super::PusherConfig>>,
    #[serde(default)]
    pub upstream: Option<super::HostUpstreamConfig>,
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
    for name in ["static", "statics"] {
        if source.contains_key(key(name)) {
            return Err(format!(
                "host '{name}' is not supported; declare static directories under routes"
            ));
        }
    }
    if source.contains_key(key("path")) {
        return Err("path belongs to a route; use root on the host".into());
    }
    let root = PathPrefix::root();
    for name in source.keys().filter_map(Value::as_str) {
        validate_feature(&root, name)?;
    }
    let routes = source.remove(key("routes"));
    // Keep the initial rule-only schema readable while deployments migrate.
    if routes.as_ref().is_some_and(|value| value.is_sequence())
        || routes.as_ref().and_then(Value::as_mapping).is_some_and(|map| {
            map.contains_key(key("path")) || map.contains_key(key("ajax_only"))
        })
    {
        let value = routes.unwrap();
        let entry_rules = if value.is_sequence() {
            serde_yaml::from_value::<Vec<RouteRule>>(value)
        } else {
            serde_yaml::from_value::<RouteRule>(value).map(|rule| vec![rule])
        }
        .map_err(|error| error.to_string())?;
        let fallback = Arc::new(leaf(source.clone(), Vec::new())?);
        let mut entries = Vec::new();
        let mut prefixes = Vec::new();
        for rule in entry_rules {
            if prefixes.contains(&rule.path) {
                return Err(format!("duplicate route prefix '{}'", rule.path));
            }
            let mut config = leaf(source.clone(), Vec::new())?;
            config
                .extensions
                .insert("ajax_only".into(), Value::Bool(rule.ajax_only));
            prefixes.push(rule.path.clone());
            entries.push((rule.path, Arc::new(config)));
        }
        let mut host = leaf(source, prefixes)?;
        host.routes = RouteTable::new(entries);
        host.fallback = Some(fallback);
        return Ok(host);
    }
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
            if ["rules", "acl", "path", "statics"]
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
        let owner_names = [
            "php",
            "lua",
            "orbit",
            "upstream",
            "broadcast",
            "pusher",
            "static",
        ];
        if owner_names
            .iter()
            .filter(|name| source.contains_key(key(name)))
            .count()
            > 1
        {
            return Err(
                "route '/' conflicts with the flat application owner".into()
            );
        }
    }
    let host_root: PathBuf = serde_yaml::from_value(
        source
            .get(key("root"))
            .cloned()
            .unwrap_or_else(|| Value::String(".".into())),
    )
    .map_err(|error| error.to_string())?;
    for (prefix, value) in routes.iter() {
        let mut route = value
            .as_mapping()
            .cloned()
            .ok_or_else(|| format!("route '{prefix}' must be a mapping"))?;
        let mapping = file_mapping(&host_root, prefix, &routes)?;
        route.remove(key("path"));
        let route_acl = route
            .remove(key("acl"))
            .map(serde_yaml::from_value::<super::acl::AccessLimiter>)
            .transpose()
            .map_err(|error| format!("route '{prefix}' ACL: {error}"))?;
        #[cfg(feature = "rate-limit")]
        let route_rate_limit = route
            .remove(key("rate_limit"))
            .map(serde_yaml::from_value::<super::quota::RateLimitConfig>)
            .transpose()
            .map_err(|error| format!("route '{prefix}' rate_limit: {error}"))?;
        #[cfg(feature = "rate-limit")]
        if let Some(definition) = &route_rate_limit {
            definition.validate()?;
        }
        if route_acl.is_some()
            && (prefix.contains(['%', '?', '#', '\\'])
                || prefix.contains("//")
                || prefix
                    .split('/')
                    .any(|segment| matches!(segment, "." | "..")))
        {
            return Err(format!("ambiguous ACL route prefix '{prefix}'"));
        }
        let conditions = match route.remove(key("rules")) {
            Some(Value::Mapping(map)) => map,
            None => Mapping::new(),
            Some(_) => {
                return Err(format!(
                    "route '{prefix}' rules must be a mapping"
                ));
            }
        };
        let (mut extensions, owns_handler) = validate(prefix, &route)?;
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
        // Compile one effective policy, never stack host and route copies.
        // A declaration on `/` covers descendants too; the closest explicit
        // declaration wins naturally when a child supplies its own policy.
        for name in ["cors", "headers"] {
            if !route.contains_key(key(name)) {
                let inherited = routes
                    .iter()
                    .find_map(|(ancestor, value)| {
                        ancestor
                            .has_path_segment(prefix.as_str())
                            .then(|| value.as_mapping()?.get(key(name)))
                            .flatten()
                    })
                    .or_else(|| source.get(key(name)));
                if let Some(value) = inherited {
                    route.insert(key(name), value.clone());
                }
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
        // Explicit `/` is the same root owner as the flat compatibility view,
        // not a new files pipeline that discards flat mounts and SPA settings.
        let mut config = if prefix.as_str() == "/" {
            leaf(source.clone(), Vec::new())?
        } else {
            leaf(route, Vec::new())?
        };
        config.extensions.extend(extensions);
        config.owns_handler = owns_handler
            || (prefix.as_str() == "/" && has_application(&source));
        config.files.mapping = Some(mapping);
        config.route_acl = route_acl;
        #[cfg(feature = "rate-limit")]
        {
            config.route_rate_limit = route_rate_limit;
        }
        entries.push((prefix.clone(), Arc::new(config)));
        entry_rules.push(prefix.clone());
    }
    source.entry(key("root")).or_insert_with(|| Value::String(".".into()));
    let mut host = leaf(source.clone(), entry_rules)?;
    // File-only consumers, including axum-free Other, receive the same mounts
    // compiled from route keys. This is not a second configuration surface.
    let mounts: Vec<_> = entries
        .iter()
        .flat_map(|(_, route)| route.statics.iter().flatten().cloned())
        .collect();
    for (prefix, config) in &mut entries {
        // A policy-only child keeps the enclosing static policy, paired with
        // its own prepared directory/prefix mapping.
        if !config.owns_handler
            && config.statics.as_ref().is_none_or(Vec::is_empty)
            && let Some(mount) = mounts
                .iter()
                .find(|mount| mount.path.has_path_segment(prefix.as_str()))
        {
            let config = Arc::get_mut(config)
                .expect("route configuration has not been published");
            let mut mount = mount.clone();
            mount.path = prefix.clone();
            mount.dir = config
                .files
                .mapping
                .as_ref()
                .expect("prepared route mapping")
                .directory()
                .to_path_buf();
            config.files.statics = Some(vec![mount]);
        }
    }
    if !mounts.is_empty() {
        host.files.statics = Some(mounts);
    }
    if !host.extensions.is_empty()
        && !entries.iter().any(|(prefix, _)| prefix.as_str() == "/")
    {
        let owns_handler = has_application(&source);
        let mut root = leaf(source, Vec::new())?;
        root.owns_handler = owns_handler;
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

fn has_application(source: &Mapping) -> bool {
    source
        .keys()
        .any(|name| name.as_str().is_some_and(is_application_directive))
}

fn is_application_directive(name: &str) -> bool {
    matches!(
        name,
        "php"
            | "lua"
            | "orbit"
            | "upstream"
            | "broadcast"
            | "pusher"
            | "static"
    )
}

fn is_builtin_directive(name: &str) -> bool {
    is_application_directive(name)
        || matches!(
            name,
            "root"
                | "path"
                | "index"
                | "try_files"
                | "spa"
                | "statics"
                | "cors"
                | "headers"
                | "rate_limit"
        )
}

fn validate_feature(prefix: &PathPrefix, name: &str) -> Result<(), String> {
    let supported = match name {
        "php" => cfg!(feature = "php"),
        "lua" => cfg!(feature = "lua"),
        "orbit" => cfg!(feature = "orbit"),
        "upstream" => cfg!(feature = "upstream"),
        "broadcast" | "pusher" => cfg!(feature = "broadcast"),
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
        owns_handler: false,
        route_acl: None,
        #[cfg(feature = "rate-limit")]
        route_rate_limit: None,
    })
}

fn validate(
    prefix: &PathPrefix,
    route: &Mapping,
) -> Result<(std::collections::BTreeMap<String, Value>, bool), String> {
    let mut owners = 0;
    let mut extensions = std::collections::BTreeMap::new();
    for (name, value) in route {
        let name = name.as_str().ok_or("route directive must be a string")?;
        if is_application_directive(name) {
            owners += 1;
        } else if !is_builtin_directive(name) {
            extensions.insert(name.to_owned(), value.clone());
        }
        validate_feature(prefix, name)?;
    }
    if owners > 1 {
        return Err(format!(
            "route '{prefix}' has more than one application owner"
        ));
    }
    Ok((extensions, owners != 0))
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
    for name in ["php", "lua", "orbit"] {
        if let Some(config) = route.get_mut(key(name)) {
            let map = config.as_mapping_mut().ok_or_else(|| {
                format!("route '{prefix}' requires one '{name}' configuration")
            })?;
            if map.contains_key(key("path")) {
                return Err(format!(
                    "route '{prefix}' owns its path; remove '{name}.path'"
                ));
            }
            map.insert(key("path"), Value::String(prefix.as_str().into()));
        }
    }
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
                "route '{prefix}' static cannot declare root; use the route path"
            ));
        }
        if map.contains_key(key("dir")) {
            return Err(format!(
                "route '{prefix}' static.dir is replaced by route.path"
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
                    .get(key("path"))
                    .map(|path| (ancestor, path))
            })
            .flatten()
    }) {
        Some((ancestor, value)) => {
            let path: PathBuf = serde_yaml::from_value(value.clone())
                .map_err(|error| format!("route '{ancestor}' path: {error}"))?;
            if path.as_os_str().is_empty() {
                return Err(format!(
                    "route '{ancestor}' path must not be empty"
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
            "route '{prefix}' cannot derive a directory from dot segments; declare path explicitly"
        ));
    }
    Ok(suffix)
}

#[cfg(test)]
#[path = "../../tests/config/routing.rs"]
mod tests;
