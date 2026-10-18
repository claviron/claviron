//! Boot-time normalization of flat host applications and prefix-keyed routes.

use super::{HostConfig, HostFields, RouteRule};
use claviron_http::config::routes::RouteTable;
use claviron_http::ext::PathPrefix;
use serde::de::Error;
use serde::{Deserialize, Deserializer};
use serde_yaml::{Mapping, Value};
use std::sync::Arc;

/// Application/file declarations, independent of host identity and transport.
/// The surrounding route table owns URL prefixes; paths here name files.
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
    let mut entries = Vec::new();
    let mut entry_rules = Vec::new();
    let mut fallback = source.clone();
    // Resolve the root before children, independent of map declaration order.
    if let Some(value) = routes.find("/") {
        let root = value.as_mapping().ok_or("route '/' must be a mapping")?;
        for (name, value) in root {
            if ["rules", "acl"].iter().any(|field| name == &key(field)) {
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
        for name in
            ["root", "index", "try_files", "spa", "statics", "cors", "headers"]
        {
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
    for (prefix, value) in routes.iter() {
        let mut route = value
            .as_mapping()
            .cloned()
            .ok_or_else(|| format!("route '{prefix}' must be a mapping"))?;
        let route_acl = route
            .remove(key("acl"))
            .map(serde_yaml::from_value::<super::acl::AccessLimiter>)
            .transpose()
            .map_err(|error| format!("route '{prefix}' ACL: {error}"))?;
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
        mount(prefix, &mut route)?;
        if prefix.as_str() == "/" && selected_static {
            source.remove(key("static"));
            source.insert(key("statics"), route[key("statics")].clone());
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
        config.selected_static = selected_static;
        config.route_acl = route_acl;
        entries.push((prefix.clone(), Arc::new(config)));
        entry_rules.push(prefix.clone());
    }
    source.entry(key("root")).or_insert_with(|| Value::String(".".into()));
    let mut host = leaf(source.clone(), entry_rules)?;
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
        host.fallback = Some(Arc::new(leaf(fallback, Vec::new())?));
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
                | "index"
                | "try_files"
                | "spa"
                | "statics"
                | "cors"
                | "headers"
        )
}

fn leaf(
    source: Mapping,
    entry_rules: Vec<PathPrefix>,
) -> Result<HostConfig, String> {
    let mut fields: HostFields = serde_yaml::from_value(Value::Mapping(source))
        .map_err(|error| error.to_string())?;
    let mut extensions = std::mem::take(&mut fields.directives);
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
        selected_static: false,
        route_acl: None,
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
        let supported = match name {
            "php" => cfg!(feature = "php"),
            "lua" => cfg!(feature = "lua"),
            "orbit" => cfg!(feature = "orbit"),
            "upstream" => cfg!(feature = "upstream"),
            "broadcast" | "pusher" => cfg!(feature = "broadcast"),
            _ => true,
        };
        if !supported {
            return Err(format!(
                "directive '{name}' on route '{prefix}' is not enabled"
            ));
        }
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
) -> Result<(), String> {
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
        map.insert(key("path"), Value::String(prefix.as_str().into()));
        route.insert(key("statics"), Value::Sequence(vec![config]));
    }
    Ok(())
}

#[cfg(test)]
#[path = "../../tests/config/routing.rs"]
mod tests;
