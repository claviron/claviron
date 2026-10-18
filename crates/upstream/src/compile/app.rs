//! Owns route compilation, ordering and validation before runtime matching.

use std::collections::BTreeMap;

use regex::Regex;

use crate::app::CompiledUpstreamApp;
use crate::app::matcher::{
    CompiledUpstreamRoute, ExcludeMatcher, RouteSelector, SelectorRank,
};
use crate::compile::profile::Profile;
use crate::config::UpstreamYaml;
use crate::config::app::{UpstreamAppConfig, UpstreamRouteConfig};
use crate::config::error::ConfigError;

impl UpstreamYaml {
    pub(crate) fn compile_apps(
        &self,
        profiles: &BTreeMap<String, std::sync::Arc<Profile>>,
    ) -> Result<BTreeMap<String, CompiledUpstreamApp>, ConfigError> {
        let mut apps = BTreeMap::new();
        for (name, app) in &self.apps {
            apps.insert(name.clone(), compile_app(name, app, profiles)?);
        }
        Ok(apps)
    }
}
fn compile_app(
    name: &str,
    app: &UpstreamAppConfig,
    profiles: &BTreeMap<String, std::sync::Arc<Profile>>,
) -> Result<CompiledUpstreamApp, ConfigError> {
    if app.routes.is_empty() {
        return Err(ConfigError::AppHasNoRoutes { app: name.to_owned() });
    }

    let mut exact = Vec::new();
    let mut prefixes = Vec::new();
    let mut regexes = Vec::new();
    let mut root_fallbacks = Vec::new();
    let mut unconditional_root_seen = false;

    for (index, route) in app.routes.iter().enumerate() {
        let route_number = index + 1;
        let profile = route.profile.as_deref().ok_or_else(|| {
            ConfigError::AppRouteMissingProfile {
                app: name.to_owned(),
                route: route_number,
            }
        })?;
        if !profiles.contains_key(profile) {
            return Err(ConfigError::AppRouteUnknownProfile {
                app: name.to_owned(),
                route: route_number,
                profile: profile.to_owned(),
            });
        }

        let selectors = compile_route_selectors(name, route_number, route)?;
        let excludes = compile_excludes(&route.excludes);
        for selector in selectors {
            let compiled = CompiledUpstreamRoute {
                profile: profile.to_owned(),
                selector,
                excludes: excludes.clone(),
            };
            match compiled.selector.rank() {
                SelectorRank::Exact => exact.push(compiled),
                SelectorRank::Prefix => prefixes.push(compiled),
                SelectorRank::Regex => regexes.push(compiled),
                SelectorRank::Root => {
                    if compiled.excludes.is_empty() {
                        if unconditional_root_seen {
                            return Err(
                                ConfigError::AppDuplicateRootFallback {
                                    app: name.to_owned(),
                                },
                            );
                        }
                        unconditional_root_seen = true;
                    }
                    root_fallbacks.push(compiled);
                }
            }
        }
    }

    prefixes.sort_by_key(|b| std::cmp::Reverse(b.selector.prefix_len()));

    Ok(CompiledUpstreamApp {
        name: name.to_owned(),
        exact,
        prefixes,
        regexes,
        root_fallbacks,
    })
}

fn compile_route_selectors(
    app: &str,
    route: usize,
    config: &UpstreamRouteConfig,
) -> Result<Vec<RouteSelector>, ConfigError> {
    let selector_count = usize::from(config.exact.is_some())
        + usize::from(config.path.is_some())
        + usize::from(config.paths.is_some())
        + usize::from(config.regex.is_some());
    if selector_count == 0 {
        return Ok(vec![RouteSelector::RootFallback]);
    }
    if selector_count > 1 {
        return Err(ConfigError::AppRouteSelectorCount {
            app: app.to_owned(),
            route,
        });
    }

    if let Some(exact) = config.exact.as_deref() {
        let exact = normalize_exact_path(app, route, exact)?;
        return Ok(vec![RouteSelector::Exact(exact)]);
    }

    if let Some(path) = config.path.as_deref() {
        return Ok(vec![compile_path_selector(app, route, path)?]);
    }

    if let Some(paths) = config.paths.as_deref() {
        if paths.is_empty() {
            return Err(ConfigError::AppRouteEmptySelector {
                app: app.to_owned(),
                route,
            });
        }
        let mut out = Vec::with_capacity(paths.len());
        for path in paths {
            out.push(compile_path_selector(app, route, path)?);
        }
        return Ok(out);
    }

    let pattern =
        config.regex.as_deref().expect("selector_count checked regex presence");
    let regex = Regex::new(pattern).map_err(|source| {
        ConfigError::AppRouteInvalidRegex {
            app: app.to_owned(),
            route,
            pattern: pattern.to_owned(),
            detail: source.to_string(),
        }
    })?;
    Ok(vec![RouteSelector::Regex(regex)])
}

fn normalize_exact_path(
    app: &str,
    route: usize,
    path: &str,
) -> Result<String, ConfigError> {
    if path.is_empty() {
        return Err(ConfigError::AppRouteEmptySelector {
            app: app.to_owned(),
            route,
        });
    }
    if !path.starts_with('/') {
        return Err(ConfigError::AppRouteSelectorMustStartWithSlash {
            app: app.to_owned(),
            route,
            selector: path.to_owned(),
        });
    }
    Ok(path.to_owned())
}

fn compile_path_selector(
    app: &str,
    route: usize,
    path: &str,
) -> Result<RouteSelector, ConfigError> {
    let path = normalize_required_path(app, route, path)?;
    if path == "/" {
        Ok(RouteSelector::RootFallback)
    } else {
        Ok(RouteSelector::PathPrefix(path))
    }
}

fn normalize_required_path(
    app: &str,
    route: usize,
    path: &str,
) -> Result<String, ConfigError> {
    if path.is_empty() {
        return Err(ConfigError::AppRouteEmptySelector {
            app: app.to_owned(),
            route,
        });
    }
    if !path.starts_with('/') {
        return Err(ConfigError::AppRouteSelectorMustStartWithSlash {
            app: app.to_owned(),
            route,
            selector: path.to_owned(),
        });
    }
    if path == "/" {
        return Ok(path.to_owned());
    }
    Ok(path.trim_end_matches('/').to_owned())
}

fn compile_excludes(patterns: &[String]) -> Vec<ExcludeMatcher> {
    patterns.iter().map(|pattern| compile_exclude(pattern)).collect()
}

fn compile_exclude(pattern: &str) -> ExcludeMatcher {
    if !pattern.contains('*') {
        return ExcludeMatcher::Exact(normalize_exclude_exact(pattern));
    }

    if let Some(prefix) =
        pattern.strip_suffix("/*").filter(|prefix| !prefix.contains('*'))
    {
        return ExcludeMatcher::Prefix(normalize_exclude_exact(prefix));
    }

    ExcludeMatcher::Wildcard {
        parts: pattern
            .split('*')
            .filter(|part| !part.is_empty())
            .map(ToOwned::to_owned)
            .collect(),
        starts_anchored: !pattern.starts_with('*'),
        ends_anchored: !pattern.ends_with('*'),
    }
}

fn normalize_exclude_exact(pattern: &str) -> String {
    if pattern == "/" {
        pattern.to_owned()
    } else if pattern.starts_with('/') {
        pattern.trim_end_matches('/').to_owned()
    } else {
        pattern.to_owned()
    }
}
