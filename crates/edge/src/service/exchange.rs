//! Immutable HTTP policy outside the application routing boundary.
//!
//! Select declared prefix scope once, then admit CORS and shape the response
//! around any owner only when the operator declared a policy.

use crate::config::HostConfig;
use claviron_http::{
    RouteResponse,
    config::{cors::CorsConfig, host::HostHeadersConfig, routes::RouteTable},
    ext::PathPrefix,
};
use http::{HeaderMap, HeaderValue, Method};
use std::collections::HashMap;
use std::sync::Arc;

pub struct ExchangePolicy {
    routes: RouteTable<Arc<ExchangeRules>>,
    fallback: Arc<ExchangeRules>,
}

pub(crate) struct ExchangeRules {
    cors: Option<CorsConfig>,
    set: Box<[(http::HeaderName, HeaderValue)]>,
    unset: Box<[http::HeaderName]>,
}

impl ExchangePolicy {
    pub(crate) fn compile(
        config: &HostConfig,
        defaults: Option<&HostHeadersConfig>,
    ) -> Option<Arc<Self>> {
        let fallback = config.fallback().map(Arc::as_ref).unwrap_or(config);
        // Server identity is already stamped by HostApp, without a policy
        // selection. Only other global rewrites need exchange processing.
        if defaults.is_none_or(|headers| {
            headers.set.is_empty() && headers.unset.is_empty()
        }) && fallback.headers.is_empty()
            && fallback.cors.is_none()
            && config.routes.iter().all(|(_, route)| {
                route.headers.is_empty() && route.cors.is_none()
            })
        {
            return None;
        }
        let fallback = Arc::new(ExchangeRules::compile(
            &fallback.headers,
            fallback.cors.as_ref(),
            defaults,
        ));
        let mut routes = config
            .routes
            .iter()
            .map(|(prefix, route)| {
                (
                    prefix.clone(),
                    Arc::new(ExchangeRules::compile(
                        &route.headers,
                        route.cors.as_ref(),
                        defaults,
                    )),
                )
            })
            .collect::<Vec<(PathPrefix, Arc<ExchangeRules>)>>();
        // Uniform host policy needs no prefix lookup. Keep the complete table
        // only when scopes differ, including barriers restoring root policy.
        if routes.iter().all(|(_, rules)| rules.same_as(&fallback)) {
            routes.clear();
        }
        Some(Arc::new(Self { routes: RouteTable::new(routes), fallback }))
    }

    pub(crate) fn select(
        &self,
        path: &str,
    ) -> Option<&ExchangeRules> {
        let rules = self.routes.find(path).unwrap_or(&self.fallback);
        if rules.is_empty() { None } else { Some(rules) }
    }
}

impl ExchangeRules {
    fn is_empty(&self) -> bool {
        self.cors.is_none() && self.set.is_empty() && self.unset.is_empty()
    }
    fn same_as(
        &self,
        other: &Self,
    ) -> bool {
        self.set == other.set
            && self.unset == other.unset
            && match (&self.cors, &other.cors) {
                (None, None) => true,
                (Some(a), Some(b)) => {
                    a.policy == b.policy
                        && a.credentials == b.credentials
                        && a.origins == b.origins
                }
                _ => false,
            }
    }
    fn compile(
        headers: &HostHeadersConfig,
        cors: Option<&CorsConfig>,
        defaults: Option<&HostHeadersConfig>,
    ) -> Self {
        // Collapse global and host actions at snapshot construction. A host
        // set revives a globally unset header, and a host unset removes a
        // global set. Each response executes only the final actions.
        let mut actions = HashMap::new();
        if let Some(defaults) = defaults {
            for name in &defaults.unset {
                actions.insert(name.clone(), None);
            }
            for (name, value) in &defaults.set {
                actions.insert(name.clone(), Some(value.clone()));
            }
        }
        if let Some(server) = &headers.server {
            actions.insert(
                http::header::SERVER,
                Some(
                    HeaderValue::from_str(server)
                        .expect("valid Server header value"),
                ),
            );
        }
        for name in &headers.unset {
            actions.insert(name.clone(), None);
        }
        for (name, value) in &headers.set {
            actions.insert(name.clone(), Some(value.clone()));
        }
        let mut set = Vec::new();
        let mut unset = Vec::new();
        for (name, value) in actions {
            match value {
                Some(value) => set.push((name, value)),
                None => unset.push(name),
            }
        }
        set.sort_unstable_by(|(a, _), (b, _)| a.as_str().cmp(b.as_str()));
        unset.sort_unstable_by(|a, b| a.as_str().cmp(b.as_str()));
        Self {
            cors: cors.cloned(),
            set: set.into_boxed_slice(),
            unset: unset.into_boxed_slice(),
        }
    }

    pub(crate) fn check(
        &self,
        method: &Method,
        headers: &HeaderMap,
    ) -> (Option<RouteResponse>, Option<HeaderValue>) {
        self.cors
            .as_ref()
            .map(|cors| super::cors::check(cors, method, headers))
            .unwrap_or((None, None))
    }

    pub(crate) fn finish(
        &self,
        mut response: RouteResponse,
        origin: Option<&HeaderValue>,
    ) -> RouteResponse {
        let headers = response.headers_mut();
        for name in &self.unset {
            headers.remove(name);
        }
        for (name, value) in &self.set {
            headers.insert(name.clone(), value.clone());
        }
        if let Some(cors) = &self.cors {
            super::cors::finish(cors, origin, headers);
        }
        response
    }
}

#[cfg(test)]
#[path = "../../tests/service/exchange.rs"]
mod tests;
