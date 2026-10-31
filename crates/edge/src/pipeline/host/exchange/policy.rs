//! Immutable HTTP policy outside the application routing boundary.
//!
//! Select declared prefix scope once, then admit CORS and shape the response
//! around any owner only when the operator declared a policy.

use crate::config::HostConfig;
use crate::pipeline::host::{ExchangeLayer, ExchangeState, HostPipeline};
use claviron_http::{
    config::{cors::CorsConfig, host::HostHeadersConfig, routes::RouteTable},
    ext::PathPrefix,
};
use http::HeaderValue;
use std::collections::HashMap;
use std::sync::Arc;

pub struct ExchangePolicy {
    routes: RouteTable<Arc<ExchangeRules>>,
    fallback: Arc<ExchangeRules>,
}

pub(crate) struct ExchangeRules {
    layer: ExchangeLayer,
    pipeline: Option<HostPipeline<ExchangeState>>,
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
    ) -> Option<&HostPipeline<ExchangeState>> {
        let rules = self.routes.find(path).unwrap_or(&self.fallback);
        rules.pipeline.as_ref()
    }
}

impl ExchangeRules {
    fn same_as(
        &self,
        other: &Self,
    ) -> bool {
        self.layer.same_as(&other.layer)
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
        let layer = ExchangeLayer::new(cors.cloned(), set, unset);
        let pipeline =
            (!layer.is_empty()).then(|| layer.clone().into_pipeline());
        Self { layer, pipeline }
    }
}

#[cfg(test)]
#[path = "../../../../tests/pipeline/host/exchange/policy.rs"]
mod tests;
