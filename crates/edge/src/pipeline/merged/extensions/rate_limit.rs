//! Opt-in per-IP rate limits prepared once for each canonical host and prefix.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::{RouteContext, RouteEffect, RouteExtension, RouteLayer, directive};
use crate::{Host, HostConfig};
use axum::{Router, middleware};
use claviron_http::config::rate_limit::RateLimitConfig;
use claviron_http::ext::PathPrefix;
use claviron_http::layers::{RateLimiter, rate_limit_mw};
use claviron_runtime::registry;

/// Worker-local IP counters shared by aliases and unchanged route snapshots.
///
/// Register explicitly to enable `rate_limit: {duration, max_attempts}` on
/// selected routes. Both values are required and positive; registration alone
/// adds no limit, inherited policy, listener setting or fleet-wide quota.
#[derive(Default)]
pub struct RateLimit {
    limits: Arc<Mutex<HashMap<(Host, PathPrefix), Prepared>>>,
}

struct Prepared {
    config: RateLimitConfig,
    limiter: RateLimiter,
}

struct Layer(RateLimiter);

impl RouteLayer for Layer {
    fn apply(
        &self,
        router: Router,
    ) -> Router {
        router.layer(middleware::from_fn_with_state(
            self.0.clone(),
            rate_limit_mw,
        ))
    }
}

impl RouteExtension<HostConfig> for RateLimit {
    fn name(&self) -> &'static str {
        "rate_limit"
    }

    fn resolve(
        &self,
        context: &RouteContext<'_>,
        source: &serde_yaml::Value,
    ) -> registry::Result<RouteEffect> {
        let limits = self.limits.clone();
        directive::<RateLimitConfig, _>(self.name(), move |context, config| {
            if config.duration.is_zero() || config.max_attempts == 0 {
                return Err(registry::Error::msg(format!(
                    "route '{}' rate_limit requires positive duration and max_attempts",
                    context.prefix,
                )));
            }
            let mut limits =
                limits.lock().expect("route rate limit registry lock poisoned");
            let prepared = limits
                .entry((context.host.clone(), context.prefix.clone()))
                .or_insert_with(|| Prepared {
                    limiter: RateLimiter::from_config(&config),
                    config: config.clone(),
                });
            if prepared.config.duration != config.duration
                || prepared.config.max_attempts != config.max_attempts
            {
                *prepared = Prepared {
                    limiter: RateLimiter::from_config(&config),
                    config,
                };
            }
            Ok(RouteEffect::Layer(Arc::new(Layer(prepared.limiter.clone()))))
        })
        .resolve(context, source)
    }
}

#[cfg(test)]
#[path = "../../../../tests/extensions/rate_limit.rs"]
mod tests;
