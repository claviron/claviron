use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[cfg(feature = "server")]
use crate::ext::ConnCtx;
use crate::ext::RealIpExt;
use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use http::StatusCode;
use moka::ops::compute::{CompResult, Op};
use moka::sync::Cache;

use crate::config::rate_limit::RateLimitConfig;

/// Per-IP fixed-window counter with automatic TTL-based eviction
/// This improved version uses moka cache to prevent unbounded memory growth
#[derive(Clone)]
pub struct RateLimiter {
    window: Duration,
    max: u64,
    // key = IpAddr; value = (window_start, count)
    // Using moka Cache with automatic eviction after 2x window duration
    store: Arc<Cache<IpAddr, (Instant, u64)>>,
}

impl RateLimiter {
    pub fn new(
        window: Duration,
        max: u64,
    ) -> Self {
        // Configure cache with TTL = 2x the rate limit window
        // This ensures old entries are automatically evicted
        let ttl = window.mul_f32(2.0);

        let cache = Cache::builder()
            .time_to_idle(ttl) // Evict after 2x window of inactivity
            .max_capacity(100_000) // Hard limit: 100k IPs max (~2.4MB)
            .build();

        Self { window, max, store: Arc::new(cache) }
    }

    /// Build a RateLimitLayer from parsed config
    pub fn from_config(cfg: &RateLimitConfig) -> Self {
        Self::new(cfg.duration, cfg.max_attempts)
    }

    fn hit(
        &self,
        ip: IpAddr,
    ) -> (Instant, u64) {
        // Moka serializes computation for one key, not unrelated clients.
        let result = self.store.entry(ip).and_compute_with(|current| {
            let now = Instant::now();
            let value = match current.map(|entry| entry.into_value()) {
                Some((start, count))
                    if now.duration_since(start) < self.window =>
                {
                    (start, count.saturating_add(1))
                }
                _ => (now, 1),
            };
            Op::Put(value)
        });
        match result {
            CompResult::Inserted(entry) | CompResult::ReplacedWith(entry) => {
                entry.into_value()
            }
            _ => unreachable!("rate limit computation always stores a counter"),
        }
    }
}

// Axum-friendly middleware fn
pub async fn rate_limit_mw(
    State(layer): State<RateLimiter>,
    mut req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    // Prefer your normalized Extension<RealIp>, else socket peer if you inject it
    let ip = req
        .extensions()
        .get::<RealIpExt>()
        .map(|x| x.0)
        .or_else(|| {
            #[cfg(feature = "server")]
            {
                req.extensions()
                    .get::<Arc<ConnCtx>>()
                    .map(|c| c.real_ip.unwrap_or_else(|| c.remote.ip()))
                    .or_else(|| {
                        req.extensions()
                            .get::<ConnCtx>()
                            .map(|c| c.real_ip.unwrap_or_else(|| c.remote.ip()))
                    })
            }
            #[cfg(not(feature = "server"))]
            {
                None
            }
        })
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?; // no IP in context = setup bug

    let (window_start, count) = layer.hit(ip);
    let now = Instant::now();

    if count > layer.max {
        // optional: set Retry-After to hint clients
        let reset_after =
            (window_start + layer.window).saturating_duration_since(now);
        let mut resp = (
            StatusCode::TOO_MANY_REQUESTS,
            "Too many requests, please try again later.",
        )
            .into_response();

        resp.headers_mut().insert(
            http::header::RETRY_AFTER,
            http::HeaderValue::from(reset_after.as_secs()),
        );

        return Ok(resp);
    }

    // (optional) surface counters to handlers
    req.extensions_mut().insert(count);

    let mut res = next.run(req).await;

    let remaining = layer.max.saturating_sub(count);
    let reset_after =
        (window_start + layer.window).saturating_duration_since(now);

    res.headers_mut()
        .insert("X-RateLimit-Limit", http::HeaderValue::from(layer.max));
    res.headers_mut()
        .insert("X-RateLimit-Remaining", http::HeaderValue::from(remaining));
    res.headers_mut().insert(
        "X-RateLimit-Reset",
        http::HeaderValue::from(reset_after.as_secs()),
    );

    Ok(res)
}

#[cfg(test)]
#[path = "../../tests/layers/rate_limit.rs"]
mod tests;
