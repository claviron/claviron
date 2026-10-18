use std::time::{Duration, SystemTime};

use claviron_http::config::caching::CompressionConfig;
// Keep historic crate-local re-exports so existing
// `crate::caching::policy::{RulesConfig, CachingConfig, ...}` imports still
// resolve. Canonical home moved to `claviron-host-types::policy`.
pub use claviron_http::config::caching::{
    CachingConfig, CompressionAlgorithm, RulesConfig,
};
use claviron_http::config::statics::StaticConfig;
use http::HeaderValue;
use http::header::{CACHE_CONTROL, EXPIRES};

#[derive(Debug)]
pub(crate) struct CachePolicy {
    pub client_ttl_secs: Option<u64>,
    pub server_ttl_secs: Option<u64>,
    pub validate_timestamps: bool,
    pub immutable: bool,
    pub compression: Option<CompressionConfig>,
}

impl CachePolicy {
    pub fn spa(caching: Option<&CachingConfig>) -> Self {
        match caching {
            Some(config) => Self {
                client_ttl_secs: config.client,
                server_ttl_secs: config.server,
                validate_timestamps: config.validate,
                immutable: config.immutable,
                compression: config.compression.clone(),
            },
            None => Self::dynamic_server_cache(),
        }
    }

    pub fn default_server_cache() -> Self {
        Self {
            client_ttl_secs: None,
            server_ttl_secs: Some(3600),
            validate_timestamps: true,
            immutable: false,
            compression: None,
        }
    }

    pub fn dynamic_server_cache() -> Self {
        Self {
            client_ttl_secs: Some(0),
            server_ttl_secs: Some(3600),
            validate_timestamps: true,
            immutable: false,
            compression: None,
        }
    }

    pub fn no_cache() -> Self {
        Self {
            client_ttl_secs: Some(0),
            server_ttl_secs: None,
            validate_timestamps: false,
            immutable: false,
            compression: None,
        }
    }
}

impl Default for CachePolicy {
    fn default() -> Self {
        Self {
            client_ttl_secs: None,
            server_ttl_secs: None,
            validate_timestamps: true,
            immutable: false,
            compression: None,
        }
    }
}

pub(crate) fn compose_static_policy(
    cfg: &StaticConfig,
    rel_path: &str,
) -> CachePolicy {
    let mut out = CachePolicy::default();
    let rules = match &cfg.rules {
        Some(v) => v,
        None => return out,
    };

    for rule in rules {
        if rule.test.is_match(rel_path) {
            if let Some(c) = &rule.caching {
                out.client_ttl_secs = c.client;
                out.server_ttl_secs = c.server;
                out.validate_timestamps = c.validate;
                out.immutable = c.immutable;
                out.compression = c.compression.clone();
            }
            return out;
        }
    }
    out
}

pub(crate) fn apply_client_cache_headers(
    headers: &mut http::HeaderMap,
    policy: &CachePolicy,
) {
    match policy.client_ttl_secs {
        Some(0) => {
            headers.insert(
                CACHE_CONTROL,
                HeaderValue::from_static("no-store, no-cache, must-revalidate"),
            );
            headers.insert(
                http::header::PRAGMA,
                HeaderValue::from_static("no-cache"),
            );
            let past = SystemTime::UNIX_EPOCH;
            let expires_str = httpdate::fmt_http_date(past);
            headers
                .insert(EXPIRES, HeaderValue::from_str(&expires_str).unwrap());
        }
        Some(secs) => {
            let cache_control = if policy.immutable {
                format!("public, max-age={}, immutable", secs)
            } else {
                format!("public, max-age={}", secs)
            };
            let _ = headers.insert(
                CACHE_CONTROL,
                HeaderValue::from_str(&cache_control).unwrap(),
            );
            let expires_at = SystemTime::now()
                .checked_add(Duration::from_secs(secs))
                .unwrap_or(SystemTime::now());
            let expires_str = httpdate::fmt_http_date(expires_at);
            let _ = headers
                .insert(EXPIRES, HeaderValue::from_str(&expires_str).unwrap());
        }
        None => {}
    }
}
