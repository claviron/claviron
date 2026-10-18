//! Cold HTTP probe request and streaming substring plan.
use std::sync::Arc;

use http::{Method, Uri};

use crate::config::error::ConfigError;
use crate::config::profile::UpstreamProtocol;
use crate::config::profile::health::l7::L7Config;
use crate::origin::Origin;
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Prepared {
    pub(crate) method: Method,
    pub(crate) uris: Arc<[Uri]>,
    pub(crate) needle: Vec<u8>,
    pub(crate) prefix: Vec<usize>,
}
impl Prepared {
    pub(crate) fn compile(
        name: &str,
        policy: &L7Config,
        protocol: UpstreamProtocol,
        origins: &[Origin],
    ) -> Result<Option<Arc<Self>>, ConfigError> {
        if !policy.enabled {
            return Ok(None);
        }
        let invalid = |field, detail| ConfigError::InvalidHealth {
            profile: name.into(),
            field,
            detail,
        };
        let method = policy
            .method
            .parse()
            .map_err(|_| invalid("l7.method", "invalid HTTP method"))?;
        let path: http::uri::PathAndQuery = policy
            .path
            .parse()
            .map_err(|_| invalid("l7.path", "invalid origin-form path"))?;
        if !policy.path.starts_with('/') || policy.path.starts_with("//") {
            return Err(invalid("l7.path", "must be an origin-form path"));
        }
        let mut uris = Vec::with_capacity(origins.len());
        for origin in origins {
            let mut parts = http::uri::Parts::default();
            parts.path_and_query = Some(path.clone());
            if protocol == UpstreamProtocol::H2 {
                let scheme = http::uri::Scheme::HTTP;
                let scheme = if matches!(
                    origin.connect,
                    crate::origin::ConnectTarget::Tls(_)
                ) {
                    http::uri::Scheme::HTTPS
                } else {
                    scheme
                };
                parts.scheme = Some(scheme);
                parts.authority = Some(origin.authority.clone());
            }
            uris.push(
                Uri::from_parts(parts)
                    .map_err(|_| invalid("l7.path", "invalid probe URI"))?,
            );
        }
        let needle = policy
            .expect
            .body_contains
            .as_deref()
            .unwrap_or("")
            .as_bytes()
            .to_vec();
        let mut prefix = vec![0; needle.len()];
        for i in 1..needle.len() {
            let mut j = prefix[i - 1];
            while j > 0 && needle[i] != needle[j] {
                j = prefix[j - 1];
            }
            if needle[i] == needle[j] {
                j += 1;
            }
            prefix[i] = j;
        }
        Ok(Some(Arc::new(Self { method, uris: uris.into(), needle, prefix })))
    }
}
