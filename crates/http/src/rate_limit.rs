//! Synchronous, body-independent rate admission over a prepared quota store.

use std::net::IpAddr;
use std::num::NonZeroU64;

use claviron_orbit::quota::{Decision, Key, Quota, QuotaStore};
use http::{Extensions, HeaderMap, Method, Request, Uri};

/// Already parsed request facts. Custom providers choose their own identity;
/// they must not trust forwarding headers in place of the accepted IP context.
pub struct RequestHead<'a> {
    pub method: &'a Method,
    pub uri: &'a Uri,
    pub headers: &'a HeaderMap,
    pub extensions: &'a Extensions,
}

impl<'a> RequestHead<'a> {
    pub fn new<B>(request: &'a Request<B>) -> Self {
        Self {
            method: request.method(),
            uri: request.uri(),
            headers: request.headers(),
            extensions: request.extensions(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KeyError {
    Missing,
    Invalid,
}

/// One instance belongs to one limiter. No global provider or identity enum.
/// Produce a bounded opaque subject, not a raw credential. Implementations own
/// their CPU/allocation bounds; this callback performs no async authentication.
pub trait KeyProvider: Send + Sync {
    type Key: AsRef<[u8]>;
    fn max_key_bytes(&self) -> usize;
    fn key(
        &self,
        request: &RequestHead<'_>,
    ) -> Result<Self::Key, KeyError>;
}

/// Binary, allocation-free accepted IP identity. IPv4-mapped IPv6 shares IPv4's key.
#[derive(Clone, Copy, Default)]
pub struct IpKey;

impl KeyProvider for IpKey {
    type Key = [u8; 17];
    fn max_key_bytes(&self) -> usize {
        17
    }
    fn key(
        &self,
        request: &RequestHead<'_>,
    ) -> Result<Self::Key, KeyError> {
        let ip = request
            .extensions
            .get::<crate::ext::RealIpExt>()
            .ok_or(KeyError::Missing)?
            .0;
        let ip = match ip {
            IpAddr::V6(ip) => {
                ip.to_ipv4_mapped().map(IpAddr::V4).unwrap_or(IpAddr::V6(ip))
            }
            ip => ip,
        };
        let mut key = [0; 17];
        match ip {
            IpAddr::V4(ip) => {
                key[0] = 4;
                key[1..5].copy_from_slice(&ip.octets());
            }
            IpAddr::V6(ip) => {
                key[0] = 6;
                key[1..].copy_from_slice(&ip.octets());
            }
        }
        Ok(key)
    }
}

#[derive(Debug)]
pub enum Error {
    InvalidDefinition,
    Key(KeyError),
    Store(claviron_orbit::quota::Error),
}

impl std::fmt::Display for Error {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        match self {
            Self::InvalidDefinition => f.write_str(
                "rate limit namespace, key bound or cost cannot fit its store",
            ),
            Self::Key(error) => {
                write!(f, "rate limit key unavailable: {error:?}")
            }
            Self::Store(error) => write!(f, "rate limit store: {error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Store(error) => Some(error),
            _ => None,
        }
    }
}

/// Prepared store, key provider and namespace. Clone shares the same budget;
/// constructing a separate Quota creates a separate local budget. No body is read.
#[derive(Clone)]
pub struct RateLimiter<K> {
    store: Quota,
    keys: K,
    namespace: Box<[u8]>,
    key_bytes: usize,
    cost: NonZeroU64,
}

impl<K: KeyProvider> RateLimiter<K> {
    pub fn new(
        store: Quota,
        keys: K,
        namespace: impl Into<Box<[u8]>>,
        cost: NonZeroU64,
    ) -> Result<Self, Error> {
        let namespace = namespace.into();
        let key_bytes = keys.max_key_bytes();
        if namespace.is_empty()
            || key_bytes == 0
            || namespace
                .len()
                .checked_add(key_bytes)
                .is_none_or(|len| len > store.geometry().key_bytes().get())
            || cost > store.policy().burst()
        {
            return Err(Error::InvalidDefinition);
        }
        Ok(Self { store, keys, namespace, key_bytes, cost })
    }

    pub fn store(&self) -> &Quota {
        &self.store
    }

    pub fn check(
        &self,
        request: &RequestHead<'_>,
    ) -> Result<Decision, Error> {
        let subject = self.keys.key(request).map_err(Error::Key)?;
        let subject = subject.as_ref();
        if subject.is_empty() || subject.len() > self.key_bytes {
            return Err(Error::Key(KeyError::Invalid));
        }
        let key = Key::new(&self.namespace, subject).map_err(Error::Store)?;
        self.store.try_consume(key, self.cost).map_err(Error::Store)
    }
}

/// Round up, never advertise an earlier retry than the quota permits.
pub fn retry_after_seconds(duration: std::time::Duration) -> u64 {
    duration.as_secs().saturating_add(u64::from(duration.subsec_nanos() != 0))
}

/// Empty protocol reply, independent of Axum and application response formats.
/// Busy/full are overload, not evidence that a subject exhausted its allowance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Rejection {
    status: http::StatusCode,
    retry_after: Option<u64>,
}

impl Rejection {
    pub fn from_result(result: Result<Decision, Error>) -> Result<(), Self> {
        use claviron_orbit::quota::Error as StoreError;
        use http::StatusCode;
        let (status, retry_after) = match result {
            Ok(Decision::Allowed) => return Ok(()),
            Ok(Decision::Limited { retry_after }) => (
                StatusCode::TOO_MANY_REQUESTS,
                Some(retry_after_seconds(retry_after)),
            ),
            Err(Error::Store(StoreError::Busy | StoreError::CapacityFull)) => {
                (StatusCode::SERVICE_UNAVAILABLE, None)
            }
            Err(Error::Key(KeyError::Invalid)) => {
                (StatusCode::BAD_REQUEST, None)
            }
            Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, None),
        };
        Err(Self { status, retry_after })
    }

    pub fn status(self) -> http::StatusCode {
        self.status
    }

    pub fn into_response(self) -> http::Response<()> {
        let mut response = http::Response::new(());
        *response.status_mut() = self.status;
        if let Some(seconds) = self.retry_after {
            response
                .headers_mut()
                .insert(http::header::RETRY_AFTER, seconds.into());
        }
        response
    }
}

#[cfg(test)]
#[path = "../tests/rate_limit.rs"]
mod tests;
