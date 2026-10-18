//! Borrow only normalized request facts; never retain a body or copy key bytes.
use std::net::IpAddr;

use http::{HeaderMap, Request};
#[derive(Clone, Copy)]
pub(crate) struct RequestKeyView<'a> {
    pub(crate) headers: &'a HeaderMap,
    pub(crate) path: &'a str,
    pub(crate) remote_ip: Option<IpAddr>,
}
impl<'a> RequestKeyView<'a> {
    pub(crate) fn new<B>(
        request: &'a Request<B>,
        remote_ip: Option<IpAddr>,
    ) -> Self {
        Self {
            headers: request.headers(),
            path: request.uri().path(),
            remote_ip,
        }
    }
    fn cookie(
        &self,
        name: &str,
    ) -> Option<&[u8]> {
        self.headers
            .get_all(http::header::COOKIE)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .flat_map(|v| v.split(';'))
            .filter_map(|pair| pair.trim().split_once('='))
            .find_map(|(key, value)| (key == name).then_some(value.as_bytes()))
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Key {
    Header(http::HeaderName),
    Cookie(Box<str>),
    Path,
    RemoteIp,
}
impl Key {
    pub(super) fn compile(value: &str) -> Option<Self> {
        if let Some(name) = value.strip_prefix("header:") {
            return name.parse().ok().map(Self::Header);
        }
        if let Some(name) = value.strip_prefix("cookie:") {
            return (!name.is_empty()
                && name.bytes().all(|b| {
                    b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b)
                }))
            .then(|| Self::Cookie(name.into()));
        }
        match value {
            "path" => Some(Self::Path),
            "remote_ip" => Some(Self::RemoteIp),
            _ => None,
        }
    }
    pub(super) fn hash(
        &self,
        request: &RequestKeyView<'_>,
    ) -> u64 {
        match self {
            Self::Header(name) => {
                hash(request.headers.get(name).map_or(b"", |v| v.as_bytes()))
            }
            Self::Cookie(name) => hash(request.cookie(name).unwrap_or(b"")),
            Self::Path => hash(request.path.as_bytes()),
            Self::RemoteIp => match request.remote_ip {
                Some(IpAddr::V4(ip)) => hash(&ip.octets()),
                Some(IpAddr::V6(ip)) => hash(&ip.octets()),
                None => hash(b""),
            },
        }
    }
}
// Fixed FNV-1a wire-independent hashing makes separately compiled peer rings
// agree. A random/default process hasher or configurable seed breaks affinity.
const BASIS: u64 = 0xcbf29ce484222325;
// FNV-1a's specified multiplier, not an operational tuning setting.
const PRIME: u64 = 0x100000001b3;
pub(super) fn hash(bytes: &[u8]) -> u64 {
    finish(extend(BASIS, bytes))
}
pub(super) fn extend(
    mut hash: u64,
    bytes: &[u8],
) -> u64 {
    for byte in bytes {
        hash = (hash ^ u64::from(*byte)).wrapping_mul(PRIME);
    }
    hash
}

// Fixed avalanche spreads related labels/replica ordinals across the full ring.
// These multipliers/shifts define peer affinity, never operator timing/policy.
pub(super) fn finish(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
    value ^ (value >> 31)
}
