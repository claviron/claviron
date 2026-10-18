//! Shared forwarding/header policy helpers for proxy-like adapters.

use std::fmt;
use std::net::IpAddr;

use http::header::{
    CONNECTION, CONTENT_LENGTH, FORWARDED, HOST, HeaderName, HeaderValue,
    TRANSFER_ENCODING,
};
use http::uri::Scheme;
use http::{HeaderMap, Version};

use crate::ext::{RealIpExt, request_scheme};

pub const X_FORWARDED_FOR: HeaderName =
    HeaderName::from_static("x-forwarded-for");
pub const X_FORWARDED_HOST: HeaderName =
    HeaderName::from_static("x-forwarded-host");
pub const X_FORWARDED_PROTO: HeaderName =
    HeaderName::from_static("x-forwarded-proto");
pub const VIA: HeaderName = HeaderName::from_static("via");

const HOP_BY_HOP_HEADERS: &[&str] = &[
    "connection",
    "proxy-connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestSmugglingError {
    ContentLengthWithTransferEncoding,
    InvalidContentLength,
    ConflictingContentLength,
}

impl fmt::Display for RequestSmugglingError {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match self {
            Self::ContentLengthWithTransferEncoding => f.write_str(
                "request contains both Content-Length and Transfer-Encoding",
            ),
            Self::InvalidContentLength => {
                f.write_str("request contains invalid Content-Length")
            }
            Self::ConflictingContentLength => f.write_str(
                "request contains conflicting Content-Length values",
            ),
        }
    }
}

impl std::error::Error for RequestSmugglingError {}

#[derive(Clone, Debug, Default)]
pub struct ForwardedRequestContext {
    pub real_ip: Option<IpAddr>,
    pub proto: Option<Scheme>,
}

impl ForwardedRequestContext {
    /// Request-local trusted identity, falling back only to connection facts.
    pub fn from_extensions(extensions: &http::Extensions) -> Self {
        let real_ip = extensions.get::<RealIpExt>().map(|ext| ext.0);
        #[cfg(feature = "server")]
        let real_ip = real_ip.or_else(|| {
            extensions
                .get::<std::sync::Arc<crate::ext::ConnCtx>>()
                .map(|ctx| ctx.real_ip.unwrap_or_else(|| ctx.remote.ip()))
        });
        Self { real_ip, proto: request_scheme(extensions) }
    }

    /// Native transport scheme wins over adapter hints. Direct proxy callers
    /// without a connection may provide a context or an explicit HTTP(S) URI.
    /// An origin-form request without context has no known scheme.
    pub fn from_request<B>(request: &http::Request<B>) -> Self {
        let native = Self::from_extensions(request.extensions());
        let supplied =
            request.extensions().get::<Self>().cloned().unwrap_or_default();
        Self {
            real_ip: supplied.real_ip.or(native.real_ip),
            proto: native.proto.or(supplied.proto).or_else(|| {
                request
                    .uri()
                    .scheme()
                    .filter(|scheme| {
                        **scheme == Scheme::HTTP || **scheme == Scheme::HTTPS
                    })
                    .cloned()
            }),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ForwardedHeaderInfo<'a> {
    pub real_ip: Option<IpAddr>,
    pub proto: Option<&'a str>,
    pub host: Option<HeaderValue>,
    pub via: Option<&'a str>,
}

pub fn is_hop_by_hop_name(name: &str) -> bool {
    HOP_BY_HOP_HEADERS.iter().any(|hop| name.eq_ignore_ascii_case(hop))
}

pub fn strip_hop_by_hop_headers(headers: &mut HeaderMap) {
    // Inspect all Connection values while they remain in the map. Borrowed
    // token matching avoids a temporary Vec and lowercase header allocations.
    loop {
        let named = headers
            .keys()
            .find(|name| {
                **name != CONNECTION
                    && headers
                        .get_all(CONNECTION)
                        .iter()
                        .filter_map(|v| v.to_str().ok())
                        .flat_map(|v| v.split(','))
                        .any(|token| {
                            token.trim().eq_ignore_ascii_case(name.as_str())
                        })
            })
            .cloned();
        let Some(named) = named else { break };
        headers.remove(named);
    }

    for name in HOP_BY_HOP_HEADERS {
        headers.remove(*name);
    }
}

/// Refuse a request whose framing two parsers could read differently, before
/// it is re-serialized onto another connection.
///
/// Placed at the proxy boundary rather than at ingress, because that is where
/// the second parser appears: a request we terminate and hand to PHP has no
/// one left to disagree with us.
///
/// Only what the ingress parser has not already refused is checked, so no
/// rule runs twice. `version` says which parser that was:
///
/// - HTTP/1.x: hyper's server. It answers an invalid `Content-Length`, or
///   several that disagree, with 400, and drops a `Content-Length` sent with
///   `Transfer-Encoding` in either order (hyper #4124, RFC 9112 §6.3).
///   Nothing is left to check. Every HTTP/1.x request in Claviron enters
///   through hyper; Orbit carries the version the edge parsed.
/// - HTTP/2: h2 refuses `Transfer-Encoding` as a connection-specific field,
///   but reads only the first `Content-Length`. Every value is checked here.
/// - HTTP/3 and anything else: the header map arrives as sent. All of it.
pub fn reject_request_smuggling(
    version: Version,
    headers: &HeaderMap,
) -> Result<(), RequestSmugglingError> {
    match version {
        Version::HTTP_09 | Version::HTTP_10 | Version::HTTP_11 => {
            return Ok(());
        }
        Version::HTTP_2 => {}
        _ => {
            if headers.contains_key(CONTENT_LENGTH)
                && headers.contains_key(TRANSFER_ENCODING)
            {
                return Err(
                    RequestSmugglingError::ContentLengthWithTransferEncoding,
                );
            }
        }
    }

    let mut seen = None;
    for value in headers.get_all(CONTENT_LENGTH) {
        let text = value
            .to_str()
            .map_err(|_| RequestSmugglingError::InvalidContentLength)?
            .trim();
        if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
            return Err(RequestSmugglingError::InvalidContentLength);
        }
        let value = text
            .parse::<u64>()
            .map_err(|_| RequestSmugglingError::InvalidContentLength)?;
        if seen.is_some_and(|previous| previous != value) {
            return Err(RequestSmugglingError::ConflictingContentLength);
        }
        seen = Some(value);
    }

    Ok(())
}

pub fn apply_forwarded_headers(
    headers: &mut HeaderMap,
    info: ForwardedHeaderInfo<'_>,
) {
    headers.remove(FORWARDED);
    headers.remove(X_FORWARDED_FOR);
    headers.remove(X_FORWARDED_HOST);
    headers.remove(X_FORWARDED_PROTO);

    if let Some(real_ip) = info.real_ip
        && let Ok(value) = ip_header_value(real_ip)
    {
        headers.insert(X_FORWARDED_FOR, value);
    }

    if let Some(proto) = info.proto
        && let Ok(value) = match proto {
            "http" => Ok(HeaderValue::from_static("http")),
            "https" => Ok(HeaderValue::from_static("https")),
            other => HeaderValue::from_str(other),
        }
    {
        headers.insert(X_FORWARDED_PROTO, value);
    }

    if let Some(host) = info.host {
        headers.insert(X_FORWARDED_HOST, host);
    }

    if let Some(via) = info.via
        && let Ok(value) = if via == "1.1 claviron" {
            Ok(HeaderValue::from_static("1.1 claviron"))
        } else {
            HeaderValue::from_str(via)
        }
    {
        headers.append(VIA, value);
    }
}

/// Replace untrusted forwarding headers with one RFC 7239 element from trusted
/// request facts. IPv6 and Host use quoted-string syntax; unknown facts stay
/// absent. With no facts, only cleanup and optional Via emission occur.
pub fn apply_rfc7239_forwarded_headers(
    headers: &mut HeaderMap,
    info: ForwardedHeaderInfo<'_>,
) {
    use std::fmt::Write;
    headers.remove(FORWARDED);
    loop {
        let name = headers
            .keys()
            .find(|name| name.as_str().starts_with("x-forwarded-"))
            .cloned();
        let Some(name) = name else { break };
        headers.remove(name);
    }
    let host = info.host.as_ref().and_then(|host| host.to_str().ok());
    // Worst-case escaped Host plus the IP/proto parameter syntax bounds this
    // request-local allocation; it imposes no operator body/header limit.
    let mut text = String::with_capacity(
        host.map_or(0, |host| {
            host.len().saturating_mul(2) + "host=\"\";".len()
        }) + info.proto.map_or(0, |proto| proto.len() + "proto=;".len())
            + info.real_ip.map_or(0, |ip| match ip {
                IpAddr::V4(_) => "for=255.255.255.255;".len(),
                IpAddr::V6(_) => {
                    "for=\"[ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff]\";".len()
                }
            }),
    );
    if let Some(ip) = info.real_ip {
        match ip {
            IpAddr::V4(ip) => write!(&mut text, "for={ip}").unwrap(),
            IpAddr::V6(ip) => write!(&mut text, "for=\"[{ip}]\"").unwrap(),
        }
    }
    if let Some(proto) =
        info.proto.filter(|proto| matches!(*proto, "http" | "https"))
    {
        if !text.is_empty() {
            text.push(';');
        }
        write!(&mut text, "proto={proto}").unwrap();
    }
    if let Some(host) = host {
        if !text.is_empty() {
            text.push(';');
        }
        text.push_str("host=\"");
        for ch in host.chars() {
            if matches!(ch, '\\' | '"') {
                text.push('\\');
            }
            text.push(ch);
        }
        text.push('"');
    }
    if !text.is_empty()
        && let Ok(value) = HeaderValue::from_str(&text)
    {
        headers.insert(FORWARDED, value);
    }
    if let Some(via) = info.via
        && let Ok(value) = if via == "1.1 claviron" {
            Ok(HeaderValue::from_static("1.1 claviron"))
        } else {
            HeaderValue::from_str(via)
        }
    {
        headers.append(VIA, value);
    }
}

fn ip_header_value(
    ip: IpAddr
) -> Result<HeaderValue, http::header::InvalidHeaderValue> {
    use std::fmt::Write;
    // 64 bytes covers every IPv4/IPv6 address representation; overflow must
    // fail rather than truncate a trusted client address.
    struct Text {
        bytes: [u8; 64],
        len: usize,
    }
    impl std::fmt::Write for Text {
        fn write_str(
            &mut self,
            s: &str,
        ) -> std::fmt::Result {
            let end = self.len + s.len();
            if end > self.bytes.len() {
                return Err(std::fmt::Error);
            }
            self.bytes[self.len..end].copy_from_slice(s.as_bytes());
            self.len = end;
            Ok(())
        }
    }
    let mut text = Text { bytes: [0; 64], len: 0 };
    write!(&mut text, "{ip}").expect("IP address fits its fixed text buffer");
    HeaderValue::from_bytes(&text.bytes[..text.len])
}

pub fn original_host_value(headers: &HeaderMap) -> Option<HeaderValue> {
    headers.get(HOST).cloned()
}

#[cfg(test)]
#[path = "../tests/forwarding.rs"]
mod tests;
