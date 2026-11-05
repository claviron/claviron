//! Request authority validation, independent of transport and virtual hosting.

use claviron_types::Host;
use http::uri::{Authority, Scheme};
use http::{
    HeaderMap, HeaderValue, Request, Uri, Version, header, request::Parts,
};

/// The selected wire authority and its normalized host identity.
///
/// Validation never rewrites the URI or headers. TLS/SNI binding and virtual
/// host selection are separate policies owned by the caller.
#[derive(Clone, Debug)]
pub struct RequestAuthority {
    authority: Authority,
    host: Host,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorityError {
    Missing,
    Invalid,
    Conflicting,
    WrongScheme,
}

impl std::fmt::Display for AuthorityError {
    fn fmt(
        &self,
        formatter: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Missing => "request authority is missing",
            Self::Invalid => "request authority is invalid",
            Self::Conflicting => "request authority fields conflict",
            Self::WrongScheme => "request authority uses an unsupported scheme",
        })
    }
}

impl std::error::Error for AuthorityError {}

impl RequestAuthority {
    fn parse(raw: &str) -> Result<Self, AuthorityError> {
        let authority: Authority =
            raw.parse().map_err(|_| AuthorityError::Invalid)?;
        if raw.contains('@') || raw.bytes().any(|c| c.is_ascii_whitespace()) {
            return Err(AuthorityError::Invalid);
        }
        let raw_host = authority.host();
        // Authority's numeric port conversion also accepts a leading '+'.
        // URI ports contain digits only; an explicit empty port is rejected
        // by this origin server even though the generic URI grammar allows it.
        if raw != raw_host {
            let port = raw
                .strip_prefix(raw_host)
                .and_then(|suffix| suffix.strip_prefix(':'))
                .ok_or(AuthorityError::Invalid)?;
            if port.is_empty()
                || !port.bytes().all(|byte| byte.is_ascii_digit())
                || authority.port_u16().is_none()
            {
                return Err(AuthorityError::Invalid);
            }
        }
        // Host also parses configuration keys (wildcards) and tolerates repeated
        // trailing dots. A request needs a concrete identity: only one DNS root
        // dot is accepted, and square brackets are reserved for IPv6 literals.
        if raw_host.contains('*') || raw_host.ends_with("..") {
            return Err(AuthorityError::Invalid);
        }
        let host = raw_host.parse().map_err(|_| AuthorityError::Invalid)?;
        if raw_host.starts_with('[')
            && !matches!(host, Host::Ip(std::net::IpAddr::V6(_)))
        {
            return Err(AuthorityError::Invalid);
        }
        Ok(Self { authority, host })
    }

    /// Validate the authority fields using the request's native HTTP version.
    ///
    /// HTTP/1.1 always requires one valid Host field. An HTTP/1 target that
    /// carries its own authority takes precedence over that field, including
    /// absolute-form and CONNECT authority-form targets. HTTP/2 and HTTP/3
    /// permit Host fallback, but reject conflicting Host and URI authorities.
    /// Only HTTP/1.0 may return `None`; its connection-bound default is a caller
    /// policy, not a fabricated authority. Native drivers validate request
    /// target framing, including classic versus extended CONNECT.
    ///
    /// This origin-server contract rejects explicit empty Host fields and
    /// accepts concrete DNS names, bare names and IP literals. It deliberately
    /// does not support every generic URI reg-name or authority-less scheme;
    /// an empty Host can be legal outside this HTTP(S) origin-server model.
    pub fn from_request<B>(
        request: &Request<B>
    ) -> Result<Option<Self>, AuthorityError> {
        Self::from_fields(request.version(), request.uri(), request.headers())
    }

    /// Validate an owned request head without reconstructing or cloning it.
    /// Uses exactly the same authority rules as [`Self::from_request`].
    pub fn from_parts(parts: &Parts) -> Result<Option<Self>, AuthorityError> {
        Self::from_fields(parts.version, &parts.uri, &parts.headers)
    }

    fn from_fields(
        version: Version,
        uri: &Uri,
        headers: &HeaderMap,
    ) -> Result<Option<Self>, AuthorityError> {
        if !matches!(
            version,
            Version::HTTP_10
                | Version::HTTP_11
                | Version::HTTP_2
                | Version::HTTP_3
        ) {
            return Err(AuthorityError::Invalid);
        }

        let mut values = headers.get_all(header::HOST).iter();
        let first = values.next();
        if values.next().is_some() {
            return Err(AuthorityError::Conflicting);
        }
        let host = first
            .map(|value| {
                Self::parse(
                    value.to_str().map_err(|_| AuthorityError::Invalid)?,
                )
            })
            .transpose()?;
        if version == Version::HTTP_11 && host.is_none() {
            return Err(AuthorityError::Missing);
        }
        let target = uri
            .authority()
            .map(|authority| Self::parse(authority.as_str()))
            .transpose()?;

        if matches!(version, Version::HTTP_2 | Version::HTTP_3)
            && let (Some(target), Some(host)) = (&target, &host)
            && !target.equivalent(host, uri.scheme())
        {
            return Err(AuthorityError::Conflicting);
        }

        match target.or(host) {
            Some(authority) => Ok(Some(authority)),
            None if version == Version::HTTP_10 => Ok(None),
            None => Err(AuthorityError::Missing),
        }
    }

    /// Select a cleartext host without manufacturing a missing authority.
    /// An absolute-form target must use HTTP; the listener's bind port is not
    /// substituted for the request's public port.
    pub fn from_cleartext<B>(
        request: &Request<B>
    ) -> Result<Self, AuthorityError> {
        if request.uri().scheme().is_some_and(|scheme| *scheme != Scheme::HTTP)
        {
            return Err(AuthorityError::WrongScheme);
        }
        Self::from_request(request)?.ok_or(AuthorityError::Missing)
    }

    pub fn authority(&self) -> &Authority {
        &self.authority
    }

    pub fn host(&self) -> &Host {
        &self.host
    }

    fn equivalent(
        &self,
        other: &Self,
        scheme: Option<&Scheme>,
    ) -> bool {
        let default_port = match scheme {
            Some(scheme) if *scheme == Scheme::HTTP => Some(80),
            Some(scheme) if *scheme == Scheme::HTTPS => Some(443),
            _ => None,
        };
        self.host == other.host
            && self.authority.port_u16().or(default_port)
                == other.authority.port_u16().or(default_port)
    }
}

/// The authority fields of the last request a connection accepted.
///
/// Validation reads only the HTTP version, the target's scheme and authority,
/// and the Host field. A request that repeats all four validates to the same
/// result, so a connection recognises it here without parsing the authority
/// again. Another spelling, a second Host field or a different version is not
/// a repeat and goes through full validation.
///
/// The bytes are copied into buffers this value keeps: a repeat allocates
/// nothing and the accepted request's own storage is not retained.
#[derive(Debug, Default)]
pub struct AcceptedAuthority {
    recorded: bool,
    version: Version,
    scheme: Option<Scheme>,
    target: Kept,
    host: Kept,
}

/// One optional field's bytes, in a buffer reused across requests.
#[derive(Debug, Default)]
struct Kept {
    present: bool,
    bytes: Vec<u8>,
}

impl Kept {
    fn is(
        &self,
        field: Option<&[u8]>,
    ) -> bool {
        match field {
            Some(bytes) => self.present && self.bytes == bytes,
            None => !self.present,
        }
    }

    fn keep(
        &mut self,
        field: Option<&[u8]>,
    ) {
        self.bytes.clear();
        self.present = field.is_some();
        if let Some(bytes) = field {
            self.bytes.extend_from_slice(bytes);
        }
    }
}

/// The Host field when there is at most one; `None` when there are several.
fn single_host(headers: &HeaderMap) -> Option<Option<&HeaderValue>> {
    let mut values = headers.get_all(header::HOST).iter();
    let first = values.next();
    values.next().is_none().then_some(first)
}

impl AcceptedAuthority {
    /// Whether `request` repeats the fields last recorded.
    pub fn repeats<B>(
        &self,
        request: &Request<B>,
    ) -> bool {
        let uri = request.uri();
        if !self.recorded
            || self.version != request.version()
            || self.scheme.as_ref() != uri.scheme()
        {
            return false;
        }
        let Some(host) = single_host(request.headers()) else {
            return false;
        };
        self.target.is(uri.authority().map(|target| target.as_str().as_bytes()))
            && self.host.is(host.map(HeaderValue::as_bytes))
    }

    /// Record the fields of a request that validation has just accepted.
    pub fn record<B>(
        &mut self,
        request: &Request<B>,
    ) {
        let uri = request.uri();
        // Several Host fields never validate; nothing is recorded for them.
        let Some(host) = single_host(request.headers()) else {
            self.recorded = false;
            return;
        };
        self.version = request.version();
        self.scheme = uri.scheme().cloned();
        self.target
            .keep(uri.authority().map(|target| target.as_str().as_bytes()));
        self.host.keep(host.map(HeaderValue::as_bytes));
        self.recorded = true;
    }
}
