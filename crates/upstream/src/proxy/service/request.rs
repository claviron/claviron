//! Owns the one outbound URI, hop-header and trusted forwarding preparation.
//!
//! ALLOCATION_BOUNDARY: Profile/Origin prepare invariant authority and header
//! values during config compilation (boot/reload). Mutable headers and client forwarding
//! context belong to this request; they cannot be shared as boot-time values.
//! Tunable preallocation/reserve sizes are build-time choices: the library owns
//! defaults and validation; downstream applications may override them through
//! their Cargo build environment. They are not runtime YAML/reload settings.
//! CLAVIRON_UPSTREAM_HEADER_RESERVE adds spare entries to the computed rewrite
//! needs (default zero). It is a storage hint, not a header/body limit or a test budget.
//! Allocation reductions must preserve the same forwarded headers and bytes.
use claviron_http::RouteBody;
use claviron_http::forwarding::{
    ForwardedHeaderInfo, ForwardedRequestContext, apply_forwarded_headers,
    apply_rfc7239_forwarded_headers, strip_hop_by_hop_headers,
};
use http::header::{CONNECTION, HOST};
use http::{HeaderValue, Request, Uri, Version};

use crate::compile::profile::Profile;
use crate::config::profile::forward::{ForwardedPolicy, HostPolicy};
use crate::origin::Origin;
use crate::proxy::service::UpstreamRequestPolicy;

/// Additional header-entry headroom beyond the computed static rewrites and
/// Host/Connection restoration. The measured default needs no spare entries.
/// Downstream Cargo builds can override this storage hint; runtime reload does
/// not change it. Decimal/overflow validation runs during constant evaluation.
/// Unlike shared-memory geometry, this local hint is not a fleet ABI value.
const HEADER_RESERVE: usize = orbitive::core::compile::usize_from_env(
    option_env!("CLAVIRON_UPSTREAM_HEADER_RESERVE"),
    0,
);

pub(crate) struct RequestScope {
    pub(crate) lease: Option<crate::proxy::lease::RequestLease>,
    pub(crate) pending: Option<super::breaker::Admission>,
    pub(crate) policy: UpstreamRequestPolicy,
    pub(crate) forwarded: ForwardedRequestContext,
    /// The request policy this request runs under: what the edge handed
    /// over, narrowed by the profile's own `requests:` when the service
    /// takes the request.
    pub(crate) requests: claviron_http::policy::RequestPolicy,
    /// The reply side of the same policy, narrowed the same way.
    pub(crate) responses: claviron_http::policy::ResponsePolicy,
}
impl RequestScope {
    pub(crate) fn from_request(req: &Request<RouteBody>) -> Self {
        let edge = req
            .extensions()
            .get::<claviron_http::policy::HttpPolicy>()
            .copied()
            .unwrap_or(claviron_http::policy::HttpPolicy::unlimited());
        Self {
            lease: None,
            pending: None,
            policy: req
                .extensions()
                .get::<UpstreamRequestPolicy>()
                .copied()
                .unwrap_or_default(),
            forwarded: ForwardedRequestContext::from_request(req),
            requests: edge.requests,
            responses: edge.responses,
        }
    }
}
#[derive(Clone, Copy)]
pub(crate) enum Form {
    OriginKeepAlive,
    OriginClose,
    OriginUpgrade,
    Absolute,
}
#[derive(Debug)]
pub(crate) enum PrepareError {
    InvalidHost,
    InvalidUri,
}
pub(crate) fn prepare<B>(
    origin: &Origin,
    profile: &Profile,
    req: &mut Request<B>,
    form: Form,
    scope: &RequestScope,
) -> Result<(), PrepareError> {
    prepare_inner(origin, profile, req, form, scope, Some(HEADER_RESERVE))
}

// The optional hint keeps the diagnostic's original preparation baseline exact.
pub(crate) fn prepare_inner<B>(
    origin: &Origin,
    profile: &Profile,
    req: &mut Request<B>,
    form: Form,
    scope: &RequestScope,
    headroom: Option<usize>,
) -> Result<(), PrepareError> {
    let host = req.headers_mut().remove(HOST).or_else(|| {
        req.uri()
            .authority()
            .and_then(|a| HeaderValue::from_str(a.as_str()).ok())
    });
    let inbound = std::mem::take(req.uri_mut()).into_parts();
    let mut parts = http::uri::Parts::default();
    parts.path_and_query = Some(
        inbound
            .path_and_query
            .unwrap_or_else(|| http::uri::PathAndQuery::from_static("/")),
    );
    if matches!(form, Form::Absolute) {
        let scheme = http::uri::Scheme::HTTP;
        let scheme =
            if matches!(origin.connect, crate::origin::ConnectTarget::Tls(_)) {
                http::uri::Scheme::HTTPS
            } else {
                scheme
            };
        parts.scheme = Some(scheme);
        parts.authority =
            Some(if profile.forward.host == HostPolicy::Preserve {
                host.as_ref()
                    .map(|host| {
                        host.to_str()
                            .map_err(|_| PrepareError::InvalidHost)?
                            .parse::<http::uri::Authority>()
                            .map_err(|_| PrepareError::InvalidHost)
                    })
                    .transpose()?
                    .unwrap_or_else(|| origin.authority.clone())
            } else {
                origin.authority.clone()
            });
    }
    *req.uri_mut() =
        Uri::from_parts(parts).map_err(|_| PrepareError::InvalidUri)?;
    *req.version_mut() = if matches!(form, Form::Absolute) {
        Version::HTTP_2
    } else {
        Version::HTTP_11
    };
    if !matches!(form, Form::OriginUpgrade) {
        strip_hop_by_hop_headers(req.headers_mut());
    }
    let outbound_host = if profile.forward.host == HostPolicy::Preserve {
        host.clone().unwrap_or_else(|| origin.host.clone())
    } else {
        origin.host.clone()
    };
    let info = ForwardedHeaderInfo {
        real_ip: scope.forwarded.real_ip,
        proto: scope.forwarded.proto.as_ref().map(http::uri::Scheme::as_str),
        host,
        via: profile.forward.via.then(|| profile.via.to_str().ok()).flatten(),
    };
    match profile.forward.forwarded {
        ForwardedPolicy::Xff => {
            loop {
                let name = req
                    .headers()
                    .keys()
                    .find(|name| name.as_str().starts_with("x-forwarded-"))
                    .cloned();
                let Some(name) = name else {
                    break;
                };
                req.headers_mut().remove(name);
            }
            apply_forwarded_headers(req.headers_mut(), info);
        }
        ForwardedPolicy::Rfc7239 => {
            apply_rfc7239_forwarded_headers(req.headers_mut(), info)
        }
        ForwardedPolicy::None => apply_rfc7239_forwarded_headers(
            req.headers_mut(),
            ForwardedHeaderInfo { via: info.via, ..Default::default() },
        ),
    }
    for name in &profile.forwarding.unset {
        req.headers_mut().remove(name);
    }
    if let Some(headroom) = headroom {
        let headers = req.headers_mut();
        let mut additional = profile
            .forwarding
            .set
            .iter()
            .filter(|(name, _)| !headers.contains_key(name))
            .count();
        let restores_host = !matches!(form, Form::Absolute);
        let restores_connection =
            matches!(form, Form::OriginKeepAlive | Form::OriginClose);
        for (name, restored) in
            [(HOST, restores_host), (CONNECTION, restores_connection)]
        {
            if restored
                && !headers.contains_key(&name)
                && !profile.forwarding.set.iter().any(|(set, _)| set == name)
            {
                additional += 1;
            }
        }
        // A best-effort storage hint must not introduce a new request rejection.
        let _ = headers.try_reserve(additional.saturating_add(headroom));
    }
    for (name, value) in &profile.forwarding.set {
        req.headers_mut().insert(name.clone(), value.clone());
    }
    match form {
        Form::Absolute => {
            req.headers_mut().remove(CONNECTION);
            req.headers_mut().remove(HOST);
        }
        Form::OriginUpgrade => {
            req.headers_mut().insert(HOST, outbound_host);
        }
        Form::OriginKeepAlive | Form::OriginClose => {
            req.headers_mut().insert(HOST, outbound_host);
            req.headers_mut().insert(
                CONNECTION,
                HeaderValue::from_static(
                    if matches!(form, Form::OriginKeepAlive) {
                        "keep-alive"
                    } else {
                        "close"
                    },
                ),
            );
        }
    }
    Ok(())
}
