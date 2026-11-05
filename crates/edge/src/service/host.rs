//! Transport-independent host admission and ordinary application dispatch.

use std::sync::Arc;

use claviron_http::config::{HttpPolicy, RequestPolicy};
use claviron_http::ext::ConnCtx;
#[cfg(test)]
use claviron_http::listener::HttpProtocol;
use claviron_http::{RouteBody, RouteResponse};
use http::{Request, StatusCode};

use super::AuthorityError;
#[cfg(feature = "tcp")]
use super::CleartextRequest;
use super::extensions::VhostCtx;
use crate::pipeline::host::HostPolicy;
use crate::pipeline::{AuthContext, HostExecution};

mod dispatch;

#[cfg(feature = "tcp")]
mod cache;
#[cfg(feature = "tcp")]
pub(crate) use cache::CleartextHostCache;

/// A fact supplied by the native transport, never by a request header.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestArrival {
    Established,
    EarlyData,
}

/// Shared decision; native transports retain their wire response/close mechanics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestRejection {
    MissingProtocol,
    MissingIdentity,
    InvalidAuthority(AuthorityError),
    Misdirected,
    AccessDenied,
    AmbiguousPath,
    InvalidRedirect,
    TooEarly,
}

impl AsRef<RequestArrival> for RequestArrival {
    fn as_ref(&self) -> &RequestArrival {
        self
    }
}

impl RequestRejection {
    pub fn status(self) -> StatusCode {
        match self {
            Self::MissingProtocol
            | Self::MissingIdentity
            | Self::InvalidRedirect => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Misdirected => StatusCode::MISDIRECTED_REQUEST,
            Self::AccessDenied => StatusCode::FORBIDDEN,
            Self::AmbiguousPath | Self::InvalidAuthority(_) => {
                StatusCode::BAD_REQUEST
            }
            Self::TooEarly => StatusCode::TOO_EARLY,
        }
    }
}

/// An accepted connection bound to a published request owner. This handoff
/// retains transport facts and declarations; it installs no host actions.
#[derive(Clone)]
pub struct HostService {
    app: HostExecution,
    policy: Arc<HostPolicy>,
}

impl HostService {
    pub fn new(
        app: HostExecution,
        connection: Arc<ConnCtx>,
        host: VhostCtx,
        listener: HttpPolicy,
    ) -> Self {
        Self {
            app,
            policy: Arc::new(HostPolicy::new(connection, host, listener)),
        }
    }

    /// Compare each TLS request with the host pinned by SNI. This borrows the
    /// native request before body adaptation or any host-pipeline action.
    ///
    /// `accepted` belongs to this connection. A request that repeats the
    /// authority fields it last accepted names the same host and is admitted
    /// without parsing them again; every other request is validated in full.
    #[cfg(any(all(feature = "tcp", feature = "tls"), feature = "quic"))]
    pub(crate) fn validate_authority<B>(
        &self,
        request: &Request<B>,
        accepted: &mut super::AcceptedAuthority,
    ) -> Result<(), RequestRejection> {
        if accepted.repeats(request) {
            return Ok(());
        }
        let authority = super::RequestAuthority::from_request(request)
            .map_err(RequestRejection::InvalidAuthority)?;
        if authority.is_some_and(|authority| {
            authority.host() != self.policy.host.hostname.0.as_ref()
        }) {
            return Err(RequestRejection::Misdirected);
        }
        accepted.record(request);
        Ok(())
    }

    /// Retain the original native body and accepted facts without executing
    /// policies or populating extensions. Host actions belong to HostPipeline.
    pub fn request<B>(
        &self,
        request: Request<B>,
        arrival: RequestArrival,
    ) -> PreparedRequest<B> {
        self.clone().into_request(request, arrival)
    }

    #[cfg(feature = "tcp")]
    pub(crate) fn request_cleartext<B>(
        self,
        request: CleartextRequest<B>,
    ) -> PreparedRequest<B> {
        let (request, _) = request.into_parts();
        self.into_request(request, RequestArrival::Established)
    }

    fn into_request<B>(
        self,
        request: Request<B>,
        arrival: RequestArrival,
    ) -> PreparedRequest<B> {
        PreparedRequest { request, service: self, arrival }
    }
}

/// A bound request keeps its owner and declaration generation while a driver
/// adapts the body. Native CONNECT/session handlers may take ownership instead.
pub struct PreparedRequest<B> {
    request: Request<B>,
    service: HostService,
    arrival: RequestArrival,
}

impl<B> PreparedRequest<B> {
    pub fn arrival(&self) -> RequestArrival {
        self.arrival
    }

    pub fn request(&self) -> &Request<B> {
        &self.request
    }

    /// Native adapters may attach transport capabilities after admission.
    pub fn extensions_mut(&mut self) -> &mut http::Extensions {
        self.request.extensions_mut()
    }

    pub fn connection(&self) -> &Arc<ConnCtx> {
        &self.service.policy.connection
    }

    /// The listener's request policy after this host's own narrowing.
    pub fn requests(&self) -> &RequestPolicy {
        &self.service.policy.http.requests
    }

    /// Largest request body this request may carry; `None` is no limit.
    pub fn max_body_size(&self) -> Option<usize> {
        self.service.policy.http.requests.max_body_size
    }

    pub fn host_key(&self) -> &claviron_http::ext::HostKey {
        &self.service.policy.host.vhost_key
    }

    pub fn auth_context(&self) -> AuthContext {
        let host = &self.service.policy.host;
        AuthContext {
            host: host.vhost_key.0.as_ref().clone(),
            host_config: host.vhost_config.clone(),
            path: Arc::from(self.request.uri().path()),
            uri: self.request.uri().clone(),
            headers: self.request.headers().clone(),
            conn: self.service.policy.connection.clone(),
            admission: None,
            graceful: None,
        }
    }

    pub fn map_body<T>(
        self,
        map: impl FnOnce(B) -> T,
    ) -> PreparedRequest<T> {
        PreparedRequest {
            request: self.request.map(map),
            service: self.service,
            arrival: self.arrival,
        }
    }

    pub fn into_request(self) -> Request<B> {
        self.request
    }
}

#[cfg(test)]
#[path = "../../tests/service/host.rs"]
mod tests;
