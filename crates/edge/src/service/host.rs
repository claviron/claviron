//! Transport-independent host admission and ordinary application dispatch.

use std::sync::Arc;

use claviron_http::config::{HttpPolicy, ProxyConfig, RequestPolicy};
use claviron_http::ext::ConnCtx;
#[cfg(any(test, all(feature = "tcp", feature = "tls")))]
use claviron_http::listener::HttpProtocol;
use claviron_http::{RouteBody, RouteResponse};
use http::{Request, StatusCode};

use super::extensions::VhostCtx;
use super::policy::PolicySnapshot;
use super::{AuthorityError, HostResponse};
#[cfg(feature = "tcp")]
use super::{CleartextRequest, RequestAuthority};
use crate::VhostResolver;
use crate::deny::{DenyCheckedRequest, DenyService};
use crate::pipeline::host::{
    HostPolicy, HostRequestState, IdentifiedRequest,
};
use crate::pipeline::{RustRouter, AuthContext, HostApp};

mod dispatch;

#[cfg(feature = "tcp")]
mod cache;
#[cfg(feature = "tcp")]
pub(crate) use cache::CleartextHostCache;

/// Accepted listener policy, not selected from an untrusted Host header.
#[derive(Clone)]
pub struct HostIngress {
    pub proxy: Option<ProxyConfig>,
    pub deny: Arc<DenyService>,
}

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
    UpgradeForbidden,
    PathDenied,
    AccessDenied,
    HostRemoved,
    AmbiguousPath,
    InvalidRedirect,
    TooEarly,
}

impl RequestRejection {
    pub fn status(self) -> StatusCode {
        match self {
            Self::MissingProtocol
            | Self::MissingIdentity
            | Self::InvalidRedirect => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Misdirected | Self::HostRemoved => {
                StatusCode::MISDIRECTED_REQUEST
            }
            Self::UpgradeForbidden | Self::PathDenied | Self::AccessDenied => {
                StatusCode::FORBIDDEN
            }
            Self::AmbiguousPath | Self::InvalidAuthority(_) => {
                StatusCode::BAD_REQUEST
            }
            Self::TooEarly => StatusCode::TOO_EARLY,
        }
    }
}

/// A pinned host binding for TLS, or one selected cleartext request. It has no
/// socket/body driver. ACL and early host replies read live policy per request;
/// accepted transport/authentication and application policy remain pinned.
/// Immutable extension values are prepared with the binding; each admitted
/// request receives its own map. Cleartext connections reuse only the last
/// matching metadata binding, selecting the application from the current
/// publication for every request.
#[derive(Clone)]
pub struct HostService {
    app: HostApp,
    policy: Arc<HostPolicy>,
}

impl HostService {
    #[cfg(all(feature = "tcp", any(feature = "tls", test)))]
    pub(crate) fn protocol(&self) -> Option<HttpProtocol> {
        self.policy.connection.proto
    }

    pub fn new(
        app: impl Into<HostApp>,
        connection: Arc<ConnCtx>,
        host: VhostCtx,
        ingress: HostIngress,
        access: Arc<VhostResolver>,
        listener: HttpPolicy,
    ) -> Self {
        Self {
            app: app.into(),
            policy: Arc::new(HostPolicy::new(
                connection, host, ingress, access, listener,
            )),
        }
    }

    /// Check the head without polling its body, then inject a fresh request
    /// identity. `Err` is a completed host reply (including redirects), not
    /// necessarily a failure. Only `Ok` enters application/native dispatch.
    pub fn prepare<B>(
        &self,
        request: Request<B>,
        arrival: RequestArrival,
    ) -> Result<PreparedRequest<B>, HostResponse> {
        let request = self
            .policy
            .pipeline
            .admit(request, HostRequestState::new(arrival))?;
        Ok(self.prepare_context(request)?.with_service(self.clone()))
    }

    /// Preserve an early transport deny check without evaluating the same
    /// matcher twice. All other host gates still run; another matcher rechecks.
    pub fn prepare_checked<B>(
        &self,
        request: DenyCheckedRequest<'_, B>,
        arrival: RequestArrival,
    ) -> Result<PreparedRequest<B>, HostResponse> {
        let request = self.policy.pipeline.admit_checked(
            request,
            &self.policy.ingress.deny,
            arrival,
            None,
        )?;
        Ok(self.prepare_context(request)?.with_service(self.clone()))
    }

    /// Reuse the authority validated for this cleartext host selection. The
    /// receiving host still checks its binding, transport and live policy.
    /// Cleartext selects an owned application per request, which moves into the
    /// admitted request. Its immutable metadata binding may be shared with the
    /// connection cache; neither that cache nor another request owns this body.
    #[cfg(feature = "tcp")]
    pub(crate) fn prepare_cleartext<B>(
        self,
        request: CleartextRequest<'_, B>,
    ) -> Result<PreparedRequest<B>, HostResponse> {
        let (request, authority) = request.into_parts();
        let request = self.policy.pipeline.admit_checked(
            request,
            &self.policy.ingress.deny,
            RequestArrival::Established,
            Some(authority),
        )?;
        Ok(self.prepare_context(request)?.with_service(self))
    }

    fn prepare_context<B>(
        &self,
        identified: IdentifiedRequest<B>,
    ) -> Result<PreparedContext<B>, HostResponse> {
        let IdentifiedRequest { head, body, connection, access } = identified;
        let policy = &self.policy;
        #[cfg(feature = "rate-limit")]
        let mut head = head;
        #[cfg(feature = "rate-limit")]
        let quota_snapshot = match &self.app.quotas {
            Some(quotas) => quotas.check(&mut head)?,
            None => None,
        };
        Ok(PreparedContext {
            request: Request::from_parts(head, body),
            connection,
            access,
            policy: policy.http,
            #[cfg(feature = "rate-limit")]
            quota_snapshot,
        })
    }
}

struct PreparedContext<B> {
    request: Request<B>,
    connection: Arc<ConnCtx>,
    access: PolicySnapshot,
    policy: HttpPolicy,
    #[cfg(feature = "rate-limit")]
    quota_snapshot: Option<claviron_http::rate_limit::Snapshot>,
}

impl<B> PreparedContext<B> {
    fn with_service(
        self,
        service: HostService,
    ) -> PreparedRequest<B> {
        PreparedRequest {
            request: self.request,
            service,
            connection: self.connection,
            access: self.access,
            policy: self.policy,
            #[cfg(feature = "rate-limit")]
            quota_snapshot: self.quota_snapshot,
        }
    }
}

/// An admitted request keeps its app and policy generation while a driver
/// adapts the body. Native CONNECT/session handlers may take ownership instead.
pub struct PreparedRequest<B> {
    request: Request<B>,
    service: HostService,
    connection: Arc<ConnCtx>,
    access: PolicySnapshot,
    /// The listener's policy, both sides, already narrowed by this host.
    /// Prepared with the immutable host binding, then copied at admission.
    policy: HttpPolicy,
    #[cfg(feature = "rate-limit")]
    quota_snapshot: Option<claviron_http::rate_limit::Snapshot>,
}

impl<B> PreparedRequest<B> {
    #[cfg(feature = "rate-limit")]
    pub(crate) fn quota_snapshot(
        &self
    ) -> Option<claviron_http::rate_limit::Snapshot> {
        self.quota_snapshot
    }
    pub fn request(&self) -> &Request<B> {
        &self.request
    }

    /// Native adapters may attach transport capabilities after admission.
    pub fn extensions_mut(&mut self) -> &mut http::Extensions {
        self.request.extensions_mut()
    }

    pub fn connection(&self) -> &Arc<ConnCtx> {
        &self.connection
    }

    /// The listener's request policy after this host's own narrowing.
    pub const fn requests(&self) -> &RequestPolicy {
        &self.policy.requests
    }

    /// Largest request body this request may carry; `None` is no limit.
    pub const fn max_body_size(&self) -> Option<usize> {
        self.policy.requests.max_body_size
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
            conn: self.connection.clone(),
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
            connection: self.connection,
            access: self.access,
            policy: self.policy,
            #[cfg(feature = "rate-limit")]
            quota_snapshot: self.quota_snapshot,
        }
    }

    pub fn into_request(self) -> Request<B> {
        self.request
    }
}

#[cfg(test)]
#[path = "../../tests/service/host.rs"]
mod tests;
