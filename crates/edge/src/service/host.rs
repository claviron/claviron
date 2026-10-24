//! Transport-independent host admission and ordinary application dispatch.

use std::sync::Arc;

use claviron_http::config::{HttpPolicy, ProxyConfig, RequestPolicy};
use claviron_http::ext::ConnCtx;
#[cfg(feature = "axum")]
use claviron_http::from_axum_response;
use claviron_http::listener::HttpProtocol;
use claviron_http::{RouteBody, RouteResponse};
use http::{Request, StatusCode};
#[cfg(feature = "axum")]
use tower::ServiceExt;

use super::extensions::{
    VhostCtx, authority_matches_sni, inject_context_and_real_ip,
};
use super::policy::PolicySnapshot;
use super::{AuthorityError, HostResponse, RequestAuthority};
use crate::VhostResolver;
use crate::deny::{DenyCheckedRequest, DenyService};
use crate::pipeline::{AppKind, AuthContext, HostApp};

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

struct HostPolicy {
    connection: Arc<ConnCtx>,
    host: VhostCtx,
    ingress: HostIngress,
    access: Arc<VhostResolver>,
}

/// A pinned host binding for TLS, or one selected cleartext request. It has no
/// socket/body driver. ACL and early host replies read live policy per request;
/// accepted transport/authentication and application policy remain pinned.
#[derive(Clone)]
pub struct HostService {
    app: HostApp,
    policy: Arc<HostPolicy>,
    /// This listener's policy, both sides, narrowed per request by the
    /// host's own in [`HostService::prepare_context`].
    listener: HttpPolicy,
}

impl HostService {
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
            policy: Arc::new(HostPolicy { connection, host, ingress, access }),
            listener,
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
        self.check_head(&request, arrival)?;
        if self.policy.ingress.deny.is_blocked(request.uri().path()) {
            return Err(RequestRejection::PathDenied.into());
        }
        self.prepare_context(request)
    }

    /// Preserve an early transport deny check without evaluating the same
    /// matcher twice. All other host gates still run; another matcher rechecks.
    pub fn prepare_checked<B>(
        &self,
        request: DenyCheckedRequest<'_, B>,
        arrival: RequestArrival,
    ) -> Result<PreparedRequest<B>, HostResponse> {
        self.check_head(request.request(), arrival)?;
        let request = self
            .policy
            .ingress
            .deny
            .accept_checked(request)
            .map_err(|_| RequestRejection::PathDenied)?;
        self.prepare_context(request)
    }

    fn check_head<B>(
        &self,
        request: &Request<B>,
        arrival: RequestArrival,
    ) -> Result<(), HostResponse> {
        let policy = &self.policy;
        let protocol =
            policy.connection.proto.ok_or(RequestRejection::MissingProtocol)?;
        let authority = RequestAuthority::from_request(request)
            .map_err(RequestRejection::InvalidAuthority)?;
        let host = &policy.host.vhost_config;
        if !host.allows_transport(&policy.connection.listener, protocol)
            || !authority_matches_sni(authority.as_ref(), &policy.host)
        {
            return Err(RequestRejection::Misdirected.into());
        }
        if arrival == RequestArrival::EarlyData
            && (protocol != HttpProtocol::H3
                || !allows_early_data(host, request.method()))
        {
            return Err(RequestRejection::TooEarly.into());
        }
        if protocol == HttpProtocol::H1
            && request.headers().contains_key(http::header::UPGRADE)
            && !host.transport.h1.upgrade
        {
            return Err(RequestRejection::UpgradeForbidden.into());
        }
        Ok(())
    }

    fn prepare_context<B>(
        &self,
        mut request: Request<B>,
    ) -> Result<PreparedRequest<B>, HostResponse> {
        let policy = &self.policy;
        let access = policy.access.request_policy(&policy.host.vhost_key.0);
        let connection = inject_context_and_real_ip(
            &mut request,
            &policy.connection,
            &policy.host,
            policy.ingress.proxy.as_ref(),
        );
        // Real IP comes from this accepted listener's trust policy, not from
        // the reloaded host or an untrusted forwarding header.
        access.check(
            request.uri(),
            connection.real_ip.ok_or(RequestRejection::MissingIdentity)?,
        )?;
        #[cfg(feature = "rate-limit")]
        let quota_snapshot = match &self.app.quotas {
            Some(quotas) => quotas.check(&request)?,
            None => None,
        };
        // Resolved here because this is the one place a host is known and a
        // `PreparedRequest` is made, once per request on both transports; the
        // sites that enforce a limit then read one policy instead of choosing
        // between two. Narrowed, never widened: the listener's values stay a
        // guarantee. A host that needs more than its listener allows belongs on
        // another listener that allows it; where hosts share one address and so
        // one listener, the listener carries the largest any of them needs and
        // the others narrow their own.
        let policy =
            self.listener.narrow(&self.policy.host.vhost_config.policy());
        Ok(PreparedRequest {
            request,
            service: self.clone(),
            connection,
            access,
            policy,
            #[cfg(feature = "rate-limit")]
            quota_snapshot,
        })
    }
}

fn allows_early_data(
    host: &crate::config::HostConfig,
    method: &http::Method,
) -> bool {
    #[cfg(feature = "http3")]
    {
        use crate::config::EarlyDataPolicy;
        match host.transport.h3.early_data {
            EarlyDataPolicy::DenyAll => false,
            EarlyDataPolicy::AllowIdempotent => {
                matches!(
                    *method,
                    http::Method::GET
                        | http::Method::HEAD
                        | http::Method::OPTIONS
                )
            }
            EarlyDataPolicy::AllowAll => true,
        }
    }
    #[cfg(not(feature = "http3"))]
    {
        let _ = (host, method);
        false
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
    /// Computed once in [`HostService::prepare_context`].
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

impl PreparedRequest<RouteBody> {
    /// The single Axum/bypass fork. Neither branch knows the native transport.
    ///
    /// It is also where an exchange is counted, for the same reason it is the
    /// fork: everything a host serves passes here exactly once. Counting in an
    /// Axum layer instead — which is where this started, when Axum was the
    /// only shape — left every bypass route invisible, so a host pinned to one
    /// `.php`, a static root or an upstream could wedge without the watchdog
    /// seeing a thing.
    ///
    /// The guard lives until the response future ends, so a cancelled request
    /// or a panicking handler releases it too.
    pub async fn dispatch(self) -> RouteResponse {
        let _exchange = claviron_runtime::activity::ACTIVITY.begin();
        // This request's access record, when `logging.yaml` asks for them:
        // made here because everything a host serves passes here once.
        #[cfg(feature = "access-log")]
        let mut access = crate::access::AccessGuard::begin(
            self.service.policy.host.vhost_config.access_log(),
            &self.service.policy.host.vhost_key.0.to_cow_str(),
            self.request.method(),
            self.request.uri().path(),
            self.request.version(),
            &self.connection,
        );
        // The application receives the policy the edge narrowed for it
        // (listener, then host) and narrows it once more with its own
        // block: the edge computes, the runner owns. A plain
        // `claviron_http` value, so no runner depends on the edge for it.
        let mut request = self.request;
        // Borrow the pinned policy: no per-request Arc clone, and absent
        // configuration means no policy selection or CORS/header processing.
        let exchange = &self.service.app.exchange_policy;
        let exchange = exchange
            .as_ref()
            .and_then(|policy| policy.select(request.uri().path()));
        let server_header = self.service.app.server_header.clone();
        let (early_reply, origin) = exchange
            .map(|policy| policy.check(request.method(), request.headers()))
            .unwrap_or((None, None));
        request.extensions_mut().insert(self.policy);
        // Taken before admission, so an application's own "request start"
        // includes the time the request waited for a place.
        request.extensions_mut().insert(claviron_http::ext::ReceivedAt(
            std::time::SystemTime::now(),
        ));
        let replies = self.policy.responses;
        let requests = self.policy.requests;
        // Admission, on every path to the host: a place among the requests it
        // serves at once, waited for up to the request's `queue_timeout`, and
        // the time to the reply's head. The listener's and the host's values,
        // narrowed; the runner behind may narrow them again for its own queue
        // and its own head. Unwritten, neither is kept.
        let Some(_place) =
            self.service.app.admission.enter(requests.queue_timeout).await
        else {
            #[cfg(feature = "access-log")]
            if let Some(access) = access {
                access.replied(StatusCode::SERVICE_UNAVAILABLE);
            }
            let response = with_server_header(
                status_reply(StatusCode::SERVICE_UNAVAILABLE),
                server_header,
            );
            return match exchange {
                Some(policy) => policy.finish(response, origin.as_ref()),
                None => response,
            };
        };
        #[cfg(feature = "access-log")]
        if let Some(access) = access.as_mut() {
            access.admitted();
        }
        let timed_elsewhere = matches!(&self.service.app.app, AppKind::Bypass(service) if service.times_its_head());
        let answering = async move {
            if let Some(response) = early_reply {
                return response;
            }
            match self.service.app.app {
                #[cfg(feature = "axum")]
                AppKind::Axum(router) => {
                    match router
                        .oneshot(request.map(axum::body::Body::new))
                        .await
                    {
                        Ok(response) => from_axum_response(response),
                        Err(never) => match never {},
                    }
                }
                AppKind::Bypass(service) => match service.handle(request).await
                {
                    Ok(response) => response,
                    Err(never) => match never {},
                },
            }
        };
        // Late here is the server's, not the client's: 504, as a runner's
        // own head timeout answers.
        let Some(response) = claviron_http::admission::head_within(
            requests.head_timeout.filter(|_| !timed_elsewhere),
            answering,
        )
        .await
        else {
            #[cfg(feature = "access-log")]
            if let Some(access) = access {
                access.replied(StatusCode::GATEWAY_TIMEOUT);
            }
            let response = with_server_header(
                status_reply(StatusCode::GATEWAY_TIMEOUT),
                server_header,
            );
            return match exchange {
                Some(policy) => policy.finish(response, origin.as_ref()),
                None => response,
            };
        };
        let response = with_server_header(response, server_header);
        let response = match exchange {
            Some(policy) => policy.finish(response, origin.as_ref()),
            None => response,
        };
        // The reply's own guard, on every path an application answers
        // through — axum or not: its ceiling and the longest gap between two
        // of its pieces. Where it was a tower layer it guarded axum's replies
        // only, and hid every reply's length from the transport.
        let response = if replies.max_body_size.is_none()
            && replies.body_idle_timeout.is_none()
        {
            response
        } else {
            response.map(|body| {
                claviron_http::body::guard_reply(
                    body,
                    replies.max_body_size,
                    replies.body_idle_timeout,
                )
            })
        };
        // Outermost, so what it counts is what the client is sent.
        #[cfg(feature = "access-log")]
        let response = match access {
            Some(access) => access.wrap(response),
            None => response,
        };
        #[cfg(feature = "rate-limit")]
        let response = {
            let mut response = response;
            claviron_http::rate_limit::apply_headers(
                self.quota_snapshot,
                &mut response,
            );
            response
        };
        response
    }
}

/// A reply with a status and nothing else.
fn status_reply(status: StatusCode) -> RouteResponse {
    let mut response = RouteResponse::new(claviron_http::empty_body());
    *response.status_mut() = status;
    response
}

fn with_server_header(
    mut response: RouteResponse,
    server: Option<http::HeaderValue>,
) -> RouteResponse {
    if let Some(server) = server {
        response.headers_mut().insert(http::header::SERVER, server);
    }
    response
}

#[cfg(test)]
#[path = "../../tests/service/host.rs"]
mod tests;
