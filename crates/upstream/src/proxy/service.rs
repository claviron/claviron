//! Owns upstream request policy checks, protocol dispatch and service construction.

pub(crate) mod control;
pub(crate) mod error;
pub(crate) mod kind;
pub(crate) mod membership;
mod policy;
pub(crate) mod prepare;
pub(crate) mod request;
pub(crate) mod runtime;
pub(crate) mod transport;
pub(crate) mod warm;
pub use membership::{MembershipError, MembershipTarget};
pub use policy::UpstreamRequestPolicy;
pub(crate) use runtime::ProtocolPools;

// One upstream, as a consumer sees it: the handle, what it is doing, the
// policy a door hands it, and the membership it serves under.
pub use crate::proxy::service::lifecycle::UpstreamLifecycle;

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::AtomicU8;

use claviron_http::forwarding::reject_request_smuggling;
use claviron_http::policy::{BodyNeed, Delivery};
use claviron_http::{RouteBody, RouteResponse};
use error::{error_response, failure_response};
use http::{Method, Request, StatusCode};
use kind::{is_h1_upgrade_request, is_h2_extended_connect_request};

use crate::compile::profile::Profile;
use crate::proxy::failure::RequestFailure;

#[derive(Clone)]
pub struct UpstreamService {
    pub(crate) inner: Arc<ServiceInner>,
}
pub(crate) struct ServiceInner {
    pub(crate) profile: Arc<Profile>,
    pub(crate) balancer: crate::proxy::balance::Selection,
    // Projection for local pool cleanup; authoritative health stays in Origin.slot.
    pub(crate) observed_health: Box<[AtomicU8]>,
    pub(crate) observed_serving: Box<[AtomicU8]>,
    pub(crate) lifecycle: lifecycle::Lifecycle,
    pub(crate) runtime: runtime::ServiceRuntime,
}

impl fmt::Debug for UpstreamService {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        f.debug_struct("UpstreamService")
            .field("profile", &self.inner.profile.name)
            .field("pools", &self.epoch().pools)
            .finish_non_exhaustive()
    }
}

impl UpstreamService {
    pub fn new(profile: Arc<Profile>) -> Self {
        let runtime = if profile.origins.len() <= crate::state::layout::CAPACITY
        {
            runtime::ServiceRuntime::detached()
        } else {
            runtime::ServiceRuntime::with_table(
                crate::state::UpstreamStateTable::memory(
                    profile.origins.len().next_power_of_two(),
                ),
            )
        };
        Self::with_runtime(profile, runtime)
    }
    pub(crate) fn with_runtime(
        mut profile: Arc<Profile>,
        runtime: runtime::ServiceRuntime,
    ) -> Self {
        // Provider boot pre-binds the compiled representation. A detached caller
        // binds a private config copy once; it cannot steal another runtime's slot.
        if profile
            .origins
            .iter()
            .any(|origin| !origin.slot.belongs_to(&runtime.state_table))
        {
            for origin in
                Arc::make_mut(&mut Arc::make_mut(&mut profile).origins)
                    .iter_mut()
            {
                origin.slot.0 = Some(
                    runtime
                        .state_table
                        .install(&origin.state_key, 0)
                        .expect("runtime state capacity prevalidated before registry adoption")
                );
            }
        }
        let lifecycle = lifecycle::Lifecycle::new(&profile, &runtime);
        Self::from_lifecycle(profile, runtime, lifecycle)
    }
    pub(crate) fn from_lifecycle(
        profile: Arc<Profile>,
        runtime: runtime::ServiceRuntime,
        lifecycle: lifecycle::Lifecycle,
    ) -> Self {
        let service = Self {
            inner: Arc::new(ServiceInner {
                lifecycle,
                balancer: crate::proxy::balance::Selection::new(
                    profile.clone(),
                ),
                observed_health: profile
                    .origins
                    .iter()
                    .map(|origin| {
                        AtomicU8::new(origin.slot.get().health() as u8)
                    })
                    .collect(),
                observed_serving: profile
                    .origins
                    .iter()
                    .map(|origin| {
                        AtomicU8::new(origin.slot.get().serving() as u8)
                    })
                    .collect(),
                profile,
                runtime,
            }),
        };
        service.register_configured_capacity();
        service.attach_origin_projections();
        service.attach_warm_pools();
        service
    }

    pub(crate) fn is_shutting_down(&self) -> bool {
        self.inner.runtime.shutdown.is_cancelled()
            || self.epoch().background.is_cancelled()
    }

    pub(crate) async fn wait_shutdown(&self) {
        let epoch = self.epoch();
        tokio::select! { _ = self.inner.runtime.shutdown.cancelled() => {}, _ = epoch.background.cancelled() => {} }
    }

    pub async fn handle(
        &self,
        req: Request<RouteBody>,
    ) -> RouteResponse {
        let scope = request::RequestScope::from_request(&req);
        self.handle_scoped(req, scope).await
    }

    pub(crate) async fn handle_scoped(
        &self,
        req: Request<RouteBody>,
        mut scope: request::RequestScope,
    ) -> RouteResponse {
        scope.requests = scope.requests.narrow(&self.inner.profile.requests);
        scope.responses = scope.responses.narrow(&self.inner.profile.responses);
        if let Err(err) = reject_request_smuggling(req.version(), req.headers())
        {
            return error_response(StatusCode::BAD_REQUEST, err.to_string());
        }

        if is_h1_upgrade_request(&req) {
            let policy = scope.policy;
            if !policy.allow_h1_upgrade {
                return error_response(
                    StatusCode::UPGRADE_REQUIRED,
                    "upstream h1 upgrade is disabled by host policy",
                );
            }
        }
        if is_h2_extended_connect_request(&req) {
            let policy = scope.policy;
            if !policy.allow_connect_protocol {
                return error_response(
                    StatusCode::FORBIDDEN,
                    "h2 extended connect is disabled by host policy",
                );
            }
        } else if req.method() == Method::CONNECT {
            return error_response(
                StatusCode::METHOD_NOT_ALLOWED,
                "generic CONNECT is not supported by http upstream",
            );
        }

        let lease =
            match self.admit(!http_body::Body::is_end_stream(req.body())) {
                Ok(lease) => lease,
                Err(error) => return failure_response(error),
            };
        // Freeze this request's pool generation through its body/tunnel lifetime.
        let epoch = lease.epoch.clone();
        if let Some(window) = &epoch.retry {
            window.record_request();
        }
        scope.lease = Some(lease);
        tokio::select! {
            biased;
            _ = epoch.force.cancelled() => failure_response(RequestFailure::ProfileStopped),
            response = self.handle_inner(req, &mut scope) => response.unwrap_or_else(failure_response),
        }
    }

    async fn handle_inner(
        &self,
        req: Request<RouteBody>,
        scope: &mut request::RequestScope,
    ) -> Result<RouteResponse, RequestFailure> {
        let epoch =
            scope.lease.as_ref().expect("admitted request").epoch.clone();
        let req = if self
            .inner
            .profile
            .retrying
            .as_ref()
            .is_some_and(|retry| retry.eligible(req.method()))
            && !is_h1_upgrade_request(&req)
            && !is_h2_extended_connect_request(&req)
        {
            scope.pending = match epoch
                .breaker
                .as_ref()
                .map(|state| state.admit(false))
                .transpose()
            {
                Ok(pending) => pending,
                Err(error) => {
                    // Before selection, the first configured origin owns this
                    // profile-wide rejection metric; no body has been polled.
                    self.record_breaker_rejection(0);
                    return Err(error);
                }
            };
            let (parts, body) = req.into_parts();
            // Held whole, a body can be sent again; the policy says whether
            // it is held (a body up to `up_to`, the rest streaming).
            let limit = hold_limit(&scope.requests, &body);
            match crate::proxy::body::buffer::collect(
                body,
                limit,
                scope.requests.body_read_timeout,
            )
            .await?
            {
                crate::proxy::body::buffer::Collected::Replay(replay) => {
                    // Preserve the input lease for the first replay writer. H2
                    // can return response EOF before those buffered bytes finish
                    // writing; incoming collection is not outbound completion.
                    let req = Request::from_parts(parts, replay);
                    return match &epoch.pools {
                        ProtocolPools::H1(pools) => {
                            retry::run(self, pools.as_ref(), &epoch, req, scope)
                                .await
                        }
                        ProtocolPools::H2(pools) => {
                            retry::run(self, pools.as_ref(), &epoch, req, scope)
                                .await
                        }
                    };
                }
                crate::proxy::body::buffer::Collected::Streaming(body) => {
                    Request::from_parts(parts, body)
                }
            }
        } else if !is_h1_upgrade_request(&req)
            && !is_h2_extended_connect_request(&req)
            && !http_body::Body::is_end_stream(req.body())
        {
            // Not retried, yet the policy may still say to take the body
            // first — an origin shielded from a slow client, as PHP's
            // workers are. Held, it goes out once from memory.
            let (parts, body) = req.into_parts();
            let limit = hold_limit(&scope.requests, &body);
            if limit == 0 {
                Request::from_parts(parts, body)
            } else {
                match crate::proxy::body::buffer::collect(
                    body,
                    limit,
                    scope.requests.body_read_timeout,
                )
                .await?
                {
                    crate::proxy::body::buffer::Collected::Replay(replay) => {
                        Request::from_parts(parts, replay.into_route_body())
                    }
                    crate::proxy::body::buffer::Collected::Streaming(body) => {
                        Request::from_parts(parts, body)
                    }
                }
            }
        } else {
            req
        };
        match &epoch.pools {
            ProtocolPools::H1(pools) => {
                self.handle_h1_inner(req, pools, scope).await
            }
            ProtocolPools::H2(pools) => {
                self.handle_h2_inner(req, pools, scope).await
            }
        }
    }
}

pub(crate) mod attempt;
pub(crate) mod breaker;
pub(crate) mod retry;

pub(crate) mod tunnel;

pub(crate) mod lifecycle;

/// How much of a request body to hold before sending it, as the request
/// policy decides: `0` holds nothing, `u64::MAX` the whole body (bounded
/// by the edge's `max_body_size`), anything else that many bytes before
/// the rest streams.
fn hold_limit(
    requests: &claviron_http::policy::RequestPolicy,
    body: &RouteBody,
) -> u64 {
    let declared = http_body::Body::size_hint(body).exact();
    match requests.delivery(BodyNeed::Stream, declared, false) {
        Delivery::Whole { up_to } => up_to.map_or(u64::MAX, |size| size as u64),
        Delivery::Pieces => 0,
    }
}
