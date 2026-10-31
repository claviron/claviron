//! Select an application before constructing its host dispatch future.

use super::*;
use crate::pipeline::bypass::BypassService;
use crate::pipeline::host::ExchangeState;
#[cfg(feature = "tcp")]
use claviron_http::RouteFuture;
use claviron_http::admission::Admission;
use futures_util::FutureExt;
#[cfg(feature = "axum")]
use futures_util::future::Either;
#[cfg(feature = "tcp")]
use std::convert::Infallible;
use std::future::Future;
#[cfg(feature = "axum")]
use tower::ServiceExt;

/// Host policy shared by both dispatch implementations, without an AppKind
/// or the other application's concrete answering future.
struct DispatchRequest {
    request: Request<RouteBody>,
    host: Arc<HostPolicy>,
    connection: Arc<ConnCtx>,
    access: PolicySnapshot,
    policy: HttpPolicy,
    admission: Admission,
    exchange_policy: Option<Arc<super::super::ExchangePolicy>>,
    server_header: Option<http::HeaderValue>,
    #[cfg(feature = "rate-limit")]
    quotas: Option<Arc<crate::HostQuotas>>,
    #[cfg(feature = "rate-limit")]
    quota_snapshot: Option<claviron_http::rate_limit::Snapshot>,
}

impl PreparedRequest<RouteBody> {
    fn into_dispatch(self) -> (AppKind, DispatchRequest) {
        let HostApp {
            app,
            admission,
            exchange_policy,
            server_header,
            #[cfg(feature = "rate-limit")]
            quotas,
        } = self.service.app;
        (
            app,
            DispatchRequest {
                request: self.request,
                host: self.service.policy,
                connection: self.connection,
                access: self.access,
                policy: self.policy,
                admission,
                exchange_policy,
                server_header,
                #[cfg(feature = "rate-limit")]
                quotas,
                #[cfg(feature = "rate-limit")]
                quota_snapshot: self.quota_snapshot,
            },
        )
    }

    /// Dispatch to the admitted application without allocating a host future.
    ///
    /// Each owner has its own concrete future. The unboxed sum returned here
    /// supports drivers that await inline; TCP uses `into_tcp_future` to keep
    /// its Bypass state independent of Axum without boxing it a second time.
    /// The activity and admission guards remain owned until the reply's head,
    /// including when the caller cancels or the handler panics.
    pub fn dispatch(self) -> impl Future<Output = RouteResponse> + Send {
        let (app, request) = self.into_dispatch();
        match app {
            #[cfg(feature = "axum")]
            AppKind::Axum(router) => Either::Left(request.dispatch(router)),
            AppKind::Bypass(service) => {
                let answering = request.dispatch(service);
                #[cfg(feature = "axum")]
                {
                    Either::Right(answering)
                }
                #[cfg(not(feature = "axum"))]
                {
                    answering
                }
            }
        }
    }

    /// Keep Bypass dispatch inline through the native TCP service boundary.
    /// Only Axum is erased here; its concrete state must not enlarge Bypass.
    #[cfg(feature = "tcp")]
    pub(crate) fn into_tcp_future(
        self
    ) -> Either<
        RouteFuture,
        impl Future<Output = Result<RouteResponse, Infallible>> + Send,
    > {
        let (app, request) = self.into_dispatch();
        match app {
            AppKind::Axum(router) => {
                let answering: RouteFuture =
                    Box::pin(request.dispatch(router).map(Ok));
                Either::Left(answering)
            }
            AppKind::Bypass(service) => {
                Either::Right(request.dispatch(service).map(Ok))
            }
        }
    }
}

trait RequestOwner: Send {
    fn times_its_head(&self) -> bool {
        false
    }

    fn answer(
        self,
        request: Request<RouteBody>,
    ) -> impl Future<Output = RouteResponse> + Send;
}

impl RequestOwner for BypassService {
    fn times_its_head(&self) -> bool {
        BypassService::times_its_head(self)
    }

    fn answer(
        self,
        request: Request<RouteBody>,
    ) -> impl Future<Output = RouteResponse> + Send {
        // Keep the owner beside its existing erased future until the reply or
        // cancellation, without another async state that carries the request.
        self.handle(request).map(move |reply| {
            let _owner = self;
            match reply {
                Ok(response) => response,
                Err(never) => match never {},
            }
        })
    }
}

#[cfg(feature = "axum")]
impl RequestOwner for axum::Router {
    fn answer(
        self,
        request: Request<RouteBody>,
    ) -> impl Future<Output = RouteResponse> + Send {
        self.oneshot(request.map(axum::body::Body::new)).map(
            |reply| match reply {
                Ok(response) => claviron_http::from_axum_response(response),
                Err(never) => match never {},
            },
        )
    }
}

impl DispatchRequest {
    async fn dispatch(
        self,
        owner: impl RequestOwner,
    ) -> RouteResponse {
        let _access = self.access;
        #[cfg(feature = "rate-limit")]
        let _quotas = self.quotas;
        let _exchange = claviron_runtime::activity::ACTIVITY.begin();
        // This request's access record, when `logging.yaml` asks for them:
        // made here because everything a host serves passes here once.
        #[cfg(feature = "access-log")]
        let mut access = crate::access::AccessGuard::begin(
            self.host.host.vhost_config.access_log(),
            &self.host.host.vhost_key.0.to_cow_str(),
            self.request.method(),
            self.request.uri().path(),
            self.request.version(),
            &self.connection,
        );
        // Retain the admitted host and connection even without access logging.
        let context = (self.host, self.connection);
        // The application receives the policy the edge narrowed for it
        // (listener, then host) and narrows it once more with its own
        // block: the edge computes, the runner owns. A plain
        // `claviron_http` value, so no runner depends on the edge for it.
        let mut request = self.request;
        // Borrow the pinned policy: no per-request Arc clone, and absent
        // configuration means no policy selection or CORS/header processing.
        let (early_reply, exchange) = match self.exchange_policy.as_ref()
            .and_then(|policy| policy.select(request.uri().path()))
        {
            Some(pipeline) => {
                let (mut head, body) = request.into_parts();
                let (reply, scope) = pipeline.prepare_head(
                    &mut head, ExchangeState::default(),
                );
                request = Request::from_parts(head, body);
                (reply, Some(scope))
            }
            None => (None, None),
        };
        let server_header = self.server_header;
        request.extensions_mut().insert(self.policy);
        // Taken before admission, so an application's own "request start"
        // includes the time the request waited for a place.
        request.extensions_mut().insert(claviron_http::ext::ReceivedAt(
            std::time::SystemTime::now(),
        ));
        let requests = self.policy.requests;
        // Admission, on every path to the host: a place among the requests it
        // serves at once, waited for up to the request's `queue_timeout`, and
        // the time to the reply's head. The listener's and the host's values,
        // narrowed; the runner behind may narrow them again for its own queue
        // and its own head. Unwritten, neither is kept.
        let Some(_place) = self.admission.enter(requests.queue_timeout).await
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
                Some(policy) => policy.finish(response),
                None => response,
            };
        };
        #[cfg(feature = "access-log")]
        if let Some(access) = access.as_mut() {
            access.admitted();
        }
        // Head-timing ownership is irrelevant when no timeout was declared.
        let head_timeout =
            requests.head_timeout.filter(|_| !owner.times_its_head());
        let answering = async move {
            if let Some(response) = early_reply {
                return response;
            }
            owner.answer(request).await
        };
        // Late here is the server's, not the client's: 504, as a runner's
        // own head timeout answers.
        let Some(response) =
            claviron_http::admission::head_within(head_timeout, answering)
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
                Some(policy) => policy.finish(response),
                None => response,
            };
        };
        let response = with_server_header(response, server_header);
        let response = match exchange {
            Some(policy) => policy.finish(response),
            None => response,
        };
        // The reply's own guard, on every path an application answers
        // through — axum or not: its ceiling and the longest gap between two
        // of its pieces. Where it was a tower layer it guarded axum's replies
        // only, and hid every reply's length from the transport.
        let response = match &context.0.reply {
            Some(policy) => policy.finish(response),
            None => response,
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

#[cfg(test)]
#[path = "../../../tests/service/host/dispatch.rs"]
mod tests;
