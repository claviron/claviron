//! Native ordinary-request execution around one already selected application.

use std::sync::Arc;
use std::future::Future;

use claviron_http::{RouteBody, RouteResponse};
use claviron_http::admission::Admission;
use claviron_http::config::HttpPolicy;
use claviron_http::ext::ConnCtx;
use http::{Request, StatusCode};

use super::{ApplicationHandler, ExchangeState, HostPolicy};
use crate::pipeline::host::ExchangePolicy;
use crate::pipeline::host::PolicySnapshot;

/// One admitted exchange and its retained binding/application resources.
/// The selected owner stays outside this value, so another owner's concrete
/// answering future cannot enlarge it.
pub(crate) struct HostExecution {
    pub(crate) request: Request<RouteBody>,
    pub(crate) host: Arc<HostPolicy>,
    pub(crate) connection: Arc<ConnCtx>,
    pub(crate) access: PolicySnapshot,
    pub(crate) policy: HttpPolicy,
    pub(crate) admission: Admission,
    pub(crate) exchange_policy: Option<Arc<ExchangePolicy>>,
    pub(crate) server_header: Option<http::HeaderValue>,
    #[cfg(feature = "rate-limit")]
    pub(crate) quotas: Option<Arc<crate::HostQuotas>>,
    #[cfg(feature = "rate-limit")]
    pub(crate) quota_snapshot: Option<claviron_http::rate_limit::Snapshot>,
}

impl HostExecution {
    pub(crate) fn dispatch<H: ApplicationHandler>(
        self,
        owner: H,
    ) -> impl Future<Output = RouteResponse> + Send {
        // Resolve concrete terminal methods before constructing async state.
        // Their invocation stays deferred to the original admission/head phase.
        self.execute(owner, H::times_its_head, H::call)
    }

    async fn execute<H, T, C, F>(
        self,
        owner: H,
        timing_owner: T,
        call: C,
    ) -> RouteResponse
    where
        H: Send,
        T: FnOnce(&H) -> bool + Send,
        C: FnOnce(H, Request<RouteBody>) -> F + Send,
        F: Future<Output = RouteResponse> + Send,
    {
        let _access = self.access;
        #[cfg(feature = "rate-limit")]
        let _quotas = self.quotas;
        let _exchange = claviron_runtime::activity::ACTIVITY.begin();
        // This request's access record, when `logging.yaml` asks for them:
        // made here because everything a host serves passes here once.
        #[cfg(feature = "access-log")]
        let mut access = self.host.access_log.as_ref()
            .map(|policy| policy.begin(&self.request, &self.connection))
            .unwrap_or_default();
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
        // Admission, on every path to the host: a place among the requests it
        // serves at once, waited for up to the request's `queue_timeout`, and
        // the time to the reply's head. The listener's and the host's values,
        // narrowed; the runner behind may narrow them again for its own queue
        // and its own head. Unwritten, neither is kept.
        let Some(_place) = context.0.admission_policy.enter(&self.admission).await
        else {
            #[cfg(feature = "access-log")]
            access.replied(StatusCode::SERVICE_UNAVAILABLE);
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
        access.admitted();
        // Head-timing ownership is irrelevant when no timeout was declared.
        let head_timeout = context.0.admission_policy
            .head_timeout_for(|| timing_owner(&owner));
        let answering = async move {
            if let Some(response) = early_reply {
                return response;
            }
            call(owner, request).await
        };
        // Late here is the server's, not the client's: 504, as a runner's
        // own head timeout answers.
        let Some(response) =
            claviron_http::admission::head_within(head_timeout, answering)
                .await
        else {
            #[cfg(feature = "access-log")]
            access.replied(StatusCode::GATEWAY_TIMEOUT);
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
        let response = access.finish(response);
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
