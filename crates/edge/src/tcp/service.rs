//! Hyper body adaptation and native H2 CONNECT hand-off.

use std::convert::Infallible;
use std::future::{Future, ready};
use std::sync::Arc;

use crate::service::CleartextRequest;
#[cfg(any(feature = "tls", test))]
use crate::transport::RequestArrival;
use crate::transport::{
    AuthContext, H2ConnectBinding, H2ConnectRegistry, HostResponse,
    HostService, PreparedRequest,
};
use claviron_http::body::declared_length_within;
use claviron_http::listener::HttpProtocol;
use claviron_http::session::SessionSpawner;
use claviron_http::{
    RouteFuture, RouteResponse, boxed_body, empty_body, err_status,
};
use claviron_runtime::AppState;
use futures_util::future::Either;
use http::{Method, Request, StatusCode};
use hyper::body::Incoming;
#[cfg(any(feature = "tls", test))]
use hyper::service::Service;
use tracing::{debug, error};

mod body;

#[derive(Clone)]
#[cfg(any(feature = "tls", test))]
pub(crate) struct RequestService {
    host: HostService,
    dispatch: RequestDispatch,
}

/// Native capabilities retained by one accepted connection, independently of
/// the host selected for an individual cleartext request.
#[derive(Clone)]
pub(crate) struct RequestDispatch {
    state: AppState,
    sessions: SessionSpawner,
    h2_connect: Option<Arc<H2ConnectRegistry>>,
}

#[cfg(any(feature = "tls", test))]
impl RequestService {
    pub(crate) fn new(
        host: HostService,
        state: AppState,
        sessions: SessionSpawner,
    ) -> Self {
        let dispatch = RequestDispatch::new(host.protocol(), state, sessions);
        Self { host, dispatch }
    }

    pub(crate) fn into_service(
        self
    ) -> impl Service<
        Request<Incoming>,
        Response = RouteResponse,
        Error = Infallible,
        Future: Send,
    > {
        hyper::service::service_fn(move |request| {
            self.dispatch.dispatch(
                self.host.prepare(request, RequestArrival::Established),
            )
        })
    }
}

impl RequestDispatch {
    pub(crate) fn state(&self) -> &AppState {
        &self.state
    }

    pub(crate) fn new(
        protocol: Option<HttpProtocol>,
        state: AppState,
        sessions: SessionSpawner,
    ) -> Self {
        let h2_connect = if protocol == Some(HttpProtocol::H2) {
            state.registry_ref().resolve::<H2ConnectRegistry>()
        } else {
            None
        };
        Self { state, sessions, h2_connect }
    }
    pub(crate) fn call_cleartext(
        &self,
        host: HostService,
        request: CleartextRequest<'_, Incoming>,
    ) -> impl Future<Output = Result<RouteResponse, Infallible>> + Send + use<>
    {
        self.dispatch(host.prepare_cleartext(request))
    }

    fn dispatch(
        &self,
        prepared: Result<PreparedRequest<Incoming>, HostResponse>,
    ) -> impl Future<Output = Result<RouteResponse, Infallible>> + Send + use<>
    {
        let mut prepared = match prepared {
            Ok(prepared) => prepared,
            Err(reply) => {
                return Either::Left(ready(Ok(reply
                    .into_response()
                    .map(|()| empty_body()))));
            }
        };
        let tunnelling = prepared.request().method() == Method::CONNECT
            || prepared.request().headers().contains_key(http::header::UPGRADE);
        if tunnelling {
            prepared.extensions_mut().insert(self.sessions.clone());
        }

        // Refused before dispatch while a status line is still available to
        // send. A body with no declared length, or one that lies, is caught by
        // the running count instead — by then the response has started and the
        // read simply fails.
        if !tunnelling
            && declared_length_within(
                prepared.request().headers(),
                prepared.max_body_size(),
            )
            .is_err()
        {
            return Either::Left(ready(Ok(err_status(
                StatusCode::PAYLOAD_TOO_LARGE,
            ))));
        }
        if prepared.connection().proto == Some(HttpProtocol::H2)
            && prepared.request().method() == Method::CONNECT
            && let Some(binding) =
                self.h2_connect.as_ref().and_then(|registry| {
                    registry.resolve(
                        &prepared.host_key().0,
                        prepared.request().uri().path(),
                    )
                })
        {
            let auth = prepared.auth_context();
            #[cfg(feature = "rate-limit")]
            let quota_snapshot = prepared.quota_snapshot();
            let request = prepared.map_body(boxed_body).into_request();
            let accepting =
                handle_h2_connect(binding, self.state.clone(), auth, request);
            let accepting: RouteFuture = Box::pin(async move {
                let response = accepting.await?;
                #[cfg(feature = "rate-limit")]
                let response = {
                    let mut response = response;
                    claviron_http::rate_limit::apply_headers(
                        quota_snapshot,
                        &mut response,
                    );
                    response
                };
                Ok(response)
            });
            return Either::Right(Either::Left(accepting));
        }
        let prepared = if tunnelling {
            // The native body carries the tunnel, even when it currently
            // reports no data. Do not apply ordinary request-body policy.
            prepared.map_body(boxed_body)
        } else {
            let max_body_size = prepared.max_body_size();
            let body_read_timeout = prepared.requests().body_read_timeout;
            prepared.map_body(|incoming| {
                body::adapt(incoming, max_body_size, body_read_timeout)
            })
        };
        Either::Right(Either::Right(prepared.into_tcp_future()))
    }
}

async fn handle_h2_connect(
    binding: H2ConnectBinding,
    state: AppState,
    context: AuthContext,
    request: Request<claviron_http::RouteBody>,
) -> Result<RouteResponse, Infallible> {
    let H2ConnectBinding::Handler(handler) = binding;
    let name = handler.name();
    let host = context.host.clone();
    let path = context.path.clone();
    let protocol = match request.extensions().get::<hyper::ext::Protocol>() {
        Some(value) if value.as_str().eq_ignore_ascii_case("websocket") => {
            "websocket"
        }
        Some(value) if value.as_str().eq_ignore_ascii_case("webtransport") => {
            "webtransport"
        }
        Some(_) => "other",
        None => "missing",
    };
    // Paths only: query strings, headers and authentication payloads may
    // contain credentials and are never part of these handshake diagnostics.
    debug!(host = %host, path = %path, handler = name, protocol,
        "H2 CONNECT received");
    let authenticated = match handler.accept(&state, &context).await {
        Ok(accepted) => accepted.into_authenticated(),
        Err(reject) => {
            let reason = reject.reason();
            let response = reject.into_response();
            debug!(host = %host, path = %path, handler = name, protocol, reason,
                status = response.status().as_u16(), "H2 CONNECT rejected");
            return Ok(response);
        }
    };
    let _session = claviron_runtime::activity::ACTIVITY.open_session();
    match handler.handle(state, context, authenticated, request).await {
        Ok(response) => {
            let status = response.status();
            if status.is_success() {
                debug!(host = %host, path = %path, handler = name, protocol,
                    status = status.as_u16(), "H2 CONNECT accepted");
            } else {
                debug!(host = %host, path = %path, handler = name, protocol,
                    reason = "handler_response",
                    status = status.as_u16(), "H2 CONNECT rejected");
            }
            Ok(response)
        }
        Err(error) => {
            error!(handler = name, %error, "H2 CONNECT handler failed");
            debug!(host = %host, path = %path, handler = name, protocol,
                reason = "handler_error",
                status = StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                "H2 CONNECT rejected");
            Ok(err_status(StatusCode::INTERNAL_SERVER_ERROR))
        }
    }
}

#[cfg(test)]
#[path = "../../tests/tcp/unit/service.rs"]
mod tests;
