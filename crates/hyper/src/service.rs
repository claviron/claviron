//! Hyper body adaptation and native H2 CONNECT hand-off.

use std::convert::Infallible;
use std::sync::Arc;

use claviron_edge::deny::DenyCheckedRequest;
use claviron_edge::transport::{
    AuthContext, H2ConnectBinding, H2ConnectRegistry, HostResponse,
    HostService, PreparedRequest, RequestArrival,
};
use claviron_http::body::{GuardedBody, declared_length_within};
use claviron_http::listener::HttpProtocol;
use claviron_http::session::SessionSpawner;
use claviron_http::{
    RouteFuture, RouteResponse, boxed_body, empty_body, err_status,
};
use claviron_runtime::AppState;
use http::{Method, Request, StatusCode};
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::service::Service;
use tracing::{debug, error};

#[derive(Clone)]
pub(crate) struct RequestService {
    host: HostService,
    state: AppState,
    sessions: SessionSpawner,
    h2_connect: Option<Arc<H2ConnectRegistry>>,
}

impl RequestService {
    pub(crate) fn new(
        host: HostService,
        state: AppState,
        sessions: SessionSpawner,
    ) -> Self {
        let h2_connect = state.registry_ref().resolve::<H2ConnectRegistry>();
        Self { host, state, sessions, h2_connect }
    }
    pub(crate) fn call_checked(
        &self,
        request: DenyCheckedRequest<'_, Incoming>,
    ) -> RouteFuture {
        self.dispatch(
            self.host.prepare_checked(request, RequestArrival::Established),
        )
    }

    fn dispatch(
        &self,
        prepared: Result<PreparedRequest<Incoming>, HostResponse>,
    ) -> RouteFuture {
        let mut prepared = match prepared {
            Ok(prepared) => prepared,
            Err(reply) => {
                return Box::pin(async move {
                    Ok(reply.into_response().map(|()| empty_body()))
                });
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
            return Box::pin(async move {
                Ok(err_status(StatusCode::PAYLOAD_TOO_LARGE))
            });
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
            let request = prepared.map_body(boxed_body).into_request();
            return Box::pin(handle_h2_connect(
                binding,
                self.state.clone(),
                auth,
                request,
            ));
        }
        // Not on a tunnel: after CONNECT or an upgrade the body is the tunnel
        // itself, and a size cap on it would end the connection it exists to
        // carry.
        if tunnelling {
            return Box::pin(async move {
                Ok(prepared.map_body(boxed_body).dispatch().await)
            });
        }

        // Both from the listener's policy as this host narrowed it; the read
        // gap is each request body's, not the connection's.
        Box::pin(async move {
            let max_body_size = prepared.max_body_size();
            let body_read_timeout = prepared.requests().body_read_timeout;
            Ok(prepared
                .map_body(|body| {
                    GuardedBody::new(body, max_body_size, body_read_timeout)
                        .boxed_unsync()
                })
                .dispatch()
                .await)
        })
    }
}

impl Service<Request<Incoming>> for RequestService {
    type Response = RouteResponse;
    type Error = Infallible;
    type Future = RouteFuture;

    fn call(
        &self,
        request: Request<Incoming>,
    ) -> Self::Future {
        self.dispatch(self.host.prepare(request, RequestArrival::Established))
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
#[path = "../tests/unit/service.rs"]
mod tests;
