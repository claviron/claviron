//! Dispatch to an application process over Orbit shared memory.
//!
//! Prepared native mount dispatch. The request body crosses one Link exchange;
//! no router, middleware state or Axum body is involved.
//!
//! ## What the edge knows
//!
//! The destination fleet and application name. Not a port, not a socket path, not an
//! interpreter, not a virtual environment, and not how many processes answer
//! to it. It reads the lanes serving that name, picks the idlest — a depth it
//! reads rather than a load anybody reports — and writes.
//!
//! ## Streaming, both ways
//!
//! The request body is pumped into the exchange's stream while the reply is
//! being waited for, and the response body is handed to the client as a stream
//! rather than collected. Both matter: the first is what lets an application
//! answer a large upload with 413 without reading it, and the second is what
//! makes SSE and chunked output arrive as they are produced.

use std::sync::Arc;

use claviron_http::{RouteBody, RouteResponse, ServiceResult, boxed_body, err_status};
use futures_util::StreamExt;
use http::Request;

use super::host::{DispatchFuture, DispatchStep};
use claviron_http::ext::{ConnCtx, PathPrefix};
use claviron_http::policy::{
    BodyNeed, HttpPolicy, RequestPolicy, RequestPolicyOverride,
};
use claviron_orbit::transport::{
    Exchange, HttpVersion, TRANSPORT_BUFFER_BYTES, TransportService,
    TransportTarget,
};
use claviron_runtime::{AppState, FleetName};
use http::StatusCode;

mod body;
mod websocket;
#[cfg(feature = "webtransport")]
pub(super) mod webtransport;

#[derive(Clone)]
struct LinkRoute {
    path: Option<PathPrefix>,
    target: Option<TransportTarget>,
    requests: RequestPolicyOverride,
    /// This worker's shutdown, at which an event stream ends its own side
    /// ([`claviron_http::body::EndsAtShutdown`]).
    shutdown: tokio_util::sync::CancellationToken,
}

/// Prepared segment-safe application mounts reached through Link.
/// No declarations means no step. Only a matching mount claims the body;
/// an unavailable matched target returns 503 rather than falling through.
#[derive(Clone)]
pub struct LinkBackend {
    routes: Arc<[LinkRoute]>,
}

impl LinkBackend {
    pub fn new(
        state: &AppState,
        routes: &[claviron_http::config::routes::OrbitRouteConfig],
    ) -> Option<Self> {
        if routes.is_empty() { return None; }
        let mut routes: Vec<_> = routes.iter().map(|route| LinkRoute {
            path: route.path.clone(),
            target: prepare_target(state, route.fleet_name, route.application.clone().into()),
            requests: route.requests,
            shutdown: state.shutdown_token(),
        }).collect();
        // Preserve stable ascending mount insertion followed by LIFO dispatch.
        routes.sort_by(|a, b| match (&a.path, &b.path) {
            (None, None) => std::cmp::Ordering::Equal,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (Some(_), None) => std::cmp::Ordering::Greater,
            (Some(a), Some(b)) => a.as_str().len().cmp(&b.as_str().len()),
        });
        routes.reverse();
        Some(Self { routes: routes.into() })
    }

    fn select(&self, path: &str) -> Option<&LinkRoute> {
        self.routes.iter().find(|route| {
            route.path.as_ref().is_none_or(|prefix| prefix.has_path_segment(path))
        })
    }
}

impl DispatchStep for LinkBackend {
    fn call(&self, request: Request<RouteBody>) -> DispatchFuture {
        let Some(route) = self.select(request.uri().path()) else {
            return DispatchFuture::ready(ServiceResult::Unhandled(request));
        };
        let Some(target) = route.target.clone() else {
            return DispatchFuture::ready(ServiceResult::Handled(
                unavailable("the Link route has no prepared destination"),
            ));
        };
        let requests = route.requests;
        let shutdown = route.shutdown.clone();
        DispatchFuture::new(async move {
            ServiceResult::Handled(dispatch(target, requests, shutdown, request).await)
        })
    }
}

async fn dispatch(
    target: TransportTarget,
    requests: RequestPolicyOverride,
    shutdown: tokio_util::sync::CancellationToken,
    request: Request<RouteBody>,
) -> RouteResponse {
    let handshake = match websocket::Handshake::from_request(&request) {
        Ok(handshake) => handshake,
        Err(status) => return err_status(status),
    };
    let websocket = handshake.is_some();
    let (mut parts, body) = request.into_parts();
    // The edge's policy for this host, narrowed by this application's block.
    let policy = parts
        .extensions
        .get::<HttpPolicy>()
        .map(|policy| policy.requests)
        .unwrap_or(RequestPolicy::unlimited())
        .narrow(&requests);
    // `Arc<ConnCtx>` is what the transports insert; asked for bare, it was
    // never there and the application saw no addresses.
    let connection = parts.extensions.get::<Arc<ConnCtx>>().cloned();
    // What completing an upgrade needs, taken before the parts are spent:
    // Hyper's handle on the connection, the owner of the session that
    // outlives this request, and the key the handshake answers.
    let upgrade = handshake.map(|handshake| websocket::Upgrade {
        on_upgrade: parts.extensions.remove::<hyper::upgrade::OnUpgrade>(),
        sessions: parts
            .extensions
            .get::<claviron_http::session::SessionSpawner>()
            .cloned(),
        handshake,
    });
    let exchange = Exchange {
        // The caller fills these three; they are its own address book.
        correlation: 0,
        origin: 0,
        body: None,
        method: parts.method.as_str().to_owned(),
        scheme: claviron_http::ext::request_scheme(&parts.extensions)
            .map_or_else(
                || "http".to_owned(),
                |scheme| scheme.as_str().to_owned(),
            ),
        version: HttpVersion::from(parts.version),
        // ASGI carries both, so both cross: `path` is decoded and `raw_path`
        // is what arrived. Deciding which the application may see is not this
        // layer's to make.
        path: percent_decoded(parts.uri.path()),
        raw_path: parts.uri.path().as_bytes().to_vec(),
        query: parts.uri.query().unwrap_or_default().as_bytes().to_vec(),
        headers: exchange_headers(&parts.uri, &parts.headers),
        client: connection.as_ref().map(|connection| connection.remote),
        server: connection.as_ref().map(|connection| {
            std::net::SocketAddr::new(
                connection.local_ip,
                connection.local_port,
            )
        }),
        upgrade: websocket,
    };

    // Known empty — a GET, or a declared length of zero — means there is
    // nothing to pump, and the direction can end before the application looks.
    // A WebSocket keeps it open: once accepted, its messages travel on it.
    let body_follows = !http_body::Body::is_end_stream(&body);
    let pending = match target.send(exchange, body_follows || websocket) {
        Ok(pending) => pending,
        // Capacity or absence, never time: nothing has run, so this is a 503
        // and the client may try again.
        Err(error) => {
            tracing::debug!(
                application = target.application(),
                "exchange refused: {error}"
            );
            return unavailable(&error.to_string());
        }
    };

    // The request body travels while the reply is being waited for. A reply
    // that starts before the body finishes is a feature of the slot, not a
    // case to handle.
    let correlation = pending.correlation;
    let (request_body, response_body, reply) = pending.split();
    let (to_application, upload) = if websocket {
        (Some(websocket::Input::new(request_body)), None)
    } else if body_follows {
        let declared = http_body::Body::size_hint(&body).exact();
        let whole = policy.delivery(BodyNeed::Stream, declared, false).takes_whole(declared);
        // The task retains independent upload backpressure. Its abort-on-drop
        // owner stays with this head wait, then with the response body.
        (None, Some(body::upload(body, request_body, whole)))
    } else {
        // Already ended by send; return the unused half immediately.
        drop(request_body);
        (None, None)
    };

    let head = match reply.head().await {
        Ok(head) => head,
        // The application took the request and did not answer: its process is
        // gone. That is a gateway failure, not a refusal to admit.
        Err(error) => {
            tracing::debug!(correlation, "no reply: {error}");
            return gateway_failure();
        }
    };

    if let Some(upgrade) = upgrade
        && head.status == StatusCode::SWITCHING_PROTOCOLS.as_u16()
        && let Some(to_application) = to_application
    {
        return websocket::accept_websocket(
            correlation,
            &head,
            upgrade,
            to_application,
            response_body,
        );
    }

    let mut response = http::Response::builder().status(head.status);
    for (name, value) in &head.headers {
        let (Ok(name), Ok(value)) = (
            http::HeaderName::from_bytes(name),
            http::HeaderValue::from_bytes(value),
        ) else {
            tracing::debug!(
                correlation,
                "reply carries a header this edge cannot send"
            );
            continue;
        };
        response = response.header(name, value);
    }

    // Streamed, not collected: an event held back to fill a buffer is an event
    // delivered late, and a response longer than memory is a response that
    // must not be held in it.
    //
    // Read in pieces as large as the reply says it is, up to one window. The
    // stream's own default is 4 KiB, which cut a 64 KiB body into sixteen
    // reads — sixteen allocations, sixteen body frames, sixteen writes to the
    // client — for bytes that were all in the ring at once. A reply that
    // gives no length, an event stream, reads a window at a time and takes
    // whatever has arrived.
    let piece = reply_length(&head.headers)
        .map_or(TRANSPORT_BUFFER_BYTES, |length| {
            length.clamp(1, TRANSPORT_BUFFER_BYTES)
        });
    let body = boxed_body(http_body_util::StreamBody::new(
        tokio_util::io::ReaderStream::with_capacity(response_body, piece)
            .map(|chunk| chunk.map(http_body::Frame::data)),
    ));
    let body = body::with_upload(body, upload);
    match response.body(body) {
        // An event stream never ends on its own, so the drain would wait out
        // its whole bound. The application process is not ours to stop, so
        // the edge ends its side, as it does for an upstream origin; the
        // application learns it as a client that left.
        Ok(response) => claviron_http::body::ends_at_shutdown(
            response,
            shutdown.cancelled_owned(),
        ),
        Err(error) => {
            tracing::debug!(correlation, "reply could not be rebuilt: {error}");
            gateway_failure()
        }
    }
}

/// The body length a reply declares, if it declares one.
fn reply_length(headers: &[(Vec<u8>, Vec<u8>)]) -> Option<usize> {
    let (_, value) = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(b"content-length"))?;
    std::str::from_utf8(value).ok()?.trim().parse().ok()
}

fn percent_decoded(path: &str) -> String {
    percent_encoding::percent_decode_str(path).decode_utf8_lossy().into_owned()
}

// Admission has already validated authority. Orbit carries a path and headers,
// not a URI authority field, so H2/H3 :authority must cross as Host. Absolute
// HTTP/1 targets likewise retain their authority precedence over Host.
fn exchange_headers(
    uri: &http::Uri,
    headers: &http::HeaderMap,
) -> claviron_orbit::transport::Headers {
    let authority = uri.authority();
    let mut result =
        Vec::with_capacity(headers.len() + usize::from(authority.is_some()));
    if let Some(authority) = authority {
        result.push((b"host".to_vec(), authority.as_str().as_bytes().to_vec()));
    }
    result.extend(headers.iter().filter_map(|(name, value)| {
        if authority.is_some() && name == http::header::HOST {
            return None;
        }
        Some((name.as_str().as_bytes().to_vec(), value.as_bytes().to_vec()))
    }));
    result
}

fn unavailable(reason: &str) -> RouteResponse {
    tracing::debug!("Link dispatch unavailable: {reason}");
    let mut response = err_status(StatusCode::SERVICE_UNAVAILABLE);
    response.headers_mut().insert(http::header::RETRY_AFTER, http::HeaderValue::from_static("1"));
    response
}

fn gateway_failure() -> RouteResponse {
    let mut response = err_status(StatusCode::BAD_GATEWAY);
    response.headers_mut().insert(http::header::CONNECTION, http::HeaderValue::from_static("close"));
    response
}

#[cfg(test)]
#[path = "../../tests/pipeline/link.rs"]
mod tests;

fn prepare_target(
    state: &AppState,
    fleet_name: FleetName,
    application: Arc<str>,
) -> Option<TransportTarget> {
    let transport = state.registry_ref().resolve::<TransportService>();
    match transport.as_ref() {
        None => {
            tracing::error!(
                fleet_name = %fleet_name,
                application = %application,
                "a Link route is configured but the transport is not registered in this build"
            );
            None
        }
        // Join here rather than at the first request. Clients join an
        // destination fleet lazily so that a server routing to no
        // application never creates segments — but this *is* a route to one,
        // and this runs once per worker when its router is built. So the lane
        // is there before the first request, and a fleet this process cannot
        // reach is an error in the log at boot rather than a 503 at an unlucky
        // moment.
        Some(transport) => {
            transport.track_target(fleet_name, &application);
            match transport.client(fleet_name) {
                Ok(client) => Some(client.target(application)),
                Err(error) => {
                    tracing::error!(
                        fleet_name = %fleet_name,
                        application = %application,
                        "a Link route is configured but its fleet cannot be joined: {error}"
                    );
                    None
                }
            }
        }
    }
}
