//! Dispatch to an application process over Orbit shared memory.
//!
//! Prepared native mount dispatch. The request body crosses one Link exchange;
//! no router, middleware state or Axum body is involved.
//!
//! ## What the edge knows
//!
//! The destination fleet and application name. Not a port, not a socket path, not an
//! interpreter, not a virtual environment, and not how many processes answer
//! to it. It reads the lanes serving that name in shared memory, reserves a
//! receiver — its own home receiver first when receivers declare a bound —
//! and writes. See `ApplicationClient::choose` for why work is concentrated
//! rather than spread.
//!
//! ## Whole or streamed, both ways
//!
//! A body the policy holds whole is collected first and carried in the frame
//! with the request, and a reply whose size the application knew comes back
//! in its frame: one crossing each way, one wake each way while the far side
//! is parked. A body that streams is pumped into the exchange's stream while
//! the reply is being waited for, and a streamed reply is handed to the client
//! as it arrives rather than collected. Both matter: the first is what lets an
//! application answer a large upload with 413 without reading it, and the
//! second is what makes SSE and chunked output arrive as they are produced.

use std::sync::Arc;

use claviron_http::{
    RouteBody, RouteResponse, ServiceResult, boxed_body, err_status,
};
use futures_util::StreamExt;
use http::Request;

use crate::{DispatchCall, DispatchStep};
use claviron_http::ext::{ConnCtx, PathPrefix};
use claviron_http::policy::{BodyNeed, RequestPolicy, RequestPolicyOverride};
use claviron_http::socket::Offer;
use claviron_orbit::link::application::{
    ApplicationService, ApplicationTarget, Error, Exchange, ExchangeBody,
    HttpVersion,
};
use claviron_runtime::{AppState, FleetName};
use http::StatusCode;

mod body;
mod websocket;

#[derive(Clone)]
struct LinkRoute {
    path: Option<PathPrefix>,
    destination: Arc<LinkDestination>,
    requests: RequestPolicyOverride,
}

/// Prepared segment-safe application mounts reached through Link.
/// No declarations means no step. Only a matching mount claims the body;
/// an unavailable matched target returns 503 rather than falling through.
#[derive(Clone)]
pub struct LinkBackend {
    routes: Arc<[LinkRoute]>,
    shutdown: tokio_util::sync::CancellationToken,
    /// The transport's `h1.upgrade`. HTTP/2 has its own, enforced before a
    /// request exists: extended CONNECT is not advertised without it.
    h1_upgrade: bool,
}

impl LinkBackend {
    pub fn new(
        state: &AppState,
        routes: &[claviron_http::config::routes::OrbitRouteConfig],
        prefix: Option<&PathPrefix>,
        requests: RequestPolicyOverride,
        h1_upgrade: bool,
    ) -> Option<Self> {
        if routes.is_empty() {
            return None;
        }
        let mut routes: Vec<_> = routes
            .iter()
            .map(|route| LinkRoute {
                path: prefix.cloned(),
                destination: Arc::new(LinkDestination::new(
                    state,
                    route.fleet_name,
                    route.application.clone().into(),
                )),
                requests,
            })
            .collect();
        // Preserve stable ascending mount insertion followed by LIFO dispatch.
        routes.sort_by(|a, b| match (&a.path, &b.path) {
            (None, None) => std::cmp::Ordering::Equal,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (Some(_), None) => std::cmp::Ordering::Greater,
            (Some(a), Some(b)) => a.as_str().len().cmp(&b.as_str().len()),
        });
        routes.reverse();
        Some(Self {
            routes: routes.into(),
            shutdown: state.shutdown_token(),
            h1_upgrade,
        })
    }

    fn select(
        &self,
        path: &str,
    ) -> Option<&LinkRoute> {
        self.routes.iter().find(|route| {
            route
                .path
                .as_ref()
                .is_none_or(|prefix| prefix.has_path_segment(path))
        })
    }
}

impl crate::BackendHandler for LinkBackend {
    fn contract(&self) -> crate::BackendContract {
        crate::BackendContract {
            identity: crate::BackendIdentity::None,
            reply: crate::BackendReply::ExternalEventStream(
                self.shutdown.clone(),
            ),
        }
    }
}

impl DispatchStep for LinkBackend {
    fn call(
        &self,
        request: Request<RouteBody>,
        context: Option<crate::HostContext<'_>>,
    ) -> DispatchCall {
        let Some(route) = self.select(request.uri().path()) else {
            return DispatchCall::ready(ServiceResult::Unhandled(request));
        };
        if !self.h1_upgrade
            && claviron_http::upgrade::is_h1_header_upgrade(
                &request,
                "websocket",
            )
        {
            tracing::debug!(
                path = request.uri().path(),
                "a WebSocket upgrade was refused: this transport has no h1.upgrade"
            );
            return DispatchCall::ready(ServiceResult::Handled(err_status(
                StatusCode::FORBIDDEN,
            )));
        }
        // Taken while the context is lent: the exchange outlives this call.
        // The edge's policy for this scope, narrowed by this application's
        // block; with no accepted transport behind the request there is no
        // edge policy to narrow.
        let policy = context
            .and_then(|context| context.policy())
            .map_or(RequestPolicy::unlimited(), |policy| policy.requests)
            .narrow(&route.requests);
        let Some(target) = route.destination.target() else {
            return DispatchCall::ready(ServiceResult::Handled(unavailable(
                "the Link route's destination has no owner yet",
                &policy,
            )));
        };
        let connection = context.map(|context| context.connection.clone());
        DispatchCall::new(async move {
            ServiceResult::Handled(
                dispatch(target, policy, connection, request).await,
            )
        })
    }
}

async fn dispatch(
    target: ApplicationTarget,
    policy: RequestPolicy,
    connection: Option<Arc<ConnCtx>>,
    request: Request<RouteBody>,
) -> RouteResponse {
    // The destination's declared stream window, not this build's ceiling.
    let window = target.stream_buffer_bytes();
    // Taken before the application hears of it: once this owner has the attempt
    // it alone can answer it, and an attempt someone else holds is not its own.
    let offer = request.extensions().get::<Offer>().cloned();
    // A WebTransport session is answered by its own adapter, stream by stream.
    if let Some(offer) = &offer
        && offer.info().kind == claviron_http::socket::SocketKind::WebTransport
    {
        return match offer.claim() {
            Some(claim) => claim.reach(),
            None => err_status(StatusCode::FORBIDDEN),
        };
    }
    let claim = match &offer {
        Some(offer) => match offer.claim() {
            Some(claim) => Some(claim),
            None => return err_status(StatusCode::FORBIDDEN),
        },
        None => None,
    };
    let websocket = claim.is_some();
    let (parts, body) = request.into_parts();
    let sessions = parts
        .extensions
        .get::<claviron_http::session::SessionSpawner>()
        .cloned();
    // Known empty — a GET, or a declared length of zero — means there is
    // nothing to pump, and the direction can end before the application looks.
    // A WebSocket keeps it open: once accepted, its messages travel on it. A
    // body the policy holds whole is collected here, before the frame, so it
    // can travel in it.
    let declared = http_body::Body::size_hint(&body).exact();
    let (request_body, streamed) = if websocket {
        (ExchangeBody::Stream, None)
    } else if http_body::Body::is_end_stream(&body) {
        (ExchangeBody::Empty, None)
    } else if policy
        .delivery(BodyNeed::Stream, declared, false)
        .takes_whole(declared)
    {
        match http_body_util::BodyExt::collect(body).await {
            Ok(collected) => (ExchangeBody::Inline(collected.to_bytes()), None),
            Err(error) => return body_failure(&error),
        }
    } else {
        (ExchangeBody::Stream, Some(body))
    };
    let exchange = Exchange {
        // The caller fills these three; they are its own address book.
        correlation: 0,
        origin: 0,
        stream: None,
        body: request_body,
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

    // One budget for the whole wait before the application starts: the
    // sender's own stream slot, then the receiver's queue.
    let deadline = policy
        .queue_timeout
        .map(|timeout| tokio::time::Instant::now() + timeout);
    let sending = target.send_wait(exchange);
    let admitted = match deadline {
        Some(deadline) => {
            match tokio::time::timeout_at(deadline, sending).await {
                Ok(result) => result,
                Err(_) => {
                    return unavailable(
                        "Link admission exceeded requests.queue_timeout",
                        &policy,
                    );
                }
            }
        }
        None => sending.await,
    };
    let pending = match admitted {
        Ok(pending) => pending,
        // Capacity or absence, never time: nothing has run, so this is a 503
        // and the client may try again.
        Err(error) => {
            tracing::debug!(
                application = target.application(),
                "exchange refused: {error}"
            );
            return unavailable(&error.to_string(), &policy);
        }
    };

    // The request body travels while the reply is being waited for. A reply
    // that starts before the body finishes is a feature of the slot, not a
    // case to handle.
    let correlation = pending.correlation;
    let (request_body, response_body, reply, unsent) = pending.split();
    let (to_application, upload) = if websocket {
        (Some(websocket::Input::new(request_body)), None)
    } else if let Some(body) = streamed {
        // The task retains independent upload backpressure. Its abort-on-drop
        // owner stays with this head wait, then with the response body.
        (None, Some(body::upload(body, request_body)))
    } else if let Some(rest) = unsent {
        // Held whole but larger than the frame and the stream's window: the
        // rest follows the head on the stream.
        (None, Some(body::upload_rest(rest, request_body)))
    } else {
        // Already ended by send; return the unused half immediately.
        drop(request_body);
        (None, None)
    };

    // A bounded receiver may hold the exchange in its queue. If the budget
    // ends first, take it back: when that wins it never ran, so this is a
    // 503; when the receiver started it first, its reply is still owed.
    let withdrawal = reply.withdrawal();
    let waiting = reply.head();
    tokio::pin!(waiting);
    let replied = match (deadline, withdrawal) {
        (Some(deadline), Some(withdrawal)) => {
            tokio::select! {
                biased;
                replied = &mut waiting => replied,
                () = tokio::time::sleep_until(deadline) => {
                    if withdrawal.withdraw() {
                        return unavailable(
                            "Link exchange waited in the receiver's queue past requests.queue_timeout",
                            &policy,
                        );
                    }
                    waiting.await
                }
            }
        }
        _ => waiting.await,
    };
    let head = match replied {
        Ok(head) => head,
        // The application took the request and did not answer: its process is
        // gone. That is a gateway failure, not a refusal to admit.
        Err(error) => {
            tracing::debug!(correlation, "no reply: {error}");
            return gateway_failure();
        }
    };

    if let Some(claim) = claim
        && head.status == StatusCode::SWITCHING_PROTOCOLS.as_u16()
        && let Some(to_application) = to_application
    {
        return websocket::accept_websocket(
            correlation,
            &head,
            claim,
            sessions,
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

    // In the frame: the reply is whole, and the stream carried nothing.
    let response_body = match head.body {
        ExchangeBody::Stream => response_body,
        ExchangeBody::Empty => {
            drop(response_body);
            let body = body::with_upload(claviron_http::empty_body(), upload);
            return finish(response, body, correlation);
        }
        ExchangeBody::Inline(bytes) => {
            drop(response_body);
            let body =
                body::with_upload(claviron_http::bytes_body(bytes), upload);
            return finish(response, body, correlation);
        }
    };

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
        .map_or(window, |length| length.clamp(1, window));
    let body = boxed_body(http_body_util::StreamBody::new(
        tokio_util::io::ReaderStream::with_capacity(response_body, piece)
            .map(|chunk| chunk.map(http_body::Frame::data)),
    ));
    let body = body::with_upload(body, upload);
    finish(response, body, correlation)
}

fn finish(
    response: http::response::Builder,
    body: RouteBody,
    correlation: u64,
) -> RouteResponse {
    match response.body(body) {
        Ok(response) => response,
        Err(error) => {
            tracing::debug!(correlation, "reply could not be rebuilt: {error}");
            gateway_failure()
        }
    }
}

/// A body held whole that could not be read to its end. Too large or stalled
/// is the client's, and says so; anything else is a client that went away,
/// which nobody will read an answer to.
fn body_failure(error: &claviron_http::RouteBodyError) -> RouteResponse {
    use claviron_http::body::BodyLimit;
    match error.downcast_ref::<BodyLimit>() {
        Some(BodyLimit::TooLarge { .. }) => {
            err_status(StatusCode::PAYLOAD_TOO_LARGE)
        }
        Some(BodyLimit::Stalled { .. }) => {
            err_status(StatusCode::REQUEST_TIMEOUT)
        }
        None => err_status(StatusCode::BAD_REQUEST),
    }
}

/// The body length a reply declares, if it declares one.
fn reply_length(headers: &[(Vec<u8>, Vec<u8>)]) -> Option<usize> {
    let (_, value) = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(b"content-length"))?;
    std::str::from_utf8(value).ok()?.trim().parse().ok()
}

/// The path as the application reads it.
pub fn percent_decoded(path: &str) -> String {
    percent_encoding::percent_decode_str(path).decode_utf8_lossy().into_owned()
}

// Admission has already validated authority. Orbit carries a path and headers,
// not a URI authority field, so H2/H3 :authority must cross as Host. Absolute
// HTTP/1 targets likewise retain their authority precedence over Host.
/// The request's headers as they cross a Link exchange.
pub fn exchange_headers(
    uri: &http::Uri,
    headers: &http::HeaderMap,
) -> claviron_orbit::link::application::Headers {
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

/// Nothing reached the application, so the client may try again: a 503 with
/// the policy's `Retry-After`, in whole seconds rounded up, or none when the
/// policy declares no wait at all.
fn unavailable(
    reason: &str,
    policy: &RequestPolicy,
) -> RouteResponse {
    tracing::debug!("Link dispatch unavailable: {reason}");
    let mut response = err_status(StatusCode::SERVICE_UNAVAILABLE);
    if let Some(wait) = policy.retry_after() {
        let seconds = wait.as_secs() + u64::from(wait.subsec_nanos() > 0);
        response
            .headers_mut()
            .insert(http::header::RETRY_AFTER, seconds.into());
    }
    response
}

fn gateway_failure() -> RouteResponse {
    let mut response = err_status(StatusCode::BAD_GATEWAY);
    response.headers_mut().insert(
        http::header::CONNECTION,
        http::HeaderValue::from_static("close"),
    );
    response
}

#[cfg(test)]
#[path = "../tests/link.rs"]
mod tests;

/// The destination of one route. Its owner creates the destination fleet and
/// declares its shape; this caller adopts both when the owner exists and
/// keeps the prepared target from then on. While the owner is absent each
/// request tries again and answers 503: the request is the event, so no timer
/// or poll waits for the owner.
pub struct LinkDestination {
    service: Option<Arc<ApplicationService>>,
    fleet_name: FleetName,
    application: Arc<str>,
    target: std::sync::OnceLock<ApplicationTarget>,
}

impl LinkDestination {
    /// Prepared once per worker when its router is built. A destination that
    /// already exists is joined here, so its lane is there before the first
    /// request; one that does not is reported and adopted later.
    pub fn new(
        state: &AppState,
        fleet_name: FleetName,
        application: Arc<str>,
    ) -> Self {
        let service = state.registry_ref().resolve::<ApplicationService>();
        match &service {
            None => tracing::error!(
                fleet_name = %fleet_name,
                application = %application,
                "a Link route is configured but the application link is not registered in this build"
            ),
            Some(service) => service.track_target(fleet_name, &application),
        }
        let destination = Self {
            service,
            fleet_name,
            application,
            target: std::sync::OnceLock::new(),
        };
        destination.adopt(true);
        destination
    }

    /// A destination with no application service behind it, which answers
    /// 503 for every request.
    #[cfg(test)]
    fn unavailable() -> Self {
        Self {
            service: None,
            fleet_name: FleetName::new("example"),
            application: Arc::from("example"),
            target: std::sync::OnceLock::new(),
        }
    }

    /// The prepared target, adopting the destination first if its owner has
    /// appeared since the last attempt.
    pub fn target(&self) -> Option<ApplicationTarget> {
        if let Some(target) = self.target.get() {
            return Some(target.clone());
        }
        self.adopt(false)
    }

    fn adopt(
        &self,
        boot: bool,
    ) -> Option<ApplicationTarget> {
        let service = self.service.as_ref()?;
        match service.client(self.fleet_name) {
            Ok(client) => {
                let target = self.target.get_or_init(|| {
                    client.target(Arc::clone(&self.application))
                });
                Some(target.clone())
            }
            Err(Error::DestinationAbsent(_)) => {
                if boot {
                    tracing::info!(
                        fleet_name = %self.fleet_name,
                        application = %self.application,
                        "the Link route's destination has no owner yet; it is adopted once the owner creates it"
                    );
                }
                None
            }
            Err(error) if boot => {
                tracing::error!(
                    fleet_name = %self.fleet_name,
                    application = %self.application,
                    "a Link route is configured but its fleet cannot be joined: {error}"
                );
                None
            }
            Err(error) => {
                tracing::debug!(
                    fleet_name = %self.fleet_name,
                    application = %self.application,
                    "the Link route's destination cannot be joined: {error}"
                );
                None
            }
        }
    }
}
