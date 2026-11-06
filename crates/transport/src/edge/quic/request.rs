//! Native H3 body adaptation, response framing and stream completion.
//! Host admission/identity/application dispatch live in claviron-edge.

use std::pin::Pin;

use super::error::{log_stream_error, request_interrupted};
use bytes::{Buf, Bytes};
use claviron_edge::config::EarlyDataPolicy;
use claviron_edge::transport::{HostService, RequestArrival, RequestRejection};
use claviron_http::boxed_body;
use futures_util::future::poll_fn;
use futures_util::stream::try_unfold;
use http::{HeaderMap, Request, Response, StatusCode};
use http_body::{Body as HttpBody, Frame};
use http_body_util::StreamBody;
use tracing::{debug, error};

pub(crate) type H3BidiStream<Q> =
    <Q as h3::quic::OpenStreams<Bytes>>::BidiStream;
pub(crate) type H3RequestStream<Q> =
    h3::server::RequestStream<H3BidiStream<Q>, Bytes>;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum BodyErrorKind {
    Malformed,
    Interrupted,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct RequestBodyError {
    kind: BodyErrorKind,
}

impl RequestBodyError {
    const fn malformed() -> Self {
        Self { kind: BodyErrorKind::Malformed }
    }

    fn from_stream_error(error: &h3::error::StreamError) -> Self {
        if request_interrupted(error) {
            Self { kind: BodyErrorKind::Interrupted }
        } else {
            Self::malformed()
        }
    }

    const fn response_status(self) -> Option<StatusCode> {
        match self.kind {
            BodyErrorKind::Malformed => Some(StatusCode::BAD_REQUEST),
            BodyErrorKind::Interrupted => None,
        }
    }
}

impl std::fmt::Display for RequestBodyError {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        match self.kind {
            BodyErrorKind::Malformed => {
                f.write_str("invalid HTTP/3 request body")
            }
            BodyErrorKind::Interrupted => {
                f.write_str("HTTP/3 request transport ended")
            }
        }
    }
}

impl std::error::Error for RequestBodyError {}

async fn next_frame<B: HttpBody + Unpin>(
    body: &mut B
) -> Option<Result<Frame<B::Data>, B::Error>> {
    poll_fn(|cx| HttpBody::poll_frame(Pin::new(body), cx)).await
}

fn parse_content_length(
    headers: &HeaderMap
) -> Result<Option<usize>, RequestBodyError> {
    let Some(value) = headers.get(http::header::CONTENT_LENGTH) else {
        return Ok(None);
    };

    let value = value.to_str().map_err(|_| RequestBodyError::malformed())?;
    let len =
        value.parse::<usize>().map_err(|_| RequestBodyError::malformed())?;
    Ok(Some(len))
}

/// Fields that describe an HTTP/1 connection rather than the message on it.
/// QUIC gives each request its own stream, so none of them has anything left
/// to describe, and RFC 9114 §4.2 requires a message carrying one to be
/// treated as malformed.
///
/// The list is h2's, verbatim (`h2::frame::headers`, RFC 9113 §8.2.2). The two
/// transports inherited the same rule from the same place, and a header that
/// is malformed over H2 should not be merely unusual over H3.
const CONNECTION_SPECIFIC_FIELDS: [http::HeaderName; 5] = [
    http::header::CONNECTION,
    http::header::TRANSFER_ENCODING,
    http::header::UPGRADE,
    http::HeaderName::from_static("keep-alive"),
    http::HeaderName::from_static("proxy-connection"),
];

/// Validate how this request claims to be framed, before any of it is read.
///
/// Worth the lookups because this is the one ingress with no parser in front
/// of it. H1 has hyper, which since 1.11.1 drops a `Content-Length` that a
/// `Transfer-Encoding` cancels — either order — and closes the connection
/// afterwards; H2 has the h2 crate, which rejects the whole set below as
/// malformed. Over H3 neither is in the path: h3 validates pseudo-headers and
/// hands us the rest of the map as sent, so a `Transfer-Encoding` would travel
/// intact to an upstream free to read the framing the other way.
fn checked_request_framing(
    headers: &HeaderMap
) -> Result<(), RequestBodyError> {
    if CONNECTION_SPECIFIC_FIELDS
        .iter()
        .any(|field| headers.contains_key(field))
    {
        return Err(RequestBodyError::malformed());
    }

    // The one connection-level field H3 keeps, and only for this value.
    if headers.get(http::header::TE).is_some_and(|value| value != "trailers") {
        return Err(RequestBodyError::malformed());
    }

    parse_content_length(headers)?;
    Ok(())
}

async fn finish_response_stream<S>(
    stream: &mut h3::server::RequestStream<S, Bytes>,
    context: &'static str,
) where
    S: h3::quic::SendStream<Bytes>,
{
    if let Err(err) = stream.finish().await {
        log_stream_error(&err, context);
    }
}

/// Short-circuit response: send a status-only reply (empty body) and
/// finish the stream. Used by guard rejections (413, 421, 400, …) where
/// we never enter the inner service.
pub(crate) async fn send_status_response<S>(
    stream: &mut h3::server::RequestStream<S, Bytes>,
    status: StatusCode,
) where
    S: h3::quic::SendStream<Bytes>,
{
    let mut response = Response::new(());
    *response.status_mut() = status;
    send_head_response(stream, response).await;
}

async fn send_head_response<S>(
    stream: &mut h3::server::RequestStream<S, Bytes>,
    response: Response<()>,
) where
    S: h3::quic::SendStream<Bytes>,
{
    let status = response.status();
    if let Err(err) = stream.send_response(response).await {
        log_stream_error(&err, &format!("send status response {status}"));
        return;
    }

    finish_response_stream(stream, "sending status response").await;
}

/// Adapt native H3 data and trailers without buffering or request policy.
/// HostPipeline guards the resulting body only when bounds were declared.
fn into_request_body_stream<S>(
    recv_stream: h3::server::RequestStream<S, Bytes>
) -> impl futures_util::Stream<Item = Result<Frame<Bytes>, RequestBodyError>>
+ Send
+ 'static
where
    S: h3::quic::RecvStream + Send + 'static,
{
    try_unfold(
        (recv_stream, false),
        move |(mut recv_stream, trailers_sent): (
            h3::server::RequestStream<S, Bytes>,
            bool,
        )| async move {
            if trailers_sent {
                return Ok(None);
            }

            match recv_stream.recv_data().await {
                Ok(Some(mut chunk)) => {
                    let chunk_len = chunk.remaining();
                    let bytes = chunk.copy_to_bytes(chunk_len);
                    Ok(Some((Frame::data(bytes), (recv_stream, false))))
                }
                Ok(None) => match recv_stream.recv_trailers().await {
                    Ok(Some(trailers)) => {
                        debug!(
                            "H3 streaming request received {} trailer headers",
                            trailers.len()
                        );
                        Ok(Some((
                            Frame::trailers(trailers),
                            (recv_stream, true),
                        )))
                    }
                    Ok(None) => Ok(None),
                    Err(err) => {
                        log_stream_error(
                            &err,
                            "recv_trailers after streaming body",
                        );
                        Err(RequestBodyError::from_stream_error(&err))
                    }
                },
                Err(err) => {
                    log_stream_error(&err, "recv_data");
                    Err(RequestBodyError::from_stream_error(&err))
                }
            }
        },
    )
}

/// Write a complete `Response<Body>` back to the H3 stream: headers
/// first, then each body frame (data + trailers). Returns `false` on
/// any send failure so the caller can skip `finish_response_stream`
/// (already broken). Mirror of hyper's automatic body writeback —
/// done by hand because h3 doesn't surface a unified body sink.
async fn send_response_body<S, B>(
    stream: &mut h3::server::RequestStream<S, Bytes>,
    response: Response<B>,
) -> bool
where
    S: h3::quic::SendStream<Bytes>,
    B: HttpBody<Data = Bytes> + Unpin,
    B::Error: std::fmt::Debug,
{
    let (res_parts, mut res_body) = response.into_parts();
    let head_only: Response<()> = Response::from_parts(res_parts, ());

    match stream.send_response(head_only).await {
        Ok(()) => {
            let mut ok = true;

            while let Some(frame_res) = next_frame(&mut res_body).await {
                match frame_res {
                    Ok(frame) => {
                        if let Some(data) = frame.data_ref() {
                            if let Err(err) =
                                stream.send_data(data.clone()).await
                            {
                                log_stream_error(&err, "send_data");
                                ok = false;
                                break;
                            }
                        } else if frame.is_trailers() {
                            match frame.into_trailers() {
                                Ok(trailers) => {
                                    debug!(
                                        "H3 response sending {} trailer headers",
                                        trailers.len()
                                    );
                                    if let Err(err) =
                                        stream.send_trailers(trailers).await
                                    {
                                        log_stream_error(&err, "send_trailers");
                                        ok = false;
                                    }
                                    break;
                                }
                                Err(frame_back) => {
                                    error!(
                                        "❌ Frame reported trailers but couldn't extract: {:?}",
                                        frame_back
                                    );
                                    ok = false;
                                    break;
                                }
                            }
                        }
                    }
                    Err(err) => {
                        error!(
                            "❌ Reading response body frame failed: {err:?}"
                        );
                        ok = false;
                        break;
                    }
                }
            }
            ok
        }
        Err(err) => {
            log_stream_error(&err, "send_response");
            false
        }
    }
}

/// Applied only to native 0-RTT streams, after listener/SNI host admission.
/// The listener chooses DenyAll here when it cannot accept early data.
pub(super) fn check_early_data(
    policy: EarlyDataPolicy,
    method: &http::Method,
) -> Result<(), RequestRejection> {
    let allowed = match policy {
        EarlyDataPolicy::AllowAll => true,
        EarlyDataPolicy::DenyAll => false,
        EarlyDataPolicy::AllowIdempotent => matches!(
            *method,
            http::Method::GET | http::Method::HEAD | http::Method::OPTIONS
        ),
    };
    if allowed { Ok(()) } else { Err(RequestRejection::TooEarly) }
}

/// Connection-bound host; native receive/send streams keep their ownership.
#[derive(Clone)]
pub(crate) struct H3RequestHandler {
    host: HostService,
}

impl H3RequestHandler {
    pub(crate) const fn new(host: HostService) -> Self {
        Self { host }
    }

    pub(crate) async fn serve<Q>(
        &self,
        head: Request<()>,
        mut stream: H3RequestStream<Q>,
        arrival: RequestArrival,
    ) where
        Q: h3::quic::Connection<Bytes>,
        H3BidiStream<Q>: h3::quic::BidiStream<Bytes> + Send + 'static,
        <H3BidiStream<Q> as h3::quic::BidiStream<Bytes>>::RecvStream:
            Send + 'static,
    {
        let prepared = self.host.request(head, arrival);
        // WebTransport has its native accept path in the listener. WebSocket
        // CONNECT is not implemented over H3; it must not enter body buffering.
        if unsupported_websocket_connect(prepared.request()) {
            send_status_response(&mut stream, StatusCode::NOT_IMPLEMENTED)
                .await;
            stream.stop_sending(h3::error::Code::H3_REQUEST_CANCELLED);
            return;
        }
        if let Err(error) =
            checked_request_framing(prepared.request().headers())
        {
            if let Some(status) = error.response_status() {
                send_status_response(&mut stream, status).await;
            }
            stream.stop_sending(h3::error::Code::H3_REQUEST_CANCELLED);
            return;
        }
        let (mut send, recv) = stream.split();
        let body = boxed_body(StreamBody::new(into_request_body_stream(recv)));
        let response = prepared.map_body(|()| body).dispatch().await;
        if send_response_body(&mut send, response).await {
            finish_response_stream(&mut send, "sending response body").await;
        }
    }
}

fn unsupported_websocket_connect(request: &Request<()>) -> bool {
    request.method() == http::Method::CONNECT
        && request.extensions().get::<h3::ext::Protocol>()
            == Some(&h3::ext::Protocol::WEBSOCKET)
}

#[cfg(test)]
#[path = "../../../tests/edge/quic/request.rs"]
mod tests;
