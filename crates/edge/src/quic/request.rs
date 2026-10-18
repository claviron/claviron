//! Native H3 body adaptation, response framing and stream completion.
//! Host admission/identity/application dispatch live in claviron-edge.

use std::pin::Pin;

use crate::transport::{
    HostResponse, HostService, RequestArrival, RequestRejection,
};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use claviron_http::boxed_body;
use futures_util::future::poll_fn;
use futures_util::stream::try_unfold;
use http::{HeaderMap, Request, Response, StatusCode};
use http_body::{Body as HttpBody, Frame};
use http_body_util::{Full, StreamBody};
use tracing::{debug, error, warn};

pub(crate) type H3BidiStream<Q> =
    <Q as h3::quic::OpenStreams<Bytes>>::BidiStream;
pub(crate) type H3RequestStream<Q> =
    h3::server::RequestStream<H3BidiStream<Q>, Bytes>;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum BodyErrorKind {
    Malformed,
    TooLarge,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct RequestBodyError {
    kind: BodyErrorKind,
}

impl RequestBodyError {
    const fn malformed() -> Self {
        Self { kind: BodyErrorKind::Malformed }
    }

    const fn too_large() -> Self {
        Self { kind: BodyErrorKind::TooLarge }
    }

    const fn kind(self) -> BodyErrorKind {
        self.kind
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
            BodyErrorKind::TooLarge => {
                f.write_str("HTTP/3 request body exceeds configured limit")
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
    headers: &HeaderMap,
    max_body_size: usize,
) -> Result<Option<usize>, RequestBodyError> {
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

    let content_length = parse_content_length(headers)?;
    if let Some(len) = content_length
        && len > max_body_size
    {
        return Err(RequestBodyError::too_large());
    }

    Ok(content_length)
}

async fn finish_response_stream<S>(
    stream: &mut h3::server::RequestStream<S, Bytes>,
    context: &'static str,
) where
    S: h3::quic::SendStream<Bytes>,
{
    if let Err(err) = stream.finish().await {
        error!("❌ H3 stream finish failed while {context}: {err:?}");
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

pub(crate) async fn send_host_response<S>(
    stream: &mut h3::server::RequestStream<S, Bytes>,
    reply: HostResponse,
) where
    S: h3::quic::SendStream<Bytes>,
{
    send_head_response(stream, host_response(reply)).await;
}

async fn send_head_response<S>(
    stream: &mut h3::server::RequestStream<S, Bytes>,
    response: Response<()>,
) where
    S: h3::quic::SendStream<Bytes>,
{
    let status = response.status();
    if let Err(err) = stream.send_response(response).await {
        error!("❌ H3 send_response failed while sending {}: {err:?}", status);
        return;
    }

    finish_response_stream(stream, "sending status response").await;
}

/// Buffered body read with running byte-count enforcement against
/// `max_body_size`. `content_length` (when present) is only used as a
/// pre-allocation hint — the actual limit check runs against bytes
/// received, so chunked / no-content-length requests are bounded too.
/// Used by the buffered dispatch path; streaming path uses
/// [`into_request_body_stream`] instead.
async fn read_request_body_with_limit<S>(
    stream: &mut h3::server::RequestStream<S, Bytes>,
    max_body_size: usize,
    content_length: Option<usize>,
) -> Result<Bytes, RequestBodyError>
where
    S: h3::quic::RecvStream,
{
    let mut total = 0usize;
    let mut body_bytes = BytesMut::with_capacity(content_length.unwrap_or(0));

    loop {
        match stream.recv_data().await {
            Ok(Some(chunk)) => {
                let chunk_len = chunk.remaining();
                total = total
                    .checked_add(chunk_len)
                    .ok_or(RequestBodyError::too_large())?;

                if total > max_body_size {
                    return Err(RequestBodyError::too_large());
                }

                body_bytes.reserve(chunk_len);
                body_bytes.put(chunk);
            }
            Ok(None) => break,
            Err(err) => {
                error!("❌ H3 recv_data failed: {err:?}");
                return Err(RequestBodyError::malformed());
            }
        }
    }

    if let Err(err) = stream.recv_trailers().await {
        error!("❌ H3 recv_trailers failed: {err:?}");
        return Err(RequestBodyError::malformed());
    }

    Ok(body_bytes.freeze())
}

/// Adapt an H3 receive stream into an `http_body::Frame` `Stream` so
/// the body can flow through Tower/Axum without buffering the whole
/// payload. Carries the running byte count and enforces `max_body_size`
/// per chunk; emits trailers as a single trailing `Frame::trailers`.
/// Counterpart to [`read_request_body_with_limit`] for the streaming
/// dispatch path.
fn into_request_body_stream<S>(
    recv_stream: h3::server::RequestStream<S, Bytes>,
    max_body_size: usize,
) -> impl futures_util::Stream<Item = Result<Frame<Bytes>, RequestBodyError>>
+ Send
+ 'static
where
    S: h3::quic::RecvStream + Send + 'static,
{
    try_unfold(
        (recv_stream, 0usize, false),
        move |(mut recv_stream, total, trailers_sent): (
            h3::server::RequestStream<S, Bytes>,
            usize,
            bool,
        )| async move {
            if trailers_sent {
                return Ok(None);
            }

            match recv_stream.recv_data().await {
                Ok(Some(mut chunk)) => {
                    let chunk_len = chunk.remaining();
                    let next_total = total
                        .checked_add(chunk_len)
                        .ok_or(RequestBodyError::too_large())?;

                    if next_total > max_body_size {
                        return Err(RequestBodyError::too_large());
                    }

                    let bytes = chunk.copy_to_bytes(chunk_len);
                    Ok(Some((
                        Frame::data(bytes),
                        (recv_stream, next_total, false),
                    )))
                }
                Ok(None) => match recv_stream.recv_trailers().await {
                    Ok(Some(trailers)) => {
                        debug!(
                            "H3 streaming request received {} trailer headers",
                            trailers.len()
                        );
                        Ok(Some((
                            Frame::trailers(trailers),
                            (recv_stream, total, true),
                        )))
                    }
                    Ok(None) => Ok(None),
                    Err(err) => {
                        error!(
                            "❌ H3 recv_trailers failed after streaming body end: {err:?}"
                        );
                        Err(RequestBodyError::malformed())
                    }
                },
                Err(err) => {
                    error!("❌ H3 recv_data failed: {err:?}");
                    Err(RequestBodyError::malformed())
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
                                error!("❌ H3 send_data failed: {err:?}");
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
                                        error!(
                                            "❌ H3 send_trailers failed: {err:?}"
                                        );
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
            error!("❌ H3 send_response failed: {err:?}");
            false
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RequestBodyMode {
    Buffered,
    Streaming,
}

/// Connection-bound application and accepted body limit. Request tasks borrow
/// no live providers and retain their native receive/send stream ownership.
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
        mode: RequestBodyMode,
    ) where
        Q: h3::quic::Connection<Bytes>,
        H3BidiStream<Q>:
            h3::quic::BidiStream<Bytes> + h3::quic::Is0rtt + Send + 'static,
        <H3BidiStream<Q> as h3::quic::BidiStream<Bytes>>::RecvStream:
            Send + 'static,
    {
        let arrival = if stream.is_0rtt() {
            RequestArrival::EarlyData
        } else {
            RequestArrival::Established
        };
        let prepared = match self.host.prepare(head, arrival) {
            Ok(prepared) => prepared,
            Err(reject) => {
                send_host_response(&mut stream, reject).await;
                return;
            }
        };
        // The listener's ceiling, already narrowed by this host. Resolved once
        // in `HostService::prepare_context`, so every enforcement point below
        // reads one number rather than choosing between two.
        // No limit declared means nothing to refuse. The helpers below still
        // take a number until H3 bodies pass `GuardedBody` like the others
        // (`docs/REVIEW.md` §3e, step 4).
        let max_body_size = prepared.max_body_size().unwrap_or(usize::MAX);
        let content_length = match checked_request_framing(
            prepared.request().headers(),
            max_body_size,
        ) {
            Ok(length) => length,
            Err(error) => {
                reject_body(
                    &mut stream,
                    error,
                    prepared.request(),
                    max_body_size,
                )
                .await;
                return;
            }
        };

        match mode {
            RequestBodyMode::Streaming => {
                let (mut send, recv) = stream.split();
                let body = boxed_body(StreamBody::new(
                    into_request_body_stream(recv, max_body_size),
                ));
                let response = prepared.map_body(|()| body).dispatch().await;
                if send_response_body(&mut send, response).await {
                    finish_response_stream(
                        &mut send,
                        "sending streaming response body",
                    )
                    .await;
                }
            }
            RequestBodyMode::Buffered => {
                let bytes = match read_request_body_with_limit(
                    &mut stream,
                    max_body_size,
                    content_length,
                )
                .await
                {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        reject_body(
                            &mut stream,
                            error,
                            prepared.request(),
                            max_body_size,
                        )
                        .await;
                        return;
                    }
                };
                let response = prepared
                    .map_body(|()| boxed_body(Full::new(bytes)))
                    .dispatch()
                    .await;
                if send_response_body(&mut stream, response).await {
                    finish_response_stream(
                        &mut stream,
                        "sending buffered response body",
                    )
                    .await;
                }
            }
        }
    }
}

/// Preserve H3's existing non-disclosing deny response; the deny decision is
/// shared, while response/close mechanics remain owned by the native driver.
fn host_response(reply: HostResponse) -> Response<()> {
    let hide_denial =
        matches!(reply, HostResponse::Rejected(RequestRejection::PathDenied));
    let mut response = reply.into_response();
    if hide_denial {
        *response.status_mut() = StatusCode::NOT_FOUND;
    }
    response
}

async fn reject_body<S>(
    stream: &mut h3::server::RequestStream<S, Bytes>,
    error: RequestBodyError,
    request: &Request<()>,
    max_body_size: usize,
) where
    S: h3::quic::RecvStream + h3::quic::SendStream<Bytes>,
{
    let status = match error.kind() {
        BodyErrorKind::TooLarge => {
            warn!(method = %request.method(), uri = %request.uri(), max_body_size,
                "H3 request body exceeds the accepted listener limit");
            stream.stop_sending(h3::error::Code::H3_REQUEST_CANCELLED);
            StatusCode::PAYLOAD_TOO_LARGE
        }
        BodyErrorKind::Malformed => StatusCode::BAD_REQUEST,
    };
    send_status_response(stream, status).await;
}

#[cfg(test)]
#[path = "../../tests/quic/request.rs"]
mod tests;
