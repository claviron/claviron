//! HTTP body ingestion before handing an owned message to a PHP context.

use bytes::{Bytes, BytesMut};
use claviron_http::RouteBody;
use claviron_http::policy::RequestPolicy;
use http::request::Parts;
use http::{Method, header::CONTENT_LENGTH};
use http_body::Body;
use http_body_util::BodyExt;
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub(super) enum Rejection {
    TooLarge,
    Stopped,
    BadRequest(String),
}

pub(super) async fn read(
    parts: &Parts,
    mut body: RouteBody,
    policy: &RequestPolicy,
    stopping: &CancellationToken,
) -> Result<Bytes, Rejection> {
    // End-of-stream is the transport's fact, not an inference from headers.
    if matches!(parts.method, Method::GET | Method::HEAD)
        && body.is_end_stream()
    {
        return Ok(Bytes::new());
    }
    if claviron_http::body::declared_length_within(
        &parts.headers,
        policy.max_body_size,
    )
    .is_err()
    {
        return Err(Rejection::TooLarge);
    }
    let mut buffered = BytesMut::new();
    loop {
        let frame = tokio::select! {
            biased;
            _ = stopping.cancelled() => return Err(Rejection::Stopped),
            frame = body.frame() => frame,
        };
        let Some(frame) = frame else { break };
        let frame = match frame {
            Ok(frame) => frame,
            Err(error)
                if matches!(
                    error.downcast_ref::<claviron_http::body::BodyLimit>(),
                    Some(claviron_http::body::BodyLimit::TooLarge { .. })
                ) =>
            {
                return Err(Rejection::TooLarge);
            }
            Err(error) => {
                return Err(Rejection::BadRequest(format!(
                    "application body rejected: {error}"
                )));
            }
        };
        if let Ok(data) = frame.into_data() {
            if policy.max_body_size.is_some_and(|limit| {
                buffered.len().saturating_add(data.len()) > limit
            }) {
                return Err(Rejection::TooLarge);
            }
            buffered.extend_from_slice(&data);
        } else {
            return Err(Rejection::BadRequest(
                "application request trailers are not supported".into(),
            ));
        }
    }
    let bytes = buffered.freeze();
    if parts
        .headers
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .is_some_and(|declared| declared != bytes.len() as u64)
    {
        return Err(Rejection::BadRequest(
            "application body length differs from Content-Length".into(),
        ));
    }
    Ok(bytes)
}

#[cfg(test)]
#[path = "../../../tests/application_body.rs"]
mod tests;
