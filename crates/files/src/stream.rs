use std::fs::Metadata;
use std::io::{ErrorKind, SeekFrom};
use std::path::PathBuf;

use claviron_http::{RouteBody, RouteResponse, boxed_body, empty_body};
use futures_util::TryStreamExt;
use http::header::{
    ACCEPT_RANGES, CONTENT_RANGE, CONTENT_TYPE, ETAG, LAST_MODIFIED,
};
use http::{HeaderValue, StatusCode};
use http_body::Frame;
use http_body_util::StreamBody;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::ReaderStream;

use crate::caching::condition::ConditionalRequest;
use crate::caching::metrics;
use crate::caching::policy::{CachePolicy, apply_client_cache_headers};
use crate::mime::resolve_content_type;
use crate::{mime, response};

pub async fn stream_from_disk(
    path: &PathBuf,
    meta: &Metadata,
    policy: &CachePolicy,
    cond: &ConditionalRequest,
) -> RouteResponse {
    let since = meta.modified().ok().map(httpdate::fmt_http_date);
    let etag = mime::make_etag_from_meta(meta);
    let len = meta.len();

    if cond.honors_range(len, etag.as_deref(), since.as_deref()) {
        return stream_range_response(
            path,
            policy,
            cond,
            since.as_deref(),
            etag.as_deref(),
            len,
        )
        .await;
    }

    let build_full_ok = |body: RouteBody, ct: String, since_ref: &str| {
        let mut resp =
            response::build_ok_response(policy, &ct, since_ref, body);
        resp.headers_mut()
            .insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
        resp
    };

    // Fallback: open and stream the file fully (200 OK)
    match File::open(&path).await {
        Ok(file) => {
            let stream = ReaderStream::new(file).map_ok(Frame::data);
            let ct = resolve_content_type(path.as_path());
            let since_ref = since.as_deref().unwrap_or("");
            let mut resp = build_full_ok(
                boxed_body(StreamBody::new(stream)),
                ct,
                since_ref,
            );
            // A body read from disk has no length of its own; the file does.
            resp.headers_mut()
                .insert(http::header::CONTENT_LENGTH, HeaderValue::from(len));
            resp.headers_mut()
                .insert("x-cache", HeaderValue::from_static("MISS"));
            if let Some(tag) = etag {
                resp.headers_mut()
                    .insert(ETAG, HeaderValue::from_str(&tag).unwrap());
            }
            metrics::miss();
            resp
        }
        Err(e) if e.kind() == ErrorKind::NotFound => {
            response::status_response(StatusCode::NOT_FOUND)
        }
        Err(_) => response::status_response(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

fn add_validators(
    headers: &mut http::HeaderMap,
    etag: Option<&str>,
    since: Option<&str>,
) {
    if let Some(tag) = etag {
        headers.insert(ETAG, HeaderValue::from_str(tag).unwrap());
    }
    if let Some(since_ref) = since {
        headers
            .insert(LAST_MODIFIED, HeaderValue::from_str(since_ref).unwrap());
    }
    headers.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
}

async fn stream_range_response(
    path: &PathBuf,
    policy: &CachePolicy,
    cond: &ConditionalRequest,
    since: Option<&str>,
    etag: Option<&str>,
    len: u64,
) -> RouteResponse {
    // Precompute validators and length

    // We have a Range and we can honor it. Validate bounds and send 206 or 416.
    if let Some((start, end)) = cond.range_bounds(len) {
        match File::open(&path).await {
            Ok(mut file) => {
                if (file.seek(SeekFrom::Start(start)).await).is_err() {
                    return response::status_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                    );
                }
                let count = end - start + 1;
                let limited = file.take(count);
                let stream = ReaderStream::new(limited).map_ok(Frame::data);
                let ct = resolve_content_type(path.as_path());
                let mut resp = http::Response::builder()
                    .status(StatusCode::PARTIAL_CONTENT)
                    .header(CONTENT_TYPE, ct.as_str())
                    .header(ACCEPT_RANGES, "bytes")
                    .header(
                        CONTENT_RANGE,
                        format!("bytes {}-{}/{}", start, end, len),
                    )
                    .body(boxed_body(StreamBody::new(stream)))
                    .unwrap();
                add_validators(resp.headers_mut(), etag, since);
                apply_client_cache_headers(resp.headers_mut(), policy);
                resp.headers_mut()
                    .insert("x-cache", HeaderValue::from_static("BYPASS-206"));
                resp
            }
            Err(e) if e.kind() == ErrorKind::NotFound => {
                response::status_response(StatusCode::NOT_FOUND)
            }
            Err(_) => {
                response::status_response(StatusCode::INTERNAL_SERVER_ERROR)
            }
        }
    } else {
        // Invalid range -> 416
        let mut resp = http::Response::builder()
            .status(StatusCode::RANGE_NOT_SATISFIABLE)
            .header(ACCEPT_RANGES, "bytes")
            .header(CONTENT_RANGE, format!("bytes */{}", len))
            .body(empty_body())
            .unwrap();
        if let Some(tag) = etag {
            resp.headers_mut()
                .insert(ETAG, HeaderValue::from_str(tag).unwrap());
        }
        resp
    }
}
