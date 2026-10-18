use claviron_http::{RouteBody, RouteResponse, empty_body};
use http::header::{CONTENT_TYPE, ETAG, LAST_MODIFIED, VARY};
use http::{HeaderName, HeaderValue, Response, StatusCode};

use crate::caching::policy::{CachePolicy, apply_client_cache_headers};

#[inline]
pub(super) fn build_ok_response(
    policy: &CachePolicy,
    content_type: &str,
    last_modified: &str,
    body: RouteBody,
) -> RouteResponse {
    static X_CONTENT_TYPE_OPTIONS: HeaderName =
        HeaderName::from_static("x-content-type-options");

    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, content_type)
        .header(&X_CONTENT_TYPE_OPTIONS, "nosniff");

    if !last_modified.is_empty() {
        builder = builder.header(LAST_MODIFIED, last_modified);
    }

    // Whenever the root can compress, the representation depends on
    // Accept-Encoding — and it depends on it for *every* response, not only the
    // ones that came back encoded. A shared cache that stores the encoded
    // variant under an Accept-Encoding-blind key hands brotli to a client that
    // never asked for it.
    if policy.compression.is_some() {
        builder = builder.header(VARY, "accept-encoding");
    }

    let mut resp = builder.body(body).unwrap();

    // Apply client cache policy
    apply_client_cache_headers(resp.headers_mut(), policy);

    resp
}

#[inline]
pub fn cached_not_modified_response(
    etag: &str,
    last_modified: &str,
) -> RouteResponse {
    let mut resp = Response::builder()
        .status(StatusCode::NOT_MODIFIED)
        .body(empty_body())
        .unwrap();
    resp.headers_mut().insert(ETAG, HeaderValue::from_str(etag).unwrap());
    resp.headers_mut()
        .insert(LAST_MODIFIED, HeaderValue::from_str(last_modified).unwrap());
    resp.headers_mut().insert("x-cache", HeaderValue::from_static("HIT"));
    resp
}

#[inline]
pub fn status_response(status: StatusCode) -> RouteResponse {
    Response::builder().status(status).body(empty_body()).unwrap()
}
