use std::path::Path;

use bytes::Bytes;
use http::{Request, Response, StatusCode, header};
use http_body_util::Full;
use hyper::body::Incoming;
use tokio::fs;
use tracing::{error, info};

type ResBody = Full<Bytes>;

/// `/.well-known/acme-challenge/` — the caller (`server.rs`) tests for
/// this prefix and only dispatches into `serve_acme_challenge` when it
/// matches. The handler doesn't re-route.
pub const ACME_PREFIX: &str = "/.well-known/acme-challenge/";

/// Serve an ACME HTTP-01 challenge. Caller has already confirmed the
/// request path starts with [`ACME_PREFIX`]; this fn extracts the
/// token, validates the character set, reads the corresponding file
/// out of `challenges_dir`, and emits the response. Always returns a
/// concrete response — `403` on a malformed token, `404` if the file
/// isn't on disk, `200` with the token contents on success — so the
/// caller never has to decide "was this even an acme request?".
pub async fn serve_acme_challenge(
    req: &Request<Incoming>,
    challenges_dir: &Path,
) -> Response<ResBody> {
    // Caller-prefix-checked; this strip is the token extraction. If
    // somehow the prefix is missing the unwrap_or("") ⇒ token validation
    // 403 — defensive but cheap.
    let token = req.uri().path().strip_prefix(ACME_PREFIX).unwrap_or("");

    if !is_valid_acme_token(token) {
        let mut res = Response::new(Full::new(Bytes::new()));
        *res.status_mut() = StatusCode::FORBIDDEN;
        res.headers_mut().insert(
            header::CONNECTION,
            header::HeaderValue::from_static("close"),
        );
        return res;
    }

    let path = challenges_dir.join(token);

    match fs::read(&path).await {
        Ok(bytes) => {
            info!(token = %token, path = %path.display(), "🧿 ACME challenge accepted");

            let mut res = Response::new(Full::new(Bytes::from(bytes)));
            *res.status_mut() = StatusCode::OK;
            res.headers_mut().insert(
                header::CONTENT_TYPE,
                header::HeaderValue::from_static("text/plain"),
            );
            res.headers_mut().insert(
                header::CACHE_CONTROL,
                header::HeaderValue::from_static("no-store"),
            );
            res.headers_mut().insert(
                header::CONNECTION,
                header::HeaderValue::from_static("close"),
            );
            info!(token = %token, path = %path.display(), "✅ ACME challenge served successfully");
            res
        }
        Err(e) => {
            error!("❌ ACME token read failed: path={:?} err={:?}", path, e);
            let mut res = Response::new(Full::new(Bytes::new()));
            *res.status_mut() = StatusCode::NOT_FOUND;
            res.headers_mut().insert(
                header::CONNECTION,
                header::HeaderValue::from_static("close"),
            );
            res
        }
    }
}

#[inline(always)]
fn is_valid_acme_token(token: &str) -> bool {
    !token.is_empty()
        && token.bytes().all(|b| {
            b.is_ascii_uppercase()
                || b.is_ascii_lowercase()
                || b.is_ascii_digit()
                || b == b'-'
                || b == b'_'
        })
}
