//! Header utilities shared across HTTP transports.
//!
//! Lives in `claviron-http` (not `claviron-php`) because the concern
//! is HTTP-wire — browsers' multi-occurrence header behaviour applies
//! to every consumer, not just CGI/SAPI runners.

use http::HeaderMap;
use http::header::COOKIE;

/// Concatenate every `Cookie:` header occurrence into a single
/// `"; "`-separated string, mirroring HTTP/1.x request shape.
///
/// HTTP/2 (RFC 7540 §8.1.2.5) and HTTP/3 explicitly allow clients to
/// split the `Cookie` field into multiple header occurrences for
/// HPACK / QPACK compression efficiency. Chrome and Firefox routinely
/// do this. A naive `headers.get(COOKIE)` returns only the first
/// occurrence, so any cookie that landed in a later `Cookie:` line
/// gets lost — manifesting on the application as "session cookie
/// suddenly vanishes after a few requests" while smaller cookies
/// (CSRF, identity) survive because they happened to be in the first
/// occurrence.
///
/// Returns `None` when no `Cookie:` header is present (lets callers
/// distinguish "no cookies sent" from "empty cookie value"). Empty
/// individual values are skipped on the way out.
#[must_use]
pub fn join_cookie_header(headers: &HeaderMap) -> Option<String> {
    let mut joined = String::new();
    for value in headers.get_all(COOKIE) {
        let Ok(s) = value.to_str() else { continue };
        if s.is_empty() {
            continue;
        }
        if !joined.is_empty() {
            joined.push_str("; ");
        }
        joined.push_str(s);
    }
    if joined.is_empty() { None } else { Some(joined) }
}

#[cfg(test)]
#[path = "../../tests/resolvers/cookies.rs"]
mod tests;
