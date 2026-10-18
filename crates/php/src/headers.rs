//! Header policy shared by every PHP runner (fcgi, embed, future
//! ones). Centralises the hop-by-hop deny-list so the rules don't
//! drift between runners.
//!
//! These are the names defined as hop-by-hop in RFC 7230 §6.1 plus a
//! couple of CGI-protocol concerns. They MUST NOT be forwarded
//! across the proxy boundary in either direction:
//!
//!   * request: they describe the client↔proxy transport, not the
//!     application; passing them as `HTTP_*` CGI vars confuses some
//!     PHP libraries and leaks transport state.
//!   * response: PHP might emit a stray `Connection: close` or
//!     `Transfer-Encoding: chunked` that clashes with HTTP/2/3
//!     framing on the way back to the client.
//!
//! `Cookie` is included for the **request** path only — runners hand
//! cookies to PHP via the SAPI `read_cookies` callback (or FCGI's
//! dedicated `Cookie` param) and not as a generic `HTTP_COOKIE`
//! variable, to avoid duplicate exposure.
//!
//! `Content-Type` and `Content-Length` are similarly excluded from
//! the request `HTTP_*` forwarding because they're delivered to PHP
//! through `CONTENT_TYPE` / `CONTENT_LENGTH` instead, per CGI 1.1.

/// Names that must not be forwarded between client and PHP. All
/// entries are lowercase; callers compare case-insensitively against
/// this list.
pub static DENY_HEADERS: &[&str] = &[
    // Hop-by-hop per RFC 7230 §6.1
    "connection",
    "transfer-encoding",
    "te",
    "upgrade",
    "expect",
    // CGI-protocol-specific (delivered via dedicated CGI vars, not
    // `HTTP_*` request env)
    "content-length",
    "content-type",
    "cookie",
];

/// Convenience case-insensitive membership check for ergonomic call
/// sites.
#[inline]
pub fn is_denied(name: &str) -> bool {
    DENY_HEADERS.iter().any(|denied| name.eq_ignore_ascii_case(denied))
}

/// CGI forwarding must not let client headers overwrite transport-owned
/// scheme metadata. This is request-only; response filtering is unchanged.
#[inline]
pub fn is_request_denied(name: &str) -> bool {
    is_denied(name)
        || name.eq_ignore_ascii_case("forwarded")
        || name.eq_ignore_ascii_case("x-forwarded-proto")
}

#[cfg(test)]
#[path = "../tests/headers.rs"]
mod tests;
