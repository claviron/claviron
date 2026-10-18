//! FastCGI keep-alive validator.
//!
//! Sends a single FCGI request to PHP-FPM's `ping.path` (configured on the
//! FPM side, typically `/ping` returning `pong`). FPM intercepts that path
//! at the master/worker level — no PHP runtime is invoked, no opcache
//! load — so the round-trip is cheap and proves three things:
//!
//! 1. The FCGI socket is wired through to a live FPM worker (not a half-
//!    open TCP / orphan Unix socket).
//! 2. The worker can complete a full request/response cycle on this
//!    keep-alive connection (BEGIN_REQUEST → STDIN → STDOUT → END_REQUEST).
//! 3. We have not been silently disconnected by FPM's idle timeout.
//!
//! Used by the pool's warm-up and replenisher paths so a connection is
//! only published to the idle queue once it has been confirmed active —
//! turning "TCP connect succeeded" into "FPM end is actually responsive".

use std::io;

use fastcgi_client::{Params, Request};
use tracing::error;

use crate::connection::FcgiClient;

/// Send a `ping.path` request on a keep-alive FCGI client and verify the
/// response body is `pong`. Returns `Ok(())` only when stderr is empty
/// and stdout (after FCGI header section) ends with `pong`.
pub(crate) async fn ping_keepalive(
    client: &mut FcgiClient,
    ping_path: &str,
) -> io::Result<()> {
    let params = Params::default()
        .gateway_interface("CGI/1.1")
        .server_software("claviron-ping")
        .request_method("GET")
        .script_name(ping_path.to_owned())
        .script_filename(ping_path.to_owned())
        .request_uri(ping_path.to_owned())
        .document_uri(ping_path.to_owned())
        .query_string("")
        .server_protocol("HTTP/1.1")
        .server_name("localhost")
        .server_addr("127.0.0.1")
        .server_port(443)
        .remote_addr("127.0.0.1")
        .remote_port(0)
        .content_length(0)
        .content_type("");

    let mut empty: &[u8] = &[];
    let resp = client
        .execute(Request::new(params, &mut empty))
        .await
        .map_err(|e| io::Error::other(format!("ping execute: {e}")))?;

    if let Some(err_bytes) = &resp.stderr
        && !err_bytes.is_empty()
    {
        let msg = String::from_utf8_lossy(err_bytes);
        error!("ping stderr: {msg}");
        return Err(io::Error::other(format!("ping stderr: {msg}")));
    }

    let Some(out) = &resp.stdout else {
        return Err(io::Error::other("ping stdout empty"));
    };

    let txt = std::str::from_utf8(out)
        .map_err(|_| io::Error::other("ping stdout not utf-8"))?;

    // FPM may emit `Content-Type: ...\r\n\r\npong` (header section) or
    // strip headers and just emit `pong`. Tolerate both shapes plus a
    // trailing-whitespace variant.
    let ok = txt.ends_with("\r\n\r\npong")
        || txt.ends_with("\n\npong")
        || txt.trim_end() == "pong";

    if !ok {
        return Err(io::Error::other(format!(
            "ping body not pong: {}",
            txt.trim_end()
        )));
    }

    Ok(())
}
