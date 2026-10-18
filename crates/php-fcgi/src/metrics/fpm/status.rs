//! FastCGI status request.
//!
//! Sends a single FCGI request to PHP-FPM's `status.path` (configured
//! on the FPM side, typically `/status` returning JSON). FPM intercepts
//! that path at the master/worker level — no PHP runtime is invoked on
//! ping, only JSON status is returned.

use std::io;

use fastcgi_client::{Params, Request};
use serde_json::Value;
use tracing::error;

use crate::connection::FcgiClient;

/// Send a `status.path` request on a keep-alive FCGI client and return
/// the parsed JSON on success.
pub(crate) async fn status_request(
    client: &mut FcgiClient,
    status_path: &str,
) -> io::Result<Value> {
    let request_uri = format!("{status_path}?json");
    let params = Params::default()
        .gateway_interface("CGI/1.1")
        .server_software("claviron-status")
        .request_method("GET")
        .script_name(status_path.to_owned())
        .script_filename(status_path.to_owned())
        .request_uri(request_uri)
        .document_uri(status_path.to_owned())
        .query_string("json")
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
        .map_err(|e| io::Error::other(format!("status execute: {e}")))?;

    // Check stderr
    if let Some(err_bytes) = &resp.stderr
        && !err_bytes.is_empty()
    {
        let msg = String::from_utf8_lossy(err_bytes);
        error!("status stderr: {msg}");
        return Err(io::Error::other(format!("status stderr: {msg}")));
    }

    // Check stdout
    let Some(out) = &resp.stdout else {
        return Err(io::Error::other("status stdout empty"));
    };

    // Parse JSON, tolerate headers before JSON
    let json_start = out
        .iter()
        .position(|&b| b == b'{')
        .ok_or_else(|| io::Error::other("status JSON not found"))?;
    let json_slice = &out[json_start..];

    let json: Value = serde_json::from_slice(json_slice).map_err(|e| {
        io::Error::other(format!("status JSON parse error: {e}"))
    })?;

    // Optionally: check that expected keys exist
    if json.get("pool").is_none() {
        return Err(io::Error::other("status JSON missing 'pool' key"));
    }

    Ok(json)
}
