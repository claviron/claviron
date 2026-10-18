use std::num::NonZeroU16;

use crate::transport::RequestAuthority;
use claviron_http::{RouteResponse, err_status};
use http::{StatusCode, Uri, header};
use tracing::debug;

use super::error::{Error, Result};

/// Build a redirect after authority validation and host policy selection.
/// No second resolver read or arbitrary TLS listener lookup.
pub fn handle_redirect(
    authority: &RequestAuthority,
    uri: &Uri,
    https_port: NonZeroU16,
) -> Result<RouteResponse> {
    let host = authority.authority().host();
    let path = uri.path_and_query().map(|pq| pq.as_str()).unwrap_or("/");
    let location = if https_port.get() == 443 {
        format!("https://{host}{path}")
    } else {
        format!("https://{host}:{https_port}{path}")
    };
    let mut response = err_status(StatusCode::PERMANENT_REDIRECT);
    response.headers_mut().insert(
        header::LOCATION,
        header::HeaderValue::from_str(&location).map_err(|_| {
            Error::Other("invalid Location header value".to_owned())
        })?,
    );
    response
        .headers_mut()
        .insert(header::CONNECTION, header::HeaderValue::from_static("close"));
    debug!(%host, %location, "🔁 redirect: sending 308");
    Ok(response)
}

#[cfg(test)]
#[path = "../../../tests/tcp/unit/redirect.rs"]
mod tests;
