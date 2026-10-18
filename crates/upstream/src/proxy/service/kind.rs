//! Owns plain, upgrade and extended CONNECT request detection.

use claviron_http::RouteBody;
use http::header::{CONNECTION, UPGRADE};
use http::{Method, Request, Version};

pub(crate) fn is_h1_upgrade_request(req: &Request<RouteBody>) -> bool {
    req.headers().contains_key(UPGRADE)
        && req
            .headers()
            .get(CONNECTION)
            .and_then(|value| value.to_str().ok())
            .map(|value| {
                value
                    .split(',')
                    .map(str::trim)
                    .any(|token| token.eq_ignore_ascii_case("upgrade"))
            })
            .unwrap_or(false)
}

pub(crate) fn is_h2_extended_connect_request(req: &Request<RouteBody>) -> bool {
    req.method() == Method::CONNECT
        && req.version() == Version::HTTP_2
        && req.extensions().get::<hyper::ext::Protocol>().is_some()
}
