//! Header-only CORS decisions and response shaping.

use claviron_http::{RouteResponse, empty_body};
use http::{HeaderMap, HeaderValue, Method, StatusCode};

use super::CorsState;
use crate::config::cors::{CorsConfig, CorsPolicy};

pub(super) fn check(
    method: &Method,
    headers: &HeaderMap,
    state: &CorsState,
) -> Option<RouteResponse> {
    if state.denied {
        let mut response = RouteResponse::new(empty_body());
        *response.status_mut() = StatusCode::FORBIDDEN;
        return Some(response);
    }
    if state.origin.is_none()
        || *method != Method::OPTIONS
        || !headers.contains_key(http::header::ACCESS_CONTROL_REQUEST_METHOD)
    {
        return None;
    }
    let mut response = RouteResponse::new(empty_body());
    *response.status_mut() = StatusCode::NO_CONTENT;
    let response_headers = response.headers_mut();
    response_headers.insert(
        http::header::ACCESS_CONTROL_ALLOW_METHODS,
        headers[http::header::ACCESS_CONTROL_REQUEST_METHOD].clone(),
    );
    if let Some(headers) =
        headers.get(http::header::ACCESS_CONTROL_REQUEST_HEADERS)
    {
        response_headers.insert(
            http::header::ACCESS_CONTROL_ALLOW_HEADERS,
            headers.clone(),
        );
    }
    add_vary(response_headers, "access-control-request-method");
    add_vary(response_headers, "access-control-request-headers");
    Some(response)
}

pub(super) fn finish(
    cors: &CorsConfig,
    origin: Option<&HeaderValue>,
    headers: &mut HeaderMap,
) {
    if cors.policy == CorsPolicy::Disabled {
        return;
    }
    let reflect = cors.policy == CorsPolicy::Allowlist || cors.credentials;
    if reflect {
        // Cached replies also vary when Origin is absent or refused.
        add_vary(headers, "origin");
    }
    let Some(origin) = origin else {
        return;
    };
    headers.insert(
        http::header::ACCESS_CONTROL_ALLOW_ORIGIN,
        if reflect { origin.clone() } else { HeaderValue::from_static("*") },
    );
    if cors.credentials {
        headers.insert(
            http::header::ACCESS_CONTROL_ALLOW_CREDENTIALS,
            HeaderValue::from_static("true"),
        );
    } else {
        headers.remove(http::header::ACCESS_CONTROL_ALLOW_CREDENTIALS);
    }
}

fn add_vary(
    headers: &mut HeaderMap,
    name: &'static str,
) {
    if headers.get_all(http::header::VARY).iter().any(|value| {
        value.to_str().is_ok_and(|value| {
            value.split(',').any(|part| {
                let part = part.trim();
                part == "*" || part.eq_ignore_ascii_case(name)
            })
        })
    }) {
        return;
    }
    headers.append(http::header::VARY, HeaderValue::from_static(name));
}
