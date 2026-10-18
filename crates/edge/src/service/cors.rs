//! Transport-independent CORS admission and response shaping.
use claviron_http::{RouteResponse, empty_body};
use http::{HeaderMap, HeaderValue, Method, StatusCode};

use crate::config::cors::{CorsConfig, CorsPolicy};

pub(super) fn check(
    cors: &CorsConfig,
    method: &Method,
    headers: &HeaderMap,
) -> (Option<RouteResponse>, Option<HeaderValue>) {
    let origin = headers
        .get(http::header::ORIGIN)
        .and_then(|header| header.to_str().ok());

    let Some(origin) = origin else {
        return (None, None);
    };

    if !cors.enabled() || !cors.allows_origin(origin) {
        let mut response = RouteResponse::new(empty_body());
        *response.status_mut() = StatusCode::FORBIDDEN;
        return (Some(response), None);
    }
    let accepted_origin = headers.get(http::header::ORIGIN).cloned();

    if *method == Method::OPTIONS {
        let mut response = RouteResponse::new(empty_body());
        *response.status_mut() = StatusCode::NO_CONTENT;
        let response_headers = response.headers_mut();
        mirror_preflight(headers, response_headers);
        return (Some(response), accepted_origin);
    }

    (None, accepted_origin)
}

pub(super) fn finish(
    cors: &CorsConfig,
    origin: Option<&HeaderValue>,
    headers: &mut HeaderMap,
) {
    // Only the origin accepted by check reaches this response phase.
    if let Some(origin) = origin.and_then(|value| value.to_str().ok()) {
        set_cors_headers(headers, origin, cors);
        add_vary(headers, cors);
    }
}

fn mirror_preflight(
    request_headers: &HeaderMap,
    response_headers: &mut HeaderMap,
) {
    if let Some(method) =
        request_headers.get(http::header::ACCESS_CONTROL_REQUEST_METHOD)
    {
        response_headers
            .insert(http::header::ACCESS_CONTROL_ALLOW_METHODS, method.clone());
    }
    if let Some(headers) =
        request_headers.get(http::header::ACCESS_CONTROL_REQUEST_HEADERS)
    {
        response_headers.insert(
            http::header::ACCESS_CONTROL_ALLOW_HEADERS,
            headers.clone(),
        );
    }
    response_headers.insert(
        http::header::ACCESS_CONTROL_MAX_AGE,
        HeaderValue::from_static("600"),
    );
}

fn set_cors_headers(
    headers: &mut HeaderMap,
    origin: &str,
    cors: &CorsConfig,
) {
    use http::header::{
        ACCESS_CONTROL_ALLOW_CREDENTIALS, ACCESS_CONTROL_ALLOW_ORIGIN,
    };

    match cors.policy {
        CorsPolicy::Permissive if cors.credentials => {
            headers.insert(
                ACCESS_CONTROL_ALLOW_ORIGIN,
                HeaderValue::from_str(origin)
                    .unwrap_or(HeaderValue::from_static("null")),
            );
            headers.insert(
                ACCESS_CONTROL_ALLOW_CREDENTIALS,
                HeaderValue::from_static("true"),
            );
        }
        CorsPolicy::Permissive => {
            headers.insert(
                ACCESS_CONTROL_ALLOW_ORIGIN,
                HeaderValue::from_static("*"),
            );
            headers.remove(ACCESS_CONTROL_ALLOW_CREDENTIALS);
        }
        CorsPolicy::Allowlist => {
            headers.insert(
                ACCESS_CONTROL_ALLOW_ORIGIN,
                HeaderValue::from_str(origin)
                    .unwrap_or(HeaderValue::from_static("null")),
            );
            if cors.credentials {
                headers.insert(
                    ACCESS_CONTROL_ALLOW_CREDENTIALS,
                    HeaderValue::from_static("true"),
                );
            } else {
                headers.remove(ACCESS_CONTROL_ALLOW_CREDENTIALS);
            }
        }
        CorsPolicy::Disabled => {}
    }
}

fn add_vary(
    headers: &mut HeaderMap,
    cors: &CorsConfig,
) {
    use http::header::VARY;

    let vary_origin = matches!(cors.policy, CorsPolicy::Allowlist)
        || (matches!(cors.policy, CorsPolicy::Permissive) && cors.credentials);
    let mut parts =
        vec!["access-control-request-method", "access-control-request-headers"];
    if vary_origin {
        parts.insert(0, "origin");
    }

    if let Some(existing) = headers.get(VARY) {
        let Ok(existing) = existing.to_str() else {
            return;
        };
        if existing.trim() == "*" {
            return;
        }
        let mut merged: Vec<&str> = existing
            .split(',')
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .collect();
        for part in parts {
            if !merged.iter().any(|item| item.eq_ignore_ascii_case(part)) {
                merged.push(part);
            }
        }
        headers
            .insert(VARY, HeaderValue::from_str(&merged.join(", ")).unwrap());
        return;
    }

    headers.insert(VARY, HeaderValue::from_str(&parts.join(", ")).unwrap());
}
