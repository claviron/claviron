//! axum compatibility shims.
//!
//! Kept here (instead of inside `ctx.rs`) so the core types stay
//! framework-agnostic. Anything that mentions `axum::` lives in this
//! module and is feature-gated under `axum`. Disabling that feature
//! drops every axum-related symbol from the build entirely; consumers
//! that don't run axum routers (pure-Handler/Upstream paths) pay
//! nothing.

#![cfg(feature = "axum")]

use axum::response::IntoResponse;
use http::Response;

use crate::ctx::{RouteResponse, boxed_body};

impl IntoResponse for crate::Error {
    fn into_response(self) -> axum::response::Response {
        let status = self.status();
        let body = serde_json::json!({
            "error": self.code(),
            "message": self.message()
        });

        (status, axum::Json(body)).into_response()
    }
}

/// `RouteResponse` → `axum::Response`. Body type rewrap only — zero
/// data copy, single `Box::new` for the trait-erasure wrapper.
#[inline]
pub fn into_axum_response(resp: RouteResponse) -> axum::response::Response {
    let (parts, body) = resp.into_parts();
    axum::response::Response::from_parts(parts, axum::body::Body::new(body))
}

/// Reverse of `into_axum_response`. Used by the vhost dispatcher when
/// the `AppKind::Axum` branch yields an axum-shaped response that
/// has to flow through the pipeline as a `RouteResponse`.
#[inline]
pub fn from_axum_response(resp: axum::response::Response) -> RouteResponse {
    let (parts, body) = resp.into_parts();
    Response::from_parts(parts, boxed_body(body))
}

#[cfg(test)]
#[path = "../tests/bindings.rs"]
mod tests;
