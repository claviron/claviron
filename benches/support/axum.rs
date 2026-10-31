//! Axum's own router, request extractor and JSON response conversion.

use claviron::axum::{Json, Router, extract::Request, response::IntoResponse, routing::get};

pub fn router() -> Router {
    Router::new().route("/json", get(|request: Request| async move {
        Json(super::json::payload(&request)).into_response()
    }))
}
