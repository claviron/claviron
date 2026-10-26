//! This application's Rust routes.
//!
//! The router is axum's — gufi has no opinion about it. Swapping in warp,
//! tower-http or a bare `service_fn` changes this file and nothing else.
//!
//! Nothing here mentions a fallback. Whatever these routes do not declare goes
//! to the FastAPI application, which is the one that knows what it serves and
//! already answers for what it does not.

use axum::Router;
use axum::extract::Path;
use axum::response::IntoResponse;
use axum::routing::get;
use claviron::AppState;
use claviron::http::ext::Provide;
use gufi::axum;
use gufi::axum::extract::State;
use serde_json::json;

use crate::app::services::CounterService;

pub fn rust_interceptor(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/hits", get(hits))
        .route("/hello/{name}", get(hello))
        .with_state(state)
}

/// A route reaching a service this application registered.
///
/// The registry is Claviron's and the extractor is axum's; nothing in between
/// belongs to the runner.
async fn hits(
    Provide(counter): Provide<CounterService>
) -> axum::response::Response {
    match counter.hit() {
        Ok(hits) => {
            axum::Json(json!({ "hits": hits, "from": "rust" })).into_response()
        }
        Err(error) => {
            (axum::http::StatusCode::SERVICE_UNAVAILABLE, error.to_string())
                .into_response()
        }
    }
}

async fn health() -> impl IntoResponse {
    axum::Json(json!({ "status": "ok", "handled_by": "rust" }))
}

/// The same path FastAPI declares. Whoever answers, says so.
async fn hello(
    state: State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    axum::Json(
        json!({ "message": format!("Hello {name}"), "from": "rust", "app":  state.name() }),
    )
}
