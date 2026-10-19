use lumenize::axum::{Router, extract::Query, http::StatusCode, routing::get};
use lumenize::claviron::{http::ext::Provide, runtime::AppState};
use serde::Deserialize;

use super::services::OrbitalCellService;

/// Rust owns only the declared routes; the runner forwards the rest to Laravel.
pub fn rust_interceptor(state: AppState) -> Router {
    Router::new()
        .route("/_lumenize/rust", get(hits))
        .route("/_lumenize/rust/value", get(value))
        .with_state(state)
}

async fn value(Provide(cell): Provide<OrbitalCellService>) -> Result<String, (StatusCode, String)> {
    let value = cell
        .value()
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;

    Ok(format!("cell={value}\n"))
}

#[derive(Deserialize)]
struct HitQuery {
    value: Option<u64>,
}

async fn hits(
    Provide(cell): Provide<OrbitalCellService>,
    Query(query): Query<HitQuery>,
) -> Result<String, (StatusCode, String)> {
    let value = cell
        .hit(query.value.unwrap_or(1))
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;

    Ok(format!("cell={value}\n"))
}
