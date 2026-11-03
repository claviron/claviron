//! Axum's terminal service on the shared Rust runner.

use ::axum::Router;
use claviron_runtime::AppState;

use super::ApplicationRegistration;

pub fn register<F>(build: F) -> ApplicationRegistration
where
    F: FnOnce(AppState) -> Router + Send + 'static,
{
    super::router(build)
}
