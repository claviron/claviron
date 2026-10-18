//! Axum extractor for the frozen process env snapshot.
//!
//! Handlers that need to resolve a deploy-time env variable do so
//! through `state.env_snapshot()` — never through `std::env::var`,
//! which returns `None` for app secrets after the worker scrub. This
//! extractor is a thin sugar layer for that pattern: the snapshot is
//! cheap to clone (`Arc<HashMap>` inside), so handlers take it by
//! value and call `.get("KEY")` directly.

use std::ops::Deref;

#[cfg(feature = "axum")]
use axum::extract::FromRequestParts;
#[cfg(feature = "axum")]
use claviron_runtime::AppState;
use claviron_runtime::env::EnvSnapshot;
#[cfg(feature = "axum")]
use http::request::Parts;

/// Process env snapshot, captured at `State::initialize`. Pulled from
/// the axum state — never reads `std::env` at request time.
#[derive(Clone, Debug)]
pub struct Env(pub EnvSnapshot);

impl Env {
    pub fn get(
        &self,
        key: &str,
    ) -> Option<String> {
        self.0.get(key)
    }
}

impl Deref for Env {
    type Target = EnvSnapshot;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(feature = "axum")]
impl FromRequestParts<AppState> for Env {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(
        _parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        Ok(Env(state.env_snapshot().clone()))
    }
}
