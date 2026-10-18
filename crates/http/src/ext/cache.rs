use std::ops::Deref;
use std::sync::Arc;

use axum::extract::FromRequestParts;
use claviron_cache::CacheProvider;
use claviron_runtime::AppState;
use http::request::Parts;

/// Axum extractor for Claviron's configured cache facade.
///
/// The runtime state remains cache-agnostic. This HTTP boundary resolves the
/// provider for the current request and exposes its default-store operations
/// directly; named stores remain available through `CacheProvider::store`.
#[derive(Clone)]
pub struct Cache(Arc<CacheProvider>);

impl Deref for Cache {
    type Target = CacheProvider;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl FromRequestParts<AppState> for Cache {
    type Rejection = crate::Error;

    async fn from_request_parts(
        _parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        state.registry_ref().resolve::<CacheProvider>().map(Cache).ok_or_else(
            || {
                crate::Error::unavailable(
                    "CacheProvider is not registered".to_string(),
                )
            },
        )
    }
}
