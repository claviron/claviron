use std::ops::Deref;
use std::sync::Arc;

#[cfg(feature = "axum")]
use axum::extract::FromRequestParts;
#[cfg(feature = "axum")]
use claviron_runtime::AppState;
#[cfg(feature = "axum")]
use http::request::Parts;

/// Axum extractor for dyn capabilities registered in the Claviron registry.
///
/// `Inject<dyn T>` resolves the current dyn binding on every request, so
/// bindings changed by a provider reload are visible without rebuilding the
/// router.
pub struct Inject<T: ?Sized + Send + Sync + 'static>(pub Arc<T>);

impl<T: ?Sized + Send + Sync + 'static> Deref for Inject<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

#[cfg(feature = "axum")]
impl<T> FromRequestParts<AppState> for Inject<T>
where
    T: ?Sized + Send + Sync + 'static,
{
    type Rejection = crate::Error;

    async fn from_request_parts(
        _parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        state.registry_ref().resolve_dyn::<T>().map(Inject).ok_or_else(|| {
            crate::Error::unavailable(format!(
                "dyn provider is not registered: {}",
                std::any::type_name::<T>()
            ))
        })
    }
}

/// Axum extractor for a concrete value registered in the Claviron registry:
/// a component or a plain value.
///
/// The sibling of [`Inject`], and deliberately a second type rather than the
/// same one: the registry keeps two tables and they answer different
/// questions. `Provide<T>` reads the one keyed by concrete type, so `T` is
/// `Sized`; `Inject<dyn T>` reads the one keyed by trait
/// object, which only has an entry once `bind_dyn::<dyn T, P>()` has been
/// called. `resolve` downcasts and so cannot take a `?Sized` type at all,
/// which is why one extractor cannot serve both.
///
/// It resolves on every request rather than once into the router, so a
/// provider replaced by a reload is visible without the router being rebuilt.
///
/// ```ignore
/// async fn hits(Provide(counter): Provide<CounterService>) -> impl IntoResponse {
///     Json(json!({ "hits": counter.hit() }))
/// }
/// ```
pub struct Provide<T>(pub Arc<T>);

impl<T> Deref for Provide<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

#[cfg(feature = "axum")]
impl<T> FromRequestParts<AppState> for Provide<T>
where
    T: Send + Sync + 'static,
{
    type Rejection = crate::Error;

    async fn from_request_parts(
        _parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        state.registry_ref().resolve::<T>().map(Provide).ok_or_else(|| {
            crate::Error::unavailable(format!(
                "provider is not registered: {}",
                std::any::type_name::<T>()
            ))
        })
    }
}
