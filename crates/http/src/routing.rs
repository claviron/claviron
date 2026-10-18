//! Axum route capabilities shared by standalone runners and edge consumers.

pub mod dispatch;
pub mod extensions;
pub use crate::config::routes::RouteTable;
pub use dispatch::{ConfiguredRoute, RouteDispatch};
pub use extensions::{
    RouteCondition, RouteContext, RouteEffect, RouteExtension, RouteLayer,
    RuleContext, directive,
};

use std::sync::{Arc, RwLock};

use axum::Router;
use axum::extract::Request;
use axum::response::Response;
use claviron_runtime::AppState;
use claviron_runtime::registry::{AnyComponent, Component, Error, Result};
use tower::ServiceExt;

/// A provider that contributes Rust-executed Axum routes.
///
/// This capability has no host, listener, TLS, or process-topology semantics.
/// A standalone runner may merge every registered routable into its local
/// router. Edge-bound composition uses its own same-named trait because host
/// selection is a separate capability.
pub trait Routable: AnyComponent<AppState> {
    /// Build this provider's current route contribution.
    ///
    /// `None` means the provider is enabled in the application composition but
    /// contributes no ordinary HTTP routes to this snapshot.
    /// The receiver is the registered shared instance, so handlers may capture
    /// it directly without cloning the provider or resolving it per request.
    fn routes(
        self: Arc<Self>,
        state: &AppState,
    ) -> Result<Option<Router<AppState>>>;
}

/// Bootstrap-time collection of application-owned Rust routes.
#[derive(Default)]
pub struct RoutableRegistry {
    routables: RwLock<Vec<Arc<dyn Routable>>>,
}

impl RoutableRegistry {
    pub fn insert<R>(
        &self,
        routable: Arc<R>,
    ) where
        R: Routable + 'static,
    {
        self.routables
            .write()
            .expect("routable registry lock poisoned")
            .push(routable);
    }

    /// Build one router from the current registration snapshot.
    pub fn build(
        &self,
        state: &AppState,
    ) -> Result<Router<AppState>> {
        let routables = self
            .routables
            .read()
            .expect("routable registry lock poisoned")
            .clone();
        routables.into_iter().try_fold(Router::new(), |router, routable| {
            Ok(match routable.routes(state)? {
                Some(routes) => router.merge(routes),
                None => router,
            })
        })
    }
}

/// Register the route-capability registry before application composition.
pub fn register(state: &AppState) -> Result<()> {
    if state.registry_ref().resolve::<RoutableRegistry>().is_none() {
        state.registry_ref().insert(Arc::new(RoutableRegistry::default()))?;
    }
    Ok(())
}

/// Register one provider for normal lifecycle handling and expose its routes.
pub fn register_routable<R>(
    state: &AppState,
    routable: Arc<R>,
) -> Result<()>
where
    R: Routable + Component<AppState> + 'static,
{
    let routes = state.registry_ref().resolve::<RoutableRegistry>().ok_or_else(|| {
        Error::msg(
            "RoutableRegistry is not registered; call claviron_http::routing::register first"
        )
    })?;
    state.registry_ref().register(routable.clone())?;
    routes.insert(routable);
    Ok(())
}

/// A callable handle to the Axum router below an intercepting Rust route.
///
/// Vhost routing may place the configured host application below it, while a
/// standalone language runner may place ASGI or another application adapter
/// below it. The handle itself knows neither owner.
#[derive(Clone)]
pub struct AxumPipeline {
    inner: Arc<Router>,
}

impl AxumPipeline {
    pub fn new(inner: Router) -> Self {
        Self { inner: Arc::new(inner) }
    }

    pub async fn call(
        &self,
        request: Request,
    ) -> Response {
        match self.inner.as_ref().clone().oneshot(request).await {
            Ok(response) => response,
            Err(error) => match error {},
        }
    }
}
