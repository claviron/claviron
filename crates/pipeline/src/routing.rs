//! Host-free Axum application route composition.

use std::sync::{Arc, RwLock};

use axum::Router;
use claviron_runtime::AppState;
use claviron_runtime::registry::{AnyComponent, Component, Error, Result};
use claviron_types::Host;

/// A provider that contributes Rust-executed Axum routes.
///
/// This capability has no host, listener, TLS, or process-topology semantics.
/// A runner merges every registered routable into its local router. Hosts are
/// a separate capability, [`HostRoutable`].
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

/// Routes that also select the hosts they answer on, where a process serves
/// hosts. The hosts are declared in `vhost.yaml`; this only selects their keys.
///
/// A process that serves hosts puts these routes in front of each selected
/// host's own owner; a path or method they do not declare goes on to it. A
/// process without hosts serves them as any [`Routable`].
pub trait HostRoutable: Routable {
    /// Select a canonical vhost key while preparing the routing snapshot,
    /// never per request.
    fn match_host(
        &self,
        key: &Host,
    ) -> bool;
}

/// Bootstrap-time collection of application-owned Rust routes.
#[derive(Default)]
pub struct RoutableRegistry {
    routables: RwLock<Vec<Arc<dyn Routable>>>,
    hosts: RwLock<Vec<Arc<dyn HostRoutable>>>,
}

impl RoutableRegistry {
    /// A [`HostRoutable`] is also an ordinary routable for a process without
    /// hosts.
    pub fn insert_host<R>(
        &self,
        routable: Arc<R>,
    ) where
        R: HostRoutable + 'static,
    {
        self.hosts
            .write()
            .expect("routable registry lock poisoned")
            .push(routable.clone());
        self.insert(routable);
    }

    /// The routables that select hosts, for a process that serves hosts.
    pub fn host_routables(&self) -> Vec<Arc<dyn HostRoutable>> {
        self.hosts.read().expect("routable registry lock poisoned").clone()
    }

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

    /// Build the registered contributions, without a router when none contributes.
    pub fn build(
        &self,
        state: &AppState,
    ) -> Result<Option<Router<AppState>>> {
        let routables = self
            .routables
            .read()
            .expect("routable registry lock poisoned")
            .clone();
        routables.into_iter().try_fold(
            None::<Router<AppState>>,
            |router, routable| {
                Ok(match routable.routes(state)? {
                    Some(routes) => Some(match router {
                        Some(router) => router.merge(routes),
                        None => routes,
                    }),
                    None => router,
                })
            },
        )
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
            "RoutableRegistry is not registered; call claviron_pipeline::routing::register first"
        )
    })?;
    state.registry_ref().register(routable.clone())?;
    routes.insert(routable);
    Ok(())
}

/// Register a [`HostRoutable`] for normal lifecycle handling: its routes go to
/// the hosts it selects where the process serves hosts, and host-free routes
/// otherwise.
pub fn register_host_routable<R>(
    state: &AppState,
    routable: Arc<R>,
) -> Result<()>
where
    R: HostRoutable + Component<AppState> + 'static,
{
    let routes = state.registry_ref().resolve::<RoutableRegistry>().ok_or_else(|| {
        Error::msg(
            "RoutableRegistry is not registered; call claviron_pipeline::routing::register first"
        )
    })?;
    state.registry_ref().register(routable.clone())?;
    routes.insert_host(routable);
    Ok(())
}
