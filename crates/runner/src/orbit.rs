//! The application behind the transport's door with no socket.

use std::sync::Arc;

use async_trait::async_trait;
use claviron_orbit::placement::{Scope, Scoped};
use claviron_runtime::{
    AppState, Component, FleetIncarnation, RunContext, Runnable,
    ServiceStartPolicy, registry,
};
use claviron_transport::orbit::OrbitDoor;
pub use claviron_transport::orbit::{
    EdgeShutdownInitiated, Endpoint, Handshake, LinkCommandReceived,
    OrbitSession, SessionShutdown,
};

use crate::routing::Application;

/// This process's place in its configured transport fleet, as a runnable
/// Claviron owns.
pub struct OrbitIngress(OrbitDoor);

impl OrbitIngress {
    /// `runtime` is what the lane says answers the name (`php`, `asgi`); a
    /// process of another runtime given the same name is refused at its join
    /// rather than splitting this one's requests. `max_in_flight` bounds the
    /// exchanges taken and not yet answered; `None` is no bound.
    pub fn new(
        application: impl Into<String>,
        runtime: &'static str,
        max_in_flight: Option<usize>,
    ) -> Self {
        Self(OrbitDoor::new(application, runtime, max_in_flight))
    }
}

impl Component<AppState> for OrbitIngress {
    type Deps = (Arc<Application>, Option<Arc<FleetIncarnation>>);

    fn name(&self) -> &'static str {
        "OrbitIngress"
    }

    fn as_scoped(&self) -> Option<&dyn Scoped> {
        Some(self)
    }

    fn as_runnable(
        self: Arc<Self>
    ) -> Option<Arc<dyn Runnable<AppState, Self::Deps>>> {
        Some(self)
    }
}

/// Workers serve on their lanes; the master watches the directory. Without
/// this the runtime would place it on workers only, its default.
impl Scoped for OrbitIngress {
    fn scope(&self) -> Scope {
        Scope::Any
    }
}

#[async_trait]
impl Runnable<AppState, <Self as Component<AppState>>::Deps> for OrbitIngress {
    fn start_policy(
        &self,
        _state: &AppState,
    ) -> ServiceStartPolicy {
        ServiceStartPolicy::Automatic
    }

    async fn run(
        self: Arc<Self>,
        (application, incarnation): <Self as Component<AppState>>::Deps,
        state: AppState,
        context: RunContext,
    ) -> registry::Result<()> {
        let sessions = application.orbit_session();
        self.0.serve(application, sessions, incarnation, state, context).await
    }
}
