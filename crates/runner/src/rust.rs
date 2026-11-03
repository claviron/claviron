//! Registration of a Rust application's terminal service.

#[cfg(feature = "axum")]
pub mod axum;
#[cfg(feature = "warp")]
pub mod warp;

use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use claviron_http::RouteBodyError;
use claviron_runtime::{AppState, Bootable, Component, registry};
use http::Response;
use tower::Service;

use crate::AppRequest;

/// Register finite preparation after service configurations have been applied.
pub fn register(
    state: &AppState,
    options: crate::RunnerOptions,
    application: ApplicationRegistration,
    sessions: Option<crate::orbit::OrbitSession>,
) -> registry::Result<()> {
    state.registry_ref().register(Arc::new(ApplicationSetup {
        options,
        application: Mutex::new(Some(PendingApplication {
            register: application,
            sessions,
        })),
    }))?;
    Ok(())
}

struct ApplicationSetup {
    options: crate::RunnerOptions,
    application: Mutex<Option<PendingApplication>>,
}

struct PendingApplication {
    register: ApplicationRegistration,
    sessions: Option<crate::orbit::OrbitSession>,
}

impl Component<AppState> for ApplicationSetup {
    type Deps = Arc<crate::Routing>;

    fn name(&self) -> &'static str {
        "RustApplication"
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<AppState, Self::Deps>> {
        Some(self)
    }
}

#[async_trait]
impl Bootable<AppState, Arc<crate::Routing>> for ApplicationSetup {
    async fn boot(
        &self,
        _: Arc<crate::Routing>,
        state: &AppState,
    ) -> registry::Result<()> {
        let application = self
            .application
            .lock()
            .map_err(|_| {
                registry::Error::msg(
                    "Rust application preparation lock poisoned",
                )
            })?
            .take()
            .ok_or_else(|| {
                registry::Error::msg("Rust application is already prepared")
            })?;
        (application.register)(state, application.sessions)?;
        crate::launch::register_trust(state, &self.options)?;
        crate::launch::register_doors(state, &self.options, "rust", None)
    }
}

/// A startup registration, consumed before any ingress starts.
pub type Registration =
    Box<dyn FnOnce(&AppState) -> registry::Result<()> + Send>;

pub type ApplicationRegistration = Box<
    dyn FnOnce(
            &AppState,
            Option<crate::orbit::OrbitSession>,
        ) -> registry::Result<()>
        + Send,
>;

/// Select the same host-free router representation that Edge consumes.
pub fn router<F, R>(build: F) -> ApplicationRegistration
where
    F: FnOnce(AppState) -> R + Send + 'static,
    R: Into<claviron_http::RustRouter> + 'static,
{
    Box::new(move |state, sessions| {
        crate::routing::register_prepared_router(
            state,
            build(state.clone()).into(),
            sessions,
        )
    })
}

/// Prepare another Rust HTTP service, with optional registered routes in front.
pub fn other<S, B>(service: S) -> ApplicationRegistration
where
    S: Service<AppRequest, Response = Response<B>, Error = Infallible>
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send + 'static,
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<RouteBodyError>,
{
    Box::new(move |state, sessions| {
        crate::routing::register_other(state, service, sessions)
    })
}
