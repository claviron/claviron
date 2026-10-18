//! Host-free registration into an existing runner's route registry.

use std::sync::Arc;

use axum::Router;
use claviron_http::routing::{Routable, register_routable};
use claviron_runtime::registry::Component;
use claviron_runtime::{AppState, Register, registry};

/// Registers native baselines without choosing a listener, host or interpreter.
pub struct BenchService {
    alias_prefix: Option<&'static str>,
}

impl BenchService {
    /// Preserve an application's existing URL prefix beside the common paths.
    /// The prefix is a programmer-selected Axum nesting path, prepared once.
    pub fn register_with_alias_prefix(
        state: &AppState,
        prefix: &'static str,
    ) -> registry::Result<()> {
        Self::register_routes(state, Some(prefix))
    }

    fn register_routes(
        state: &AppState,
        alias_prefix: Option<&'static str>,
    ) -> registry::Result<()> {
        register_routable(state, Arc::new(Self { alias_prefix }))?;
        tracing::info!("native bench routes registered (runner)");
        Ok(())
    }
}

impl Register<AppState> for BenchService {
    fn register(state: &AppState) -> registry::Result<()> {
        Self::register_routes(state, None)
    }
}

impl Component<AppState> for BenchService {
    type Deps = ();

    fn name(&self) -> &'static str {
        "BenchService"
    }
}

impl Routable for BenchService {
    fn routes(
        self: Arc<Self>,
        _state: &AppState,
    ) -> registry::Result<Option<Router<AppState>>> {
        let mut router = crate::routes();
        if let Some(prefix) = self.alias_prefix {
            router = router.nest(prefix, crate::baseline_routes());
        }
        Ok(Some(router.with_state(())))
    }
}
