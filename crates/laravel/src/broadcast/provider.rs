use std::sync::Arc;

use claviron_php_embed::extension::register_extension;
use claviron_runtime::{AppState, Register, registry};

use super::BroadcastExtension;

/// Connects PHP's native broadcaster to the existing Broadcast service.
/// Each PHP publication selects its catalog application by its own key.
pub struct LaravelBroadcastProvider;

impl Register<AppState> for LaravelBroadcastProvider {
    fn register(state: &AppState) -> registry::Result<()> {
        if state.is_master() {
            return Ok(());
        }
        register_extension(state, Arc::new(BroadcastExtension))
            .map_err(registry::Error::msg)
    }
}
