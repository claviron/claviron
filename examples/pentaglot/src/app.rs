mod asgi;
mod native;

use std::sync::Arc;

use claviron::runtime::{AppState, registry};

pub fn register(state: &AppState) -> registry::Result<()> {
    claviron_deno::register(state)?;
    claviron::edge::register_routable(
        state,
        Arc::new(native::Native::default()),
    )?;
    claviron::edge::register_routable(state, Arc::new(asgi::Asgi::new(state)))
}
