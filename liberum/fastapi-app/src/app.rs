mod extensions;
mod listeners;
pub mod routes;
mod services;

use claviron::AppState;
use claviron::runtime::registry;

use crate::app::services::CounterService;

/// Register this application's Rust routes, providers, and services here.
pub fn configure(state: &AppState) -> registry::Result<()> {
    // Your own services go in the same registry Claviron boots its own from,
    // and get the same lifecycle: a plain `Component` is booted in dependency
    // order and resolved by type; one that also declares `Runnable` gets a
    // runtime-owned generation, started after every boot has completed and
    // ended by cancellation rather than by being dropped.
    //
    // A counter needs none of that — it is a `u64` behind an `Arc`, and a
    // route could just as well hold it. It is a provider here to show the
    // shape, not because the work asks for one.
    CounterService::register(state)?;
    listeners::register(state)
}
