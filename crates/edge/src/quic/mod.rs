//! HTTP/3 (QUIC) transport. H3 is Claviron's preferred modern lane when a
//! host advertises it with `Alt-Svc`, but it is intentionally selective:
//! UDP, 0-RTT, authority/SNI coalescing, and streamed request bodies are policy
//! decisions, not server-wide defaults. Early-data requests rejected by vhost
//! policy return `425 Too Early`; authority/SNI mismatches return `421`.
//!
//! `H3Server` registers cheaply and prepares its config through Congif.
//! Run and reload resolve runtime dependencies and build coherent listener
//! snapshots; no provider boot or separate HTTP config provider is needed.
//!
//! The important bridge lives in `listener.rs` + `request.rs`: Quinn/H3 streams
//! are resolved directly into Claviron's shared request shape, injected with
//! vhost/connection extensions, and admitted by the shared HostService before dispatch to the Axum/Tower router
//! or the axum-free bypass chain. There is no sidecar proxy between QUIC and
//! the application pipeline.

mod listener;
mod tls;
mod types;

// Native request/response stream adapter; host dispatch lives in vhosts.
mod request;
mod wt;

use claviron_runtime::{AppState, Register};
pub use types::H3Server;
pub fn register(state: &AppState) -> claviron_runtime::registry::Result<()> {
    <H3Server as Register<AppState>>::register(state)
}
