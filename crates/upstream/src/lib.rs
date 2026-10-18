//! HTTP upstream reverse-proxy primitive for Claviron.
//!
//! The crate owns the outbound upstream pool contract: admission, lifecycle,
//! dirty-drop safety, generation, and availability metrics. It does not own transport
//! listeners or vhost resolution. The optional `axum` feature exposes the
//! merged-router binding consumed by `claviron-edge`.

// What a consumer never names: the compiler that turns the file into
// profiles, the process-local slot state, and the health machinery that keeps
// it current.
mod app;
mod compile;
mod health;
mod provider;
mod state;

// The public surface, arranged the way the crate is: how a host reaches it,
// what an operator declares, who the origins are, what one request touches on
// its way to them, how membership is resolved, and what it reports.
pub mod binding;
pub mod config;
pub mod metrics;
pub mod origin;
pub mod proxy;
pub mod resolve;

/// Registering the provider is the whole entry point; everything else is
/// reached through the module that owns it.
pub use provider::{UpstreamProvider, register};
