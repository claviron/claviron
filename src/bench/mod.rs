//! Native HTTP workloads, independent of execution language and transport.
//!
//! Edge registration reads `bench.yaml` hosting through the existing Congif
//! and Edge contracts. Runner registration contributes host-free routes.
//! Both use the same handlers; neither opens a listener or executes a script.
//! Selecting a Cargo feature exposes an adapter, not an implicit registration.
//! Edge benchmark host declarations require explicit `access_log: off`;
//! normal host logging is unchanged. External edges serving runner benchmarks
//! must disable access logging for those test hosts in their own configuration.
#![forbid(unsafe_code)]

#[cfg(feature = "edge")]
pub mod edge;
mod handlers;
#[cfg(feature = "runner")]
pub mod runner;

pub use handlers::{Ask, baseline_routes, routes};
