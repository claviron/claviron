//! Native HTTP workloads, independent of execution language and transport.
//!
//! [`BenchService`] is one service for every process shape: a `HostRoutable`
//! that answers on the `vhost.yaml` hosts `bench.yaml` selects where the
//! process serves hosts, and contributes host-free routes in a runner. Both use
//! the same handlers; neither opens a listener or executes a script. Selecting
//! a Cargo feature exposes the service, not an implicit registration. Edges
//! serving benchmarks disable access logging for those hosts in their own
//! configuration.
#![forbid(unsafe_code)]

mod handlers;
mod service;

pub use handlers::{Ask, routes};
pub use service::{BenchService, Config};
