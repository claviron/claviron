//! Service-local metrics adapter.
//!
//! Upstream service code records lifecycle events unconditionally:
//! request status, request error, probe result, health transition, and
//! configured capacity. Keeping those calls unconditional keeps the H1/H2,
//! active probe, recovery probe, and drop-probe paths readable.
//!
//! The `metrics` feature is therefore isolated here. With the feature on,
//! `enabled` publishes events to the owner-local `UPSTREAM_METRICS`; with
//! it off, `noop` provides the same `UpstreamService` methods as empty
//! calls. This avoids scattering `#[cfg(feature = "metrics")]` through the
//! service hot paths while still compiling the crate without the metrics
//! dependency.

#[cfg(feature = "metrics")]
mod enabled;
#[cfg(not(feature = "metrics"))]
mod noop;
#[cfg(feature = "metrics")]
mod orbit;
#[cfg(feature = "metrics")]
mod provider;
#[cfg(feature = "metrics")]
mod store;

#[cfg(feature = "metrics")]
pub use provider::register;
#[cfg(feature = "metrics")]
pub use store::{UPSTREAM_METRICS, UpstreamMetrics};
#[cfg(feature = "metrics")]
mod export;
#[cfg(feature = "metrics")]
pub use export::*;
#[cfg(feature = "metrics")]
mod scalars;
#[cfg(feature = "metrics")]
pub use scalars::Scalars;

mod slot;
pub(crate) use slot::OriginMetrics;
