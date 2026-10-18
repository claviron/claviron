//! What one request holds on the service while it runs.
//!
//! A lease is the request's side of the service's life: it pins the epoch it
//! was admitted under, the gate permit that keeps that epoch from finishing
//! its drain, and the optional breaker and origin admissions it acquired on
//! the way. Every leg carries one — `h1`, `h2` and the body wrappers all
//! name it — so it lives beside `failure` as request vocabulary rather than
//! inside the service lifecycle that grants it.

use claviron_runtime::gate::Permit;

use super::service::lifecycle::ServiceEpoch;
use super::service::{breaker, transport};

pub(crate) struct RequestLease {
    pub(crate) epoch: std::sync::Arc<ServiceEpoch>,
    pub(crate) _permit: Permit,
    pub(crate) request: Option<InputLease>,
    pub(crate) breaker: Option<std::sync::Arc<breaker::Admission>>,
    pub(crate) origin: Option<transport::OriginLease>,
}

/// The request-body leg, tracked apart from the response leg: H2 may answer
/// before a streaming writer has finished, and neither needs an `Arc<Permit>`.
pub(crate) struct InputLease {
    pub(crate) origin: Option<Permit>,
    pub(crate) breaker: Option<std::sync::Arc<breaker::Admission>>,
    pub(crate) _permit: Permit,
}
