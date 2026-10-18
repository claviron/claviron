//! Owns noop policy and runtime helpers.

use crate::health::probe::{ProbeKind, ProbeOutcome};
use crate::origin::OriginHealth;
use crate::proxy::service::UpstreamService;

impl UpstreamService {
    pub(crate) fn register_configured_capacity(&self) {}

    pub(crate) fn record_probe_result(
        &self,
        _origin_index: usize,
        _source: ProbeKind,
        _result: ProbeOutcome,
    ) {
    }

    pub(crate) fn record_health_transition(
        &self,
        _origin_index: usize,
        _from: OriginHealth,
        _to: OriginHealth,
    ) {
    }

    pub(crate) fn record_serving_transition(
        &self,
        _: usize,
        _: crate::origin::Serving,
    ) {
    }
    pub(crate) fn record_l7_probe_result(
        &self,
        _: usize,
        _: ProbeOutcome,
    ) {
    }
    pub(crate) fn record_request_status(
        &self,
        _origin_index: usize,
        _status: http::StatusCode,
    ) {
    }

    pub(crate) fn record_retry(
        &self,
        _index: usize,
        _reason: crate::config::profile::retry::RetryCause,
    ) {
    }
    pub(crate) fn record_outlier_ejection(
        &self,
        _index: usize,
    ) {
    }
    pub(crate) fn record_breaker_rejection(
        &self,
        _: usize,
    ) {
    }
    pub(crate) fn record_request_error(
        &self,
        _origin_index: usize,
    ) {
    }
}
