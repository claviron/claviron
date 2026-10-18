//! OriginId-indexed atomics and the configuration-bound bank handle.
#[derive(Clone, Debug)]
pub(crate) struct OriginMetrics {
    pub(crate) index: u16,
    #[cfg(feature = "metrics")]
    pub(crate) bank:
        std::sync::OnceLock<std::sync::Arc<super::store::ProfileMetrics>>,
}
impl OriginMetrics {
    pub(crate) fn new(index: u16) -> Self {
        Self {
            index,
            #[cfg(feature = "metrics")]
            bank: Default::default(),
        }
    }
    #[cfg(feature = "metrics")]
    pub(crate) fn counters(&self) -> Option<&OriginSlot> {
        self.bank.get()?.origins.get(usize::from(self.index))
    }
}
// Config equality excludes live metrics, including which store owns the bank.
impl PartialEq for OriginMetrics {
    fn eq(
        &self,
        other: &Self,
    ) -> bool {
        self.index == other.index
    }
}
impl Eq for OriginMetrics {}
#[cfg(feature = "metrics")]
use std::sync::atomic::{AtomicU8, AtomicU64};
// Fixed status categories are the export schema, not an operator limit.
#[cfg(feature = "metrics")]
pub(super) const REQUEST_LABELS: [&str; 7] =
    ["1xx", "2xx", "3xx", "4xx", "5xx", "error", "other"];
// Probe categories are the four mechanisms in the configuration schema.
#[cfg(feature = "metrics")]
pub(super) const PROBE_LABELS: [&str; 4] = [
    crate::health::probe::ProbeKind::Drop.as_str(),
    crate::health::probe::ProbeKind::Recovery.as_str(),
    crate::health::probe::ProbeKind::Active.as_str(),
    "l7",
];
// A probe has exactly one of these protocol outcomes.
#[cfg(feature = "metrics")]
pub(super) const OUTCOME_LABELS: [&str; 3] = [
    crate::health::probe::ProbeOutcome::Ok.as_str(),
    crate::health::probe::ProbeOutcome::Fail.as_str(),
    crate::health::probe::ProbeOutcome::Timeout.as_str(),
];
// These reasons correspond to the RetryCause variants, not configurable capacity.
#[cfg(feature = "metrics")]
pub(super) const RETRY_LABELS: [&str; 4] = [
    crate::config::profile::retry::RetryCause::Connect.as_str(),
    crate::config::profile::retry::RetryCause::Refused.as_str(),
    crate::config::profile::retry::RetryCause::ResetBeforeResponse.as_str(),
    crate::config::profile::retry::RetryCause::ResponseHeaderTimeout.as_str(),
];
#[cfg(feature = "metrics")]
#[derive(Debug)]
pub(crate) struct OriginSlot {
    pub(crate) requests: [AtomicU64; REQUEST_LABELS.len()],
    pub(crate) probes: [[AtomicU64; OUTCOME_LABELS.len()]; PROBE_LABELS.len()],
    pub(crate) transitions: [AtomicU64; 2],
    pub(crate) health: AtomicU8,
    pub(crate) serving: AtomicU8,
    pub(crate) retries: [AtomicU64; RETRY_LABELS.len()],
    pub(crate) ejections: AtomicU64,
    pub(crate) breaker_rejections: AtomicU64,
}
#[cfg(feature = "metrics")]
impl Default for OriginSlot {
    fn default() -> Self {
        Self {
            requests: Default::default(),
            probes: Default::default(),
            transitions: Default::default(),
            health: AtomicU8::new(0),
            serving: AtomicU8::new(1),
            retries: Default::default(),
            ejections: AtomicU64::new(0),
            breaker_rejections: AtomicU64::new(0),
        }
    }
}

// The fourth metric ordinal is L7; keeping it separate prevents a serving
// verdict from entering the reachability-only ProbeKind/apply_outcome API.
#[cfg(feature = "metrics")]
pub(crate) const L7_PROBE_INDEX: usize = 3;
