//! The fixed metric totals carried by the fleet snapshot wire record.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct Scalars {
    pub requests_total: u64,
    pub request_errors_total: u64,
    pub probes_total: u64,
    pub probe_failures_total: u64,
    pub state_transitions_total: u64,
    pub configured_live_capacity: u64,
    #[serde(default)]
    pub retries_total: u64,
    #[serde(default)]
    pub ejections_total: u64,
}
