//! Pure-data Link receiver selection, shared by configuration and callers.

use serde::{Deserialize, Serialize};

/// How a caller balances new exchanges across an application's receivers.
/// This does not select the owner's worker policy (`workers.policy`).
#[derive(
    Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
pub enum LinkBalance {
    /// Keep the caller's preferred receiver busy before spilling to others.
    /// With unbounded receivers, choose the less loaded of two candidates.
    #[default]
    First,
    /// Rotate through receivers, skipping unavailable or saturated ones.
    RoundRobin,
}
