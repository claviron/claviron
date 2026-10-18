//! Orbit-owned runtime policy from `orbit.yaml`.
//!
//! Physical SHM geometry remains a build-time contract shared by every peer.

use std::time::Duration;

use claviron_runtime::registry;
use serde::Deserialize;

/// Runtime policy prepared by Congif before the Orbit providers boot.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct OrbitConfig {
    pub service_snapshots: ServiceSnapshotPolicy,
}

impl OrbitConfig {
    pub fn validate(&self) -> registry::Result<()> {
        self.service_snapshots.validate()?;
        Ok(())
    }
}

fn default_publish_interval() -> Duration {
    Duration::from_secs(1)
}

fn default_max_age() -> Duration {
    Duration::from_secs(5)
}

/// Observation cadence and freshness contract for fleet service snapshots.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ServiceSnapshotPolicy {
    #[serde(default = "default_publish_interval", with = "humantime_serde")]
    publish_interval: Duration,
    #[serde(default = "default_max_age", with = "humantime_serde")]
    max_age: Duration,
}

impl ServiceSnapshotPolicy {
    pub fn new(
        publish_interval: Duration,
        max_age: Duration,
    ) -> Self {
        Self { publish_interval, max_age }
    }

    pub fn publish_interval(self) -> Duration {
        self.publish_interval
    }

    pub fn max_age(self) -> Duration {
        self.max_age
    }

    pub(crate) fn validate(self) -> registry::Result<()> {
        if self.publish_interval.is_zero() {
            return Err(registry::Error::msg(
                "service snapshot publish interval must be greater than zero",
            ));
        }
        if tokio::time::Instant::now()
            .checked_add(self.publish_interval)
            .is_none()
        {
            return Err(registry::Error::msg(
                "service snapshot publish interval exceeds the runtime clock",
            ));
        }
        if self.max_age.is_zero() {
            return Err(registry::Error::msg(
                "service snapshot max age must be greater than zero",
            ));
        }
        Ok(())
    }
}

impl Default for ServiceSnapshotPolicy {
    fn default() -> Self {
        Self {
            publish_interval: default_publish_interval(),
            max_age: default_max_age(),
        }
    }
}
