//! Prepared quota stores. No HTTP identity, provider loop or implicit reset.

use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

use orbitive::FleetName;
use orbitive::quota::LocalGuard;
pub use orbitive::quota::{
    CasBudget, Decision, Error, Geometry, Key, Policy, QuotaStore,
};
#[cfg(unix)]
use orbitive::quota::{FleetGuard, FleetQuota, ShmOptions, ShmResource};
use serde::{Deserialize, Deserializer};

/// Immutable store definition. Every bound is explicit; construction is cold-path work.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct QuotaConfig {
    pub units: NonZeroU64,
    #[serde(with = "humantime_serde")]
    pub period: Duration,
    pub burst: NonZeroU64,
    pub table: QuotaTable,
    pub cas_attempts: NonZeroUsize,
    pub store: QuotaBacking,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct QuotaTable {
    pub slots: NonZeroUsize,
    pub shards: NonZeroUsize,
    pub key_bytes: NonZeroUsize,
    pub probes: NonZeroUsize,
}

/// An explicitly owned quota-only SHM namespace, not the runtime fleet's kinds.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Hash)]
#[serde(deny_unknown_fields)]
pub struct QuotaResource {
    #[serde(deserialize_with = "deserialize_fleet")]
    pub fleet: FleetName,
    pub kind: u8,
}

fn deserialize_fleet<'de, D: Deserializer<'de>>(
    de: D
) -> Result<FleetName, D::Error> {
    let name = String::deserialize(de)?;
    name.parse().map_err(serde::de::Error::custom)
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum QuotaBacking {
    // Struct form makes Serde enforce unknown-field rejection for this mode.
    LocalGuard {},
    FleetGuard {
        resource: QuotaResource,
        owners: NonZeroUsize,
        recovery_attempts: usize,
    },
    FleetQuota {
        resource: QuotaResource,
        owners: NonZeroUsize,
        recovery_attempts: usize,
    },
}

impl QuotaConfig {
    /// Pure structural validation; this neither attaches to nor creates SHM.
    pub fn validate(&self) -> Result<(), Error> {
        self.geometry()?;
        self.policy()?;
        #[cfg(not(unix))]
        if !matches!(self.store, QuotaBacking::LocalGuard {}) {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "fleet quota stores require Unix shared memory",
            )));
        }
        Ok(())
    }

    pub fn geometry(&self) -> Result<Geometry, Error> {
        Geometry::new(
            self.table.slots,
            self.table.shards,
            self.table.key_bytes,
            self.table.probes,
        )
    }

    pub fn policy(&self) -> Result<Policy, Error> {
        Policy::new(self.units, self.period, self.burst)
    }

    pub fn resource(&self) -> Option<QuotaResource> {
        match &self.store {
            QuotaBacking::LocalGuard {} => None,
            QuotaBacking::FleetGuard { resource, .. }
            | QuotaBacking::FleetQuota { resource, .. } => Some(*resource),
        }
    }
}

/// One prepared authority. New local stores are independent; clones share debt.
/// Matching fleet resources attach to preserved debt, never reset or unlink it.
/// The owner retains this handle for its serving lifetime; it is not a Runnable.
#[derive(Clone)]
pub struct Quota {
    config: QuotaConfig,
    backend: Backend,
}

#[derive(Clone)]
enum Backend {
    Local(LocalGuard),
    #[cfg(unix)]
    Guard(FleetGuard),
    #[cfg(unix)]
    Strict(FleetQuota),
}

impl Quota {
    pub fn new(config: QuotaConfig) -> Result<Self, Error> {
        config.validate()?;
        let geometry = config.geometry()?;
        let policy = config.policy()?;
        let budget = CasBudget::new(config.cas_attempts);
        let backend = match &config.store {
            QuotaBacking::LocalGuard {} => {
                Backend::Local(LocalGuard::new(geometry, policy, budget)?)
            }
            #[cfg(unix)]
            QuotaBacking::FleetGuard {
                resource,
                owners,
                recovery_attempts,
            } => Backend::Guard(FleetGuard::open(
                ShmResource::new(resource.fleet, resource.kind),
                geometry,
                policy,
                budget,
                ShmOptions::new(*owners, *recovery_attempts),
            )?),
            #[cfg(unix)]
            QuotaBacking::FleetQuota {
                resource,
                owners,
                recovery_attempts,
            } => Backend::Strict(FleetQuota::open(
                ShmResource::new(resource.fleet, resource.kind),
                geometry,
                policy,
                budget,
                ShmOptions::new(*owners, *recovery_attempts),
            )?),
            #[cfg(not(unix))]
            _ => {
                unreachable!("configuration validation rejects fleet backends")
            }
        };
        Ok(Self { config, backend })
    }

    pub fn config(&self) -> &QuotaConfig {
        &self.config
    }

    fn backend(&self) -> &dyn QuotaStore {
        match &self.backend {
            Backend::Local(store) => store,
            #[cfg(unix)]
            Backend::Guard(store) => store,
            #[cfg(unix)]
            Backend::Strict(store) => store,
        }
    }
}

impl QuotaStore for Quota {
    fn policy(&self) -> Policy {
        self.backend().policy()
    }
    fn geometry(&self) -> Geometry {
        self.backend().geometry()
    }
    fn try_consume(
        &self,
        key: Key<'_>,
        cost: NonZeroU64,
    ) -> Result<Decision, Error> {
        self.backend().try_consume(key, cost)
    }
}

#[cfg(test)]
#[path = "../tests/quota.rs"]
mod tests;
