//! Shared client-pool configuration and Orbitive's code-first pool API.
//!
//! Each consumer creates its own runtime pool. Sharing a configuration type
//! does not share connections or lifecycle ownership; a pool declared with
//! `scope: fleet` shares its request budget across the fleet
//! ([`AdmittedPool`], with `clients-fleet`).

pub use orbitive::client_pool::*;

#[cfg(feature = "clients-fleet")]
mod admitted;
mod config;
#[cfg(feature = "clients-fleet")]
pub use admitted::{AdmissionError, AdmittedLease, AdmittedPool};
pub use config::{HttpClientConfig, PoolConfig, PoolScope};
