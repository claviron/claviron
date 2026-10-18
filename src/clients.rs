//! Shared client-pool configuration and Orbitive's code-first pool API.
//!
//! Each consumer creates its own runtime pool. Sharing a configuration type
//! does not share connections, admission budgets or lifecycle ownership.

pub use orbitive::client_pool::*;

mod config;
pub use config::PoolConfig;
