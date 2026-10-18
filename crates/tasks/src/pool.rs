//! Service-owned state machine for fleet-coordinated task execution.
//!
//! Each `TaskService` has one lightweight controller per coordinated task and
//! runtime, not one waiter per task slot. It locks the stable slot keys
//! `task:<name>:pool:<slot>`, renews the slots it owns, and runs only those
//! task triggers. `one` always desires one slot; `pool` desires its configured
//! daemon capacity. Standalone uses the same controller without Orbit locks.
//!
//! A locked slot retains its ownership lease while `TaskRunner` performs
//! local attempts and backoff. When that runner ends (success, terminal exit,
//! panic, or exhausted attempts), the controller drops the lock and exposes
//! the missing slot to the next reconciliation. Every eligible runtime,
//! including the former owner, may acquire the next ownership session; its
//! attempt counter starts again at one. Owner process death bypasses task
//! attempts and relies on lease expiry for peer failover.

mod controller;
mod reload;

pub(crate) use controller::PoolTask;
pub(crate) use reload::PoolReloader;
