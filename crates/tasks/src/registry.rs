//! Task registry with static registration via `inventory` crate
//!
//! Tasks are registered using the `#[task]` attribute macro from `claviron-macros`.

pub use claviron_orbit::placement::{
    Dispatch as TaskDispatch, Scope as TaskScope,
};

use super::runner::TaskRunner;
/// Compile-time registration entry collected by the [`inventory`] crate.
///
/// The `#[task]` proc-macro (in `claviron-macros`) emits one
/// `inventory::submit!` of `TaskRegistration` per annotated function.
/// At service boot, `TaskServiceBuilder::register_discovered_tasks`
/// walks `inventory::iter::<TaskRegistration>` to find every task in
/// the binary — no central list, no manual wiring, dead-code-elimination
/// safe (the inventory submission keeps the registration alive even
/// when the function is only reachable through the macro).
///
/// Why a `fn() -> TaskRunner` and not `static TaskRunner`: `TaskRunner`
/// holds heap allocations (`Arc`, `Mutex`, `Semaphore`) that can't be
/// const-evaluated. The factory pointer defers construction to runtime.
pub struct TaskRegistration {
    pub name: &'static str,
    pub scope: TaskScope,
    pub dispatch: TaskDispatch,
    pub create_runner: fn() -> TaskRunner,
    /// The task's own `tasks.yaml` entry, as YAML, for when the file has
    /// none; `None` leaves the task to the file.
    pub config: Option<&'static str>,
}

// Collect all task registrations
inventory::collect!(TaskRegistration);

#[cfg(test)]
#[path = "../tests/registry.rs"]
mod tests;
