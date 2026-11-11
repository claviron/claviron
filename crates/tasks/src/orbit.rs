//! The controller of an Orbit task of kind `task`: TaskService running the
//! processes of one Link fleet and growing their number on its pressure.
//!
//! The processes create and join the fleet and declare its geometry; the
//! controller only waits for the Link the first one creates, then watches it
//! without a lane. It counts the processes it started, never lanes or
//! application names: a process may claim several lanes and serve several
//! names, or none at all. One pressure episode starts one process; the
//! episode ends when that process claims a lane, or when its slot ends.
//! Idle retirement uses only an explicitly declared duration and capacity
//! wakes. There is no polling or request-path hook.

use std::collections::BTreeMap;
use std::sync::Arc;

use claviron_orbit::config::Workers;
use claviron_orbit::link::application::{
    ApplicationSegment, FleetGrowth, IdleRetirement, Watched,
};
use claviron_runtime::{AppState, registry};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::{OrbitTask, TaskConfig, TaskDispatch, TaskRunner, TaskScope};

/// Slot numbers are a task's `u8`; the controller numbers processes 1..=max.
const SLOT_CAPACITY: u16 = u8::MAX as u16;

/// The fleet-level checks of a task table: one task runs each fleet, and
/// never the fleet hosting TaskService itself.
pub(crate) fn validate_fleets<'a>(
    tasks: impl Iterator<Item = (&'a str, &'a TaskConfig)>,
    hosting_fleet: &str,
) -> registry::Result<()> {
    let mut fleets = std::collections::HashSet::new();
    for (name, config) in tasks {
        let Some(orbit) = &config.orbit else { continue };
        orbitive::FleetName::try_new(&orbit.fleet).map_err(|error| {
            registry::Error::msg(format!("task '{name}': {error}"))
        })?;
        if orbit.fleet == hosting_fleet {
            return Err(registry::Error::msg(format!(
                "task '{name}': fleet '{}' hosts this TaskService; an Orbit task runs another fleet",
                orbit.fleet
            )));
        }
        if !fleets.insert(orbit.fleet.as_str()) {
            return Err(registry::Error::msg(format!(
                "task '{name}': another task already runs fleet '{}'",
                orbit.fleet
            )));
        }
    }
    Ok(())
}

pub(crate) struct Execution {
    pub runner: Arc<TaskRunner>,
    pub state: AppState,
    pub config: Arc<TaskConfig>,
    pub scope: TaskScope,
    pub token: CancellationToken,
}

impl Execution {
    pub async fn run(self) {
        if let Err(error) = self.control().await {
            tracing::error!(task = self.runner.name(), %error, "Orbit task stopped");
        }
    }

    fn spawn(
        &self,
        slots: &mut JoinSet<u8>,
        tokens: &mut BTreeMap<u8, CancellationToken>,
        slot: u8,
        instance: &Arc<TaskConfig>,
    ) {
        let runner = Arc::clone(&self.runner);
        let state = self.state.clone();
        let config = Arc::clone(instance);
        let scope = self.scope;
        let token = self.token.child_token();
        tokens.insert(slot, token.clone());
        // The task's own `one` slot elects this controller; its processes
        // keep stable pool slot identities, so a retained one is reattached.
        slots.spawn(async move {
            runner
                .run_owned_slot(
                    slot,
                    state,
                    config,
                    scope,
                    TaskDispatch::Pool,
                    token,
                )
                .await;
            slot
        });
    }

    async fn control(&self) -> registry::Result<()> {
        let task = self.runner.name();
        let orbit: &OrbitTask =
            self.config.orbit.as_ref().expect("an Orbit task has its block");
        let workers: Workers =
            orbit.workers.expect("a task-kind Orbit task declares workers");
        // TaskService instances on one host may run the same task table;
        // one of them runs a fleet's processes.
        let _owner = claviron_sys::process::ProcessLock::try_acquire(&format!(
            "claviron.orbit.task:{}:{}",
            unsafe { nix::libc::geteuid() },
            orbit.fleet,
        ))
        .map_err(|error| {
            registry::Error::msg(format!(
                "fleet '{}' is run by another TaskService: {error}",
                orbit.fleet
            ))
        })?;
        workers.validate()?;
        let max = workers.maximum(SLOT_CAPACITY);
        if max > SLOT_CAPACITY || workers.initial_count() > max {
            return Err(registry::Error::msg(format!(
                "Orbit workers {}..={max} exceed the task's {SLOT_CAPACITY} slots",
                workers.initial_count()
            )));
        }
        let retained = u16::from(
            crate::process::presence::retained_pool_slots(&self.state, task)
                .map_err(|error| registry::Error::msg(error.to_string()))?,
        );
        if retained > max {
            return Err(registry::Error::msg(format!(
                "{retained} retained processes exceed workers.max {max}; stop them before lowering it"
            )));
        }
        // Each slot runs the plain command; only this controller is Orbit's.
        let instance =
            Arc::new(TaskConfig { orbit: None, ..(*self.config).clone() });
        let mut slots = JoinSet::new();
        let mut tokens = BTreeMap::new();
        let mut started = workers.initial_count().max(retained);
        for slot in 1..=started {
            self.spawn(&mut slots, &mut tokens, slot as u8, &instance);
        }

        let outcome = self
            .grow(
                orbit,
                workers,
                max,
                &mut started,
                &mut slots,
                &mut tokens,
                &instance,
            )
            .await;
        self.token.cancel();
        while let Some(result) = slots.join_next().await {
            if let Err(error) = result {
                tracing::error!(task, %error, "Orbit task process slot failed");
            }
        }
        outcome
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "The controller owns the process slots and cancellation tokens through shutdown. The growth loop borrows that state and its worker policy."
    )]
    async fn grow(
        &self,
        orbit: &OrbitTask,
        workers: Workers,
        max: u16,
        started: &mut u16,
        slots: &mut JoinSet<u8>,
        tokens: &mut BTreeMap<u8, CancellationToken>,
        instance: &Arc<TaskConfig>,
    ) -> registry::Result<()> {
        let task = self.runner.name();
        let segment = tokio::select! {
            biased;
            () = self.token.cancelled() => return Ok(()),
            adopted = ApplicationSegment::adopt_when_created(&orbit.fleet) => {
                adopted.map_err(|error| registry::Error::msg(error.to_string()))?
            }
        };
        let segment = Arc::new(segment);
        let policy = workers.for_owner(
            SLOT_CAPACITY,
            usize::from(segment.spec().fleet_capacity),
            *started,
        )?;
        let mut watcher = segment
            .watch("task")
            .map_err(|error| registry::Error::msg(error.to_string()))?;
        let mut growth = FleetGrowth::publish(&segment, policy);
        let retirement = workers.retire_idle().timeout();
        let mut idle = retirement
            .map(|_| IdleRetirement::new(Arc::clone(&segment), 1))
            .transpose()
            .map_err(|error| registry::Error::msg(error.to_string()))?;
        let mut retiring = None;
        let mut failed = false;
        // However this ends, the fleet is left with no owner to report to.
        struct Unpublish<'a>(&'a ApplicationSegment);
        impl Drop for Unpublish<'_> {
            fn drop(&mut self) {
                self.0.inbox().set_growth_policy(None);
            }
        }
        let _unpublish = Unpublish(&segment);
        let mut processes = self.runner.stop_context().subscribe_processes();
        // The slot started for the open episode.
        let mut asked: Option<u8> = None;

        loop {
            let candidate = if retiring.is_none()
                && asked.is_none()
                && !failed
                && *started > workers.initial_count()
            {
                processes.borrow().get(&(*started as u8)).copied()
            } else {
                None
            };
            let idle_wait = async {
                match (&mut idle, retirement, candidate) {
                    (Some(idle), Some(timeout), Some(pid)) => {
                        idle.wait(pid, timeout).await
                    }
                    _ => std::future::pending().await,
                }
            };
            tokio::select! {
                biased;
                () = self.token.cancelled() => return Ok(()),
                expired = idle_wait => {
                    if let Some(pid) = expired.map_err(|error| registry::Error::msg(error.to_string()))? {
                        let slot = *started as u8;
                        if processes.borrow().get(&slot).copied() == Some(pid)
                            && let Some(token) = tokens.get(&slot)
                        {
                            retiring = Some(slot);
                            token.cancel();
                            tracing::info!(task, slot, pid, "retiring idle Orbit command slot");
                        }
                    }
                }
                finished = slots.join_next(), if !slots.is_empty() => {
                    match finished {
                        Some(Ok(slot)) if retiring == Some(slot) => {
                            tokens.remove(&slot);
                            *started -= 1;
                            retiring = None;
                            growth.set_policy(&segment, policy);
                            growth.arrived(&mut watcher);
                            watcher.take_pressure();
                            continue;
                        }
                        Some(Ok(slot)) => { tokens.remove(&slot); tracing::warn!(task, slot,
                            "Orbit task process slot ended; its attempts are spent, so the fleet grows no further"); },
                        Some(Err(error)) => tracing::error!(task, %error,
                            "Orbit task process slot failed; the fleet grows no further"),
                        None => {}
                    }
                    // A slot that gave up is not refilled by the next episode.
                    failed = true;
                    growth.set_policy(&segment, None);
                    growth.arrived(&mut watcher);
                    asked = None;
                }
                Ok(()) = processes.changed() => {
                    if arrived(&segment, &processes.borrow_and_update(), asked) {
                        growth.arrived(&mut watcher);
                        asked = None;
                    }
                }
                change = watcher.next_change() => match change {
                    Ok(Watched::Directory(_)) => {
                        if arrived(&segment, &processes.borrow(), asked) {
                            growth.arrived(&mut watcher);
                            asked = None;
                        }
                    }
                    Ok(Watched::Pressure) => {
                        if retiring.is_some() { watcher.take_pressure(); continue; }
                        if growth.pressure(&mut watcher).is_none() {
                            continue;
                        }
                        if *started >= max {
                            growth.set_policy(&segment, None);
                            growth.arrived(&mut watcher);
                            continue;
                        }
                        *started += 1;
                        let slot = *started as u8;
                        tracing::info!(task, fleet = %orbit.fleet, slot,
                            "Orbit pressure starts another process");
                        self.spawn(slots, tokens, slot, instance);
                        asked = Some(slot);
                        if *started == max {
                            // Senders stop reporting; this episode still ends
                            // when the process arrives.
                            growth.set_policy(&segment, None);
                        }
                    }
                    Err(error) => return Err(registry::Error::msg(error.to_string())),
                },
            }
        }
    }
}

/// Whether the process started for the open episode claimed a lane.
fn arrived(
    segment: &ApplicationSegment,
    processes: &BTreeMap<u8, u32>,
    asked: Option<u8>,
) -> bool {
    let Some(pid) = asked.and_then(|slot| processes.get(&slot)) else {
        return false;
    };
    let inbox = segment.inbox();
    (0..inbox.lanes())
        .any(|lane| inbox.owner(lane).ok().flatten() == Some(*pid))
}

#[cfg(test)]
#[path = "../tests/orbit.rs"]
mod tests;
