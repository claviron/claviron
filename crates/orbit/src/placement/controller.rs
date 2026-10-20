use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use claviron_runtime::registry::{Component, Error, Result};
use claviron_runtime::{AppState, Register, RunContext};
use tokio::task::{Id, JoinSet};
use tokio::time::{Instant, MissedTickBehavior, interval_at};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use super::{
    Dispatch, DispatchPolicy, DispatchRequest, Dispatcher, ServiceExecution,
};
use crate::lock::{
    Lock, LockAcquire, LockKey, LockOwner, LockTransition, LockType, OrbitLock,
};
use crate::{
    OrbitStateExt,
    service::{FleetMemberRole, current_members},
};

struct ServiceSlot;
impl LockType for ServiceSlot {
    const NAMESPACE: &'static str = "claviron.services.dispatch";
}

/// Orbit implementation of Continuo's generation-long dispatch contract.
///
/// Services supply immutable policy snapshots for their managed generations.
/// This adapter owns coordination, not a config reader or service policy map.
///
/// [INCARNATION_MASKING] separates stable service-slot keys from exact lease
/// owners; a service declares dispatch intent rather than a worker identity.
///
/// [INCARNATION_MASKING]: https://github.com/claviron/claviron/blob/main/docs/design/INCARNATION_MASKING.md
#[derive(Default)]
pub struct OrbitDispatcher;

impl Component<AppState> for OrbitDispatcher {
    type Deps = ();

    fn name(&self) -> &'static str {
        "OrbitDispatcher"
    }
}

impl OrbitDispatcher {
    pub fn new() -> Self {
        Self
    }

    fn policy(
        &self,
        request: &DispatchRequest,
    ) -> Result<DispatchPolicy> {
        request.policy.ok_or_else(|| {
            Error::msg(format!(
                "dispatch policy missing for '{}'; provide it through Dispatched::dispatch_policy",
                request.name
            ))
        })
    }
}

/// Install the existing Orbit coordination adapter without a policy file.
impl Register<AppState> for OrbitDispatcher {
    fn register(state: &AppState) -> Result<()> {
        if state.registry_ref().resolve::<Self>().is_some() {
            return Ok(());
        }
        register_dispatcher(state, Arc::new(Self::new()))
    }
}

/// Register the adapter and managed lock resources before provider boot.
/// Claviron's server bootstrap installs this same instance on its Runtime.
/// Direct Continuo hosts instead pass it to `Runtime::with_dispatcher`.
pub fn register_dispatcher(
    state: &AppState,
    dispatcher: Arc<OrbitDispatcher>,
) -> Result<()> {
    if state.registry_ref().resolve::<OrbitDispatcher>().is_some() {
        return Err(Error::msg("Orbit dispatcher is already registered"));
    }
    if !state.is_standalone() {
        crate::register_lock(state)?;
    } else {
        crate::register_fleet(state)?;
    }
    state.registry_ref().register(dispatcher)?;
    Ok(())
}

#[async_trait]
impl Dispatcher<AppState> for OrbitDispatcher {
    fn validate(
        &self,
        request: &DispatchRequest,
        state: &AppState,
    ) -> Result<()> {
        if !request.scope.includes(state.run_mode()) {
            return Err(Error::msg(
                "dispatch requested outside the process scope",
            ));
        }
        if request.dispatch.is_controller_driven() {
            let policy = self.policy(request)?;
            policy.validate()?;
            // Orbit stores lease deadlines as u64 milliseconds. This is an
            // adapter constraint, not part of Continuo's neutral policy.
            if policy.lease_ttl.as_millis() == 0
                || policy.lease_ttl.as_millis() > u128::from(u64::MAX)
            {
                return Err(Error::msg(
                    "dispatch lease_ttl must fit nonzero Orbit milliseconds",
                ));
            }
            if !state.is_standalone() && state.fleet().is_none() {
                return Err(Error::msg(
                    "Orbit fleet must be booted before service dispatch",
                ));
            }
        }
        Ok(())
    }

    async fn run(
        &self,
        request: DispatchRequest,
        execution: ServiceExecution<AppState>,
        context: RunContext,
    ) -> Result<()> {
        Dispatcher::validate(self, &request, execution.state())?;
        if request.dispatch == Dispatch::Each {
            return execution.run(context).await;
        }
        let policy = self.policy(&request)?;
        let coordinator = Coordinator::new(execution.state(), &request)?;
        #[cfg(any(
            target_os = "linux",
            target_os = "freebsd",
            target_os = "macos"
        ))]
        let readiness = match &coordinator {
            Coordinator::Local => None,
            Coordinator::Fleet { locks, .. } => {
                let event_fd = locks.event_fd()?;
                // SAFETY: RingEventFd owns its stable descriptor until drop;
                // this controller only drains it and never replaces it.
                Some(
                    unsafe { tokio::io::unix::AsyncFd::register(event_fd) }
                        .map_err(std::io::Error::from)?,
                )
            }
        };
        // Native readiness is armed before acquisition. Unlike timer fallback,
        // unavailable readiness is an explicit adapter error.
        #[cfg(not(any(
            target_os = "linux",
            target_os = "freebsd",
            target_os = "macos"
        )))]
        if matches!(&coordinator, Coordinator::Fleet { .. }) {
            return Err(Error::msg(
                "Orbit service dispatch requires native lock readiness",
            ));
        }
        #[cfg(not(any(
            target_os = "linux",
            target_os = "freebsd",
            target_os = "macos"
        )))]
        let readiness = ();
        let mut active = HashMap::<Id, ActiveSlot>::new();
        let mut executions = JoinSet::<Result<()>>::new();
        let deadline = Instant::now()
            .checked_add(policy.renewal_interval)
            .ok_or_else(|| Error::msg("dispatch renewal deadline overflow"))?;
        let mut renewal = interval_at(deadline, policy.renewal_interval);
        renewal.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut outcome = Ok(());
        if !context.is_cancelled() {
            outcome = acquire_missing(
                &coordinator,
                &request,
                policy,
                &execution,
                &context,
                &mut active,
                &mut executions,
            );
        }
        while outcome.is_ok() && !context.is_cancelled() {
            tokio::select! {
                biased;
                _ = context.cancelled() => break,
                joined = executions.join_next_with_id(), if !executions.is_empty() => {
                    if let Some(joined) = joined {
                        match finish_execution(joined, &mut active) {
                            Ok(true) => break,
                            Ok(false) => {},
                            Err(error) => outcome = Err(error),
                        }
                    }
                }
                _ = renewal.tick(), if matches!(&coordinator, Coordinator::Fleet { .. }) => {
                    renew_owned(&request, policy, &mut active);
                    outcome = acquire_missing(&coordinator, &request, policy, &execution, &context, &mut active, &mut executions);
                }
                notification = lock_changed(&coordinator, &request, &readiness) => {
                    outcome = match notification {
                        Ok(true) => acquire_missing(&coordinator, &request, policy, &execution, &context, &mut active, &mut executions),
                        Ok(false) => Ok(()),
                        Err(error) => Err(error),
                    };
                }
            }
        }
        // Cancel every child, but retain its exact lease until its future has
        // returned. The runnable, not this adapter, declares graceful bounds.
        for slot in active.values_mut() {
            slot.retiring = true;
            slot.token.cancel();
        }
        while !executions.is_empty() {
            tokio::select! {
                joined = executions.join_next_with_id() => {
                    if let Some(joined) = joined
                        && let Err(error) = finish_execution(joined, &mut active) {
                            warn!(service = request.name, %error, "dispatched execution failed during drain");
                            if outcome.is_ok() { outcome = Err(error); }
                    }
                }
                _ = renewal.tick(), if matches!(&coordinator, Coordinator::Fleet { .. }) => renew_owned(&request, policy, &mut active),
            }
        }
        debug_assert!(active.is_empty());
        outcome
    }
}

enum Coordinator {
    Local,
    Fleet { locks: OrbitLock, owner: LockOwner },
}

impl Coordinator {
    fn new(
        state: &AppState,
        request: &DispatchRequest,
    ) -> Result<Self> {
        if state.is_standalone() {
            return Ok(Self::Local);
        }
        let fleet = state
            .fleet()
            .ok_or_else(|| Error::msg("Orbit fleet unavailable"))?;
        Ok(Self::Fleet {
            locks: OrbitLock::new(fleet)?,
            owner: LockOwner::from(format!(
                "{}:pid{}:{}:{}",
                state.run_mode().as_process_label(),
                std::process::id(),
                request.name,
                request.generation
            )),
        })
    }

    fn acquire(
        &self,
        name: &str,
        slot: u16,
        policy: DispatchPolicy,
    ) -> Result<Option<Lock>> {
        match self {
            Self::Local => Ok(None),
            Self::Fleet { locks, owner } => {
                // Stable across process replacement; local generation is an
                // owner identity, never part of the shared slot key.
                let key = LockKey::new::<ServiceSlot>(format!(
                    "service:{name}:slot:{slot}"
                ));
                match locks.try_acquire(&key, owner, policy.lease_ttl)? {
                    LockAcquire::Acquired(lease) => {
                        Ok(Some(Lock::new(locks.clone(), lease)))
                    }
                    LockAcquire::Occupied(_) => Ok(None),
                }
            }
        }
    }
}

struct ActiveSlot {
    number: u16,
    ownership: Option<Lock>,
    token: CancellationToken,
    retiring: bool,
}

fn acquire_missing(
    coordinator: &Coordinator,
    request: &DispatchRequest,
    policy: DispatchPolicy,
    execution: &ServiceExecution<AppState>,
    context: &RunContext,
    active: &mut HashMap<Id, ActiveSlot>,
    executions: &mut JoinSet<Result<()>>,
) -> Result<()> {
    if context.is_cancelled() {
        return Ok(());
    }
    let desired = match request.dispatch {
        Dispatch::One => 1,
        _ => policy.capacity.get(),
    };
    let members = match current_members(execution.state()) {
        Ok(members) => members,
        Err(error) => {
            debug!(service = request.name, %error, "dispatch membership unavailable; acquisition deferred");
            return Ok(());
        }
    };
    let eligible = members.iter().any(|member| {
        let role = match member.role {
            FleetMemberRole::Master => super::Scope::Master,
            FleetMemberRole::Worker => super::Scope::Worker,
            FleetMemberRole::Standalone => super::Scope::Any,
        };
        member.node_id == execution.state().fleet_node_id().raw()
            && request.scope.includes(role)
    });
    if !eligible {
        return Ok(());
    }
    // Membership proves process ownership, not registration of this service.
    // Dividing capacity by all eligible members could leave slots permanently
    // unused when only some members register the provider. Stable slot keys
    // enforce the fleet-wide bound without an invented fair-share cap.
    for number in 1..=desired {
        if active.len() >= usize::from(desired) || context.is_cancelled() {
            break;
        }
        if active.values().any(|slot| slot.number == number) {
            continue;
        }
        let ownership = coordinator.acquire(request.name, number, policy)?;
        if ownership.is_none() && !matches!(coordinator, Coordinator::Local) {
            continue;
        }
        let token = context.cancellation_token().child_token();
        let child_context = RunContext::new(token.clone());
        let child = execution.clone();
        let id = executions
            .spawn(async move { child.run(child_context).await })
            .id();
        active.insert(
            id,
            ActiveSlot { number, ownership, token, retiring: false },
        );
        debug!(
            service = request.name,
            slot = number,
            "dispatched service slot acquired"
        );
    }
    Ok(())
}

fn renew_owned(
    request: &DispatchRequest,
    policy: DispatchPolicy,
    active: &mut HashMap<Id, ActiveSlot>,
) {
    for slot in active.values_mut() {
        let Some(lock) = &mut slot.ownership else {
            continue;
        };
        match lock.renew(policy.lease_ttl) {
            Ok(true) => {}
            result => {
                if !slot.retiring {
                    warn!(service = request.name, slot = slot.number, result = ?result, "dispatch ownership lost; draining local execution");
                }
                slot.retiring = true;
                slot.token.cancel();
                if matches!(result, Ok(false)) {
                    // This exact tenure is already gone. Do not renew a
                    // successor or repeatedly report the same lost lease.
                    slot.ownership = None;
                }
            }
        }
    }
}

fn finish_execution(
    joined: std::result::Result<(Id, Result<()>), tokio::task::JoinError>,
    active: &mut HashMap<Id, ActiveSlot>,
) -> Result<bool> {
    let (id, result) = match joined {
        Ok(joined) => joined,
        Err(error) => (
            error.id(),
            Err(Error::msg(format!(
                "dispatched execution join failed: {error}"
            ))),
        ),
    };
    let slot = active.remove(&id).ok_or_else(|| {
        Error::msg("dispatched execution has no ownership record")
    })?;
    let retiring = slot.retiring;
    drop(slot); // Release only after the child completed, including panics.
    result?;
    // A normal runnable completion ends its managed generation, just as Each
    // does. Only a slot cancelled after ownership loss may be reacquired.
    Ok(!retiring)
}

#[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]
async fn lock_changed(
    coordinator: &Coordinator,
    request: &DispatchRequest,
    readiness: &Option<tokio::io::unix::AsyncFd<orbitive::core::RingEventFd>>,
) -> Result<bool> {
    let Some(readiness) = readiness else {
        return std::future::pending().await;
    };
    let mut ready = readiness.readable().await?;
    readiness.get_ref().drain()?;
    ready.clear_ready();
    let Coordinator::Fleet { locks, .. } = coordinator else {
        return Ok(false);
    };
    let poll = locks.poll();
    let prefix = format!("service:{}:slot:", request.name);
    Ok(!poll.loss.is_empty()
        || poll.malformed != 0
        || poll.events.iter().any(|event| {
            !matches!(event.transition, LockTransition::Renewed(_))
                && event.transition.key().namespace()
                    == ServiceSlot::NAMESPACE.as_bytes()
                && event.transition.key().label().starts_with(prefix.as_bytes())
        }))
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "freebsd",
    target_os = "macos"
)))]
async fn lock_changed(
    _: &Coordinator,
    _: &DispatchRequest,
    _: &(),
) -> Result<bool> {
    std::future::pending().await
}

#[cfg(test)]
#[path = "../../tests/placement/controller.rs"]
mod tests;
