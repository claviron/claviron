//! Immutable member views retain stable profile and unchanged native ownership.
use std::sync::Arc;
use std::sync::atomic::Ordering;

use claviron_runtime::gate::GateDrainOutcome;

use super::UpstreamService;
use super::lifecycle::{Lifecycle, ServiceEpoch, UpstreamLifecycle};
use crate::compile::profile::Profile;
use crate::origin::PoolGeneration;

#[derive(Debug, thiserror::Error)]
pub enum MembershipError {
    #[error("membership candidate belongs to a superseded serving view")]
    Superseded,
    #[error("membership update changes profile policy")]
    PolicyChanged,
    #[error("membership update requires the current running service")]
    NotRunning,
    #[error("membership state could not be bound: {0}")]
    StateBinding(#[source] Box<dyn std::error::Error + Send + Sync>),
}
/// A cold candidate's immutable publication target. The service handle itself
/// can survive operator restart; capture its epoch generation before preparing
/// the candidate, rather than reading that handle's current generation later.
#[derive(Clone, Debug)]
pub struct MembershipTarget {
    pub(crate) service: UpstreamService,
    pub(crate) generation: PoolGeneration,
}
impl MembershipTarget {
    pub fn generation(&self) -> PoolGeneration {
        self.generation
    }
    pub fn profile(&self) -> Arc<Profile> {
        self.service.inner.profile.clone()
    }
}
#[derive(Clone)]
pub(super) struct Retirement {
    pub(super) removed: Vec<super::control::OwnedOrigin>,
}
impl UpstreamService {
    /// Capture the exact view/epoch before doing cold candidate preparation.
    pub fn membership_target(&self) -> MembershipTarget {
        let _admission = self
            .inner
            .lifecycle
            .admission
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        MembershipTarget {
            service: self.clone(),
            generation: self.generation(),
        }
    }
    /// Replace only the compiled member view, preserving profile admission,
    /// breaker/retry budgets and unchanged origin transports/pools. Construction
    /// is inert. The old handle is permanently sealed; stop() on it joins only
    /// removed origins and its maintenance. stop() on the returned current
    /// handle drains the stable profile and all active/draining origin owners.
    ///
    /// DNS coordination must separately fence fleet slot reuse: this local
    /// operation is not a retirement acknowledgement from another worker.
    pub fn replace_membership(
        &self,
        profile: Arc<Profile>,
    ) -> Result<Self, MembershipError> {
        self.replace_membership_at(profile, None)
    }
    /// Adopt all named members against exact authoritative revisions. Stale
    /// answers cannot seal the live view or leave prepared native registrations.
    pub fn replace_resolved_membership(
        &self,
        profile: Arc<Profile>,
        publications: &[(crate::resolve::ResolutionEntry, u64)],
    ) -> Result<Self, MembershipError> {
        self.replace_membership_checked(
            profile,
            None,
            Some(publications),
            |_| {},
        )
    }
    pub(crate) fn replace_resolved_membership_at(
        &self,
        profile: Arc<Profile>,
        expected: PoolGeneration,
        publications: &[(crate::resolve::ResolutionEntry, u64)],
        on_commit: impl FnOnce(&Self),
    ) -> Result<Self, MembershipError> {
        self.replace_membership_checked(
            profile,
            Some(expected),
            Some(publications),
            on_commit,
        )
    }
    pub(crate) fn replace_membership_at(
        &self,
        profile: Arc<Profile>,
        expected: Option<PoolGeneration>,
    ) -> Result<Self, MembershipError> {
        self.replace_membership_checked(profile, expected, None, |_| {})
    }
    fn replace_membership_checked(
        &self,
        mut profile: Arc<Profile>,
        expected: Option<PoolGeneration>,
        publications: Option<&[(crate::resolve::ResolutionEntry, u64)]>,
        on_commit: impl FnOnce(&Self),
    ) -> Result<Self, MembershipError> {
        let _admission = self
            .inner
            .lifecycle
            .admission
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let old = self.epoch();
        if expected.is_some_and(|generation| generation != old.generation) {
            return Err(MembershipError::Superseded);
        }
        if self.lifecycle() != UpstreamLifecycle::Running
            || old.gate.is_shutting_down()
            || self.inner.lifecycle.retired.load(Ordering::Acquire)
        {
            return Err(MembershipError::NotRunning);
        }
        let mut policy = self.inner.profile.as_ref().clone();
        policy.origins = profile.origins.clone();
        policy.balancer = profile.balancer.clone();
        policy.l7_probe = profile.l7_probe.clone();
        if policy != *profile {
            return Err(MembershipError::PolicyChanged);
        }
        if self.inner.profile.as_ref() == profile.as_ref()
            && self
                .inner
                .profile
                .origins
                .iter()
                .zip(profile.origins.iter())
                .all(|(old, next)| old.slot.same_assignment(&next.slot))
        {
            return with_publication_fence(
                &self.inner.runtime,
                &self.inner.profile,
                &profile,
                publications,
                || {
                    on_commit(self);
                    self.clone()
                },
            );
        }
        // Validate/bind the whole candidate before sealing the serving view.
        for origin in
            Arc::make_mut(&mut Arc::make_mut(&mut profile).origins).iter_mut()
        {
            if !origin.slot.belongs_to(&self.inner.runtime.state_table) {
                origin.slot.0 = Some(
                    self.inner
                        .runtime
                        .state_table
                        .install(&origin.state_key, 0)
                        .map_err(|error| {
                            MembershipError::StateBinding(Box::new(error))
                        })?,
                );
            }
        }
        let generation = PoolGeneration(
            self.inner.runtime.generations.fetch_add(1, Ordering::Relaxed) + 1,
        );
        let mapping = profile
            .origins
            .iter()
            .map(|origin| {
                self.inner
                    .profile
                    .origins
                    .iter()
                    .position(|previous| same_transport(previous, origin))
            })
            .collect::<Vec<_>>();
        // Reusing native ownership also keeps its local health projection.
        // A newly prepared equal physical binding has not registered an owner.
        for (origin, previous) in
            Arc::make_mut(&mut Arc::make_mut(&mut profile).origins)
                .iter_mut()
                .zip(&mapping)
        {
            if let Some(index) = previous {
                origin.slot = self.inner.profile.origins[*index].slot.clone();
            }
        }
        let owners = profile
            .origins
            .iter()
            .map(|origin| {
                self.inner
                    .profile
                    .origins
                    .iter()
                    .position(|previous| same_transport(previous, origin))
                    .map(|index| old.owners[index].clone())
                    .unwrap_or_else(|| {
                        super::control::OwnedOrigin::new(
                            &profile,
                            generation,
                            origin,
                            &old.lifetime,
                        )
                    })
            })
            .collect::<Vec<_>>();
        if owners.iter().any(|owner| owner.transport.force.is_cancelled()) {
            // Construction is inert. Refuse lost assignment registration before
            // sealing the current view or registering any candidate in its root.
            // Only newly constructed owners may be consumed by the empty-IO
            // abort proof; unchanged owners belong to the serving lifetime.
            for (owner, previous) in owners.into_iter().zip(&mapping) {
                if previous.is_none() {
                    owner.abort_prepared();
                }
            }
            return Err(MembershipError::StateBinding(Box::new(
                crate::resolve::MembershipStateError::Busy,
            )));
        }
        let removed = old
            .owners
            .iter()
            .filter(|owner| {
                !owners
                    .iter()
                    .any(|next| Arc::ptr_eq(&owner.transport, &next.transport))
            })
            .cloned()
            .collect::<Vec<_>>();
        let validation_profile = profile.clone();
        let mut prepared = Some(owners);
        let result = with_publication_fence(
            &self.inner.runtime,
            &self.inner.profile,
            &validation_profile,
            publications,
            || {
                let owners = prepared.take().expect("candidate commits once");
                let mut epoch = ServiceEpoch::from_owners(
                    &profile,
                    generation,
                    old.lifetime.clone(),
                    owners,
                );
                self.inner.lifecycle.retired.store(true, Ordering::Release);
                self.inner.lifecycle.status.store(
                    UpstreamLifecycle::Stopping as u8,
                    Ordering::Release,
                );
                old.background.cancel();
                old.tasks.close();
                if let Some(outlier) = &old.outlier {
                    Arc::get_mut(&mut epoch)
                        .expect("membership epoch not yet published")
                        .outlier = Some(outlier.remap(&mapping));
                }
                for owner in &removed {
                    owner.seal(self.inner.profile.lifecycle.drain_timeout);
                }
                let next = Self::from_lifecycle(
                    profile,
                    self.inner.runtime.clone(),
                    Lifecycle::from_epoch(epoch),
                );
                *self
                    .inner
                    .lifecycle
                    .membership_retired
                    .lock()
                    .unwrap_or_else(|p| p.into_inner()) =
                    Some(Retirement { removed });
                // Registry/app publication shares the table's revision exclusion. The
                // callback must not register/read/lock this table or re-enter admission.
                on_commit(&next);
                self.inner.runtime.changed.notify_waiters();
                next
            },
        );
        if let Some(owners) = prepared {
            for (owner, previous) in owners.into_iter().zip(&mapping) {
                if previous.is_none() {
                    owner.abort_prepared();
                }
            }
        }
        result
    }
}
fn same_transport(
    a: &crate::origin::Origin,
    b: &crate::origin::Origin,
) -> bool {
    if a.tls_name != b.tls_name {
        return false;
    }
    a.slot.same_assignment(&b.slot)
        && a.source == b.source
        && a.state_key == b.state_key
        && a.authority == b.authority
        && a.host == b.host
        && a.connect == b.connect
        && a.lock == b.lock
}
pub(super) fn combine(outcomes: Vec<GateDrainOutcome>) -> GateDrainOutcome {
    let mut forced = false;
    let mut remaining = 0;
    for outcome in outcomes {
        if let GateDrainOutcome::Forced { remaining: count } = outcome {
            forced = true;
            remaining += count;
        }
    }
    if forced {
        GateDrainOutcome::Forced { remaining }
    } else {
        GateDrainOutcome::Drained
    }
}

fn with_publication_fence<T>(
    runtime: &super::runtime::ServiceRuntime,
    previous: &Profile,
    profile: &Profile,
    publications: Option<&[(crate::resolve::ResolutionEntry, u64)]>,
    action: impl FnOnce() -> T,
) -> Result<T, MembershipError> {
    let Some(publications) = publications else {
        return Ok(action());
    };
    let Some((entry, _)) = publications.first() else {
        return Err(MembershipError::StateBinding(Box::new(
            crate::resolve::PublicationError::Invalid(
                "resolved membership requires publications",
            ),
        )));
    };
    if entry.fleet_authority() != runtime.lock.is_some()
        || previous.origins.iter().any(|origin| match origin.slot.0.as_ref() {
            Some(crate::state::SlotRef::Resolution(member)) => {
                !member.represented_by(publications)
            }
            _ => false,
        })
    {
        return Err(MembershipError::StateBinding(Box::new(
            crate::resolve::PublicationError::Invalid(
                "publication omits current bindings or differs from runtime authority",
            ),
        )));
    }
    entry
        .with_publications(publications, profile, action)
        .map_err(|error| MembershipError::StateBinding(Box::new(error)))?
        .ok_or(MembershipError::Superseded)
}
