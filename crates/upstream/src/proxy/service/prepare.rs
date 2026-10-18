//! Unique inert native construction before authoritative boot publication.
use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::UpstreamService;
use super::control::{OwnedOrigin, ProfileLifetime};
use super::lifecycle::{Lifecycle, ServiceEpoch};
use super::runtime::ServiceRuntime;
use crate::compile::profile::Profile;
use crate::origin::PoolGeneration;
use crate::proxy::service::MembershipError;

pub(crate) struct PreparedService {
    profile: Arc<Profile>,
    runtime: ServiceRuntime,
    generation: PoolGeneration,
    lifetime: Arc<ProfileLifetime>,
    owners: Option<Vec<OwnedOrigin>>,
}
impl PreparedService {
    pub(crate) fn new(
        mut profile: Arc<Profile>,
        runtime: ServiceRuntime,
    ) -> Result<Self, MembershipError> {
        for origin in Arc::make_mut(&mut Arc::make_mut(&mut profile).origins) {
            if !origin.slot.belongs_to(&runtime.state_table) {
                origin.slot.0 = Some(
                    runtime.state_table.install(&origin.state_key, 0).map_err(
                        |error| MembershipError::StateBinding(Box::new(error)),
                    )?,
                );
            }
        }
        let generation = PoolGeneration(
            runtime.generations.fetch_add(1, Ordering::Relaxed) + 1,
        );
        let lifetime = ProfileLifetime::new(&profile);
        let owners = profile
            .origins
            .iter()
            .map(|origin| {
                OwnedOrigin::new(&profile, generation, origin, &lifetime)
            })
            .collect::<Vec<_>>();
        let candidate = Self {
            profile,
            runtime,
            generation,
            lifetime,
            owners: Some(owners),
        };
        if candidate
            .owners
            .as_ref()
            .unwrap()
            .iter()
            .any(|owner| owner.transport.force.is_cancelled())
        {
            return Err(MembershipError::StateBinding(Box::new(
                crate::resolve::MembershipStateError::Busy,
            )));
        }
        Ok(candidate)
    }
    pub(crate) fn commit(mut self) -> UpstreamService {
        let owners = self.owners.take().expect("unique candidate commits once");
        let epoch = ServiceEpoch::from_owners(
            &self.profile,
            self.generation,
            self.lifetime.clone(),
            owners,
        );
        UpstreamService::from_lifecycle(
            self.profile.clone(),
            self.runtime.clone(),
            Lifecycle::from_epoch(epoch),
        )
    }
}
impl Drop for PreparedService {
    fn drop(&mut self) {
        // Unique owners have no lifetime registration, projections or IO. The
        // explicit consumed abort proves empty joins; Drop alone is never ACK.
        if let Some(owners) = self.owners.take() {
            for owner in owners {
                owner.abort_prepared();
            }
        }
    }
}
