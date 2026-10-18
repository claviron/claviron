//! Convert authoritative assignments into one dense concrete member view.
use super::*;
use crate::origin::{ConnectTarget, OriginId};
use crate::resolve::{ProfileResolver, PublicationError, ResolutionEntry};

pub struct MaterializedProfile {
    pub profile: Arc<Profile>,
    pub publications: Vec<(ResolutionEntry, u64)>,
}
impl DesiredProfile {
    /// Build an inert candidate for publish_resolved_membership. The target must
    /// be frozen before this call; later revision changes reject adoption.
    pub fn materialize(
        &self,
        service: &crate::proxy::service::UpstreamService,
        owner: Option<&ProfileResolver>,
    ) -> Result<MaterializedProfile, DesiredError> {
        self.materialize_with(&service.inner.runtime, owner)
    }
    pub(crate) fn materialize_with(
        &self,
        runtime: &crate::proxy::service::runtime::ServiceRuntime,
        owner: Option<&ProfileResolver>,
    ) -> Result<MaterializedProfile, DesiredError> {
        let invalid = PublicationError::Invalid;
        let publications = match (&self.plan, owner) {
            (Some(plan), Some(owner)) if owner.matches_plan(plan) => {
                owner.publications()?
            }
            (None, None) => vec![],
            _ => {
                return Err(invalid(
                    "materialization requires its exact prepared resolver plan",
                )
                .into());
            }
        };
        let mut approvals = Vec::new();
        let mut origins = Vec::new();
        let mut next_name = 0usize;
        for member in &self.members {
            match member {
                Member::Literal(index) => {
                    origins.push(self.policy.origins[*index].clone());
                }
                Member::Named(named) => {
                    let (entry, publication) = &publications[next_name];
                    next_name += 1;
                    let publication =
                        publication.as_ref().ok_or_else(|| {
                            invalid(
                                "named materialization has no published answer",
                            )
                        })?;
                    approvals.push((entry.clone(), publication.revision));
                    let set = publication.snapshot.restore()?;
                    let mut assignments = set.active().collect::<Vec<_>>();
                    assignments
                        .sort_by_key(|assignment| assignment.address.socket);
                    for assignment in assignments {
                        let id =
                            u16::try_from(origins.len()).map_err(|_| {
                                invalid(
                                    "materialized members exceed dense OriginId"
                                )
                            })?;
                        let identity = serde_json::to_vec(&(
                            &self.policy.name,
                            &named.name.label,
                            &named.source,
                            assignment.address.socket,
                        ))
                        .map_err(PublicationError::from)?;
                        let mut hash = blake3::Hasher::new_derive_key(
                            "claviron.upstream.named.address.v1",
                        );
                        hash.update(&identity);
                        let hash = claviron_helpers::base64::base64_encode(
                            hash.finalize().as_bytes(),
                        );
                        let key = crate::state::origin_key(
                            &self.policy.name,
                            &hash,
                            "dns",
                        )
                        .map_err(|_| {
                            invalid(
                                "named materialized state key exceeds key ABI",
                            )
                        })?;
                        let connect =
                            ConnectTarget::Tcp(assignment.address.socket);
                        let connect = if named.tls {
                            ConnectTarget::Tls(assignment.address.socket)
                        } else {
                            connect
                        };
                        let mut origin =
                            crate::compile::origin::prepare_origin(
                                named.source.clone(),
                                format!(
                                    "{}@{}",
                                    named.name.label, assignment.address.socket
                                )
                                .into(),
                                named.authority.clone(),
                                key,
                                connect,
                                assignment.address.weight,
                                OriginId(id),
                            )
                            .map_err(|error| {
                                ConfigError::InvalidOrigin {
                                    profile: self.policy.name.clone(),
                                    origin: named.source.to_string(),
                                    detail: error.to_string(),
                                }
                            })?;
                        {
                            origin.tls_name = named.tls_name.clone();
                        }
                        origin = super::super::table::membership::bind_origin(
                            &self.policy.name,
                            &runtime.state_table,
                            runtime.lock.is_some(),
                            entry,
                            publication.revision,
                            &origin,
                            assignment,
                        )?;
                        origins.push(origin);
                    }
                }
            }
        }
        for (index, origin) in origins.iter_mut().enumerate() {
            let id = u16::try_from(index).map_err(|_| {
                invalid("materialized members exceed dense OriginId")
            })?;
            origin.id = OriginId(id);
            origin.metrics = crate::metrics::OriginMetrics::new(id);
        }
        let mut profile = self.policy.as_ref().clone();
        profile.origins = origins.into();
        if let Some(limit) = profile.breaker.max_connections
            && profile.capacity.min_idle().saturating_mul(profile.origins.len())
                > limit
        {
            return Err(ConfigError::InvalidCapacity {
                field: "breaker.max_connections",
                detail: "must accommodate materialized H1 warm floors across origins"
            }
            .into());
        }
        profile.l7_probe = crate::compile::l7::Prepared::compile(
            &profile.name,
            &profile.health.l7,
            profile.protocol,
            &profile.origins,
        )?;
        profile.balancer = crate::proxy::balance::Prepared::compile(
            &profile.name,
            &profile.balance,
            &profile.origins,
        )?;
        Ok(MaterializedProfile {
            profile: Arc::new(profile),
            publications: approvals,
        })
    }
}
