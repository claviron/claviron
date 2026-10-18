//! One cold profile owner. The caller polls this future in its Runnable;
//! preparation starts no work and this module spawns no tasks.
use std::sync::Arc;
use std::time::{Duration, Instant};

use claviron_orbit::lock::{Lock, LockAcquire, LockOwner, OrbitLock};
use claviron_resolver::Resolver;
use claviron_runtime::RunContext;
use claviron_runtime::events::ProcessEventBus;
use tokio_util::sync::CancellationToken;

mod boot;
#[cfg(feature = "metrics")]
mod metrics;
mod run;
mod tenure;

use super::{
    Name, PublicationError, PublishedResolution, ReservedOriginId,
    ResolutionEntry, ResolutionTable, ResolutionWriter, ResolveConfig,
    ResolvedSet,
};

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct ResolutionName {
    pub label: String,
    pub source: Name,
    pub first: ReservedOriginId,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolverPlan {
    pub profile: String,
    pub names: Vec<ResolutionName>,
    pub config: ResolveConfig,
    /// The owning profile's connection.connect_timeout, not a DNS transport timeout.
    pub budget: Duration,
}
/// Fleet locks and table must belong to the same Fleet. Losing that capability
/// is never permission to elect a private local owner.
pub enum ResolutionAuthority {
    Local { table: Arc<ResolutionTable> },
    Fleet { table: Arc<ResolutionTable>, locks: OrbitLock, owner: LockOwner },
}
#[derive(Debug, thiserror::Error)]
pub enum ResolverError {
    #[error(transparent)]
    Publication(#[from] PublicationError),
    #[error(transparent)]
    Resolve(#[from] claviron_resolver::ResolveError),
    #[error(
        "required upstream resolution failed: profile={profile} name={name}: {error}"
    )]
    Required {
        profile: String,
        name: String,
        #[source]
        error: claviron_resolver::ResolveError,
    },
    #[error(
        "upstream resolution boot exceeded its total budget: profile={profile} names=[{names}]"
    )]
    BootTimeout { profile: String, names: String },
    #[error("upstream resolution boot was cancelled")]
    Cancelled,
}
struct Binding {
    name: ResolutionName,
    entry: ResolutionEntry,
    #[cfg(feature = "metrics")]
    counters: Option<Arc<super::metrics::Counters>>,
}
pub struct ProfileResolver {
    profile: String,
    bindings: Vec<Binding>,
    config: ResolveConfig,
    budget: Duration,
    ttl: Duration,
    resolver: Arc<dyn Resolver>,
    authority: ResolutionAuthority,
}
struct Tenure {
    lease: Option<Lock>,
    writers: Vec<ResolutionWriter>,
}
impl ProfileResolver {
    pub(crate) fn matches_plan(
        &self,
        plan: &ResolverPlan,
    ) -> bool {
        self.profile == plan.profile
            && self.config == plan.config
            && self.budget == plan.budget
            && self
                .bindings
                .iter()
                .map(|binding| &binding.name)
                .eq(plan.names.iter())
    }
    /// Cold complete snapshot paired with its exact physical binding. Missing
    /// answers remain explicit; reading starts no query or election.
    pub fn publications(
        &self
    ) -> Result<
        Vec<(ResolutionEntry, Option<PublishedResolution>)>,
        ResolverError,
    > {
        self.bindings
            .iter()
            .map(|binding| {
                let current = binding.entry.read()?;
                if let Some(current) = &current {
                    self.restore(binding, current)?;
                }
                Ok((binding.entry.clone(), current))
            })
            .collect()
    }
    pub fn prepare(
        plan: ResolverPlan,
        resolver: Arc<dyn Resolver>,
        authority: ResolutionAuthority,
    ) -> Result<Self, ResolverError> {
        plan.config.validate()?;
        let invalid = |detail| PublicationError::Invalid(detail);
        // Orbit deadlines have millisecond resolution. Twice the total query
        // budget leaves a full budget for renewal, independent of record TTL.
        let ttl = plan
            .budget
            .checked_mul(2)
            .filter(|ttl| Instant::now().checked_add(*ttl).is_some())
            .ok_or_else(|| {
                invalid("resolver tenure exceeds the monotonic clock")
            })?;
        if plan.budget < Duration::from_millis(1) {
            return Err(
                invalid("resolver total budget must be at least 1ms").into()
            );
        }
        if plan.profile.is_empty() || plan.names.is_empty() {
            return Err(invalid(
                "resolver requires a profile and named bindings",
            )
            .into());
        }
        let (table, fleet) = match &authority {
            ResolutionAuthority::Local { table } => (table, false),
            ResolutionAuthority::Fleet { table, .. } => (table, true),
        };
        if table.fleet_authority() != fleet {
            return Err(
                invalid("resolver authority does not match its table").into()
            );
        }
        if plan.config.max_addresses > table.address_capacity() {
            return Err(invalid(
                "resolver reservation exceeds named health build geometry",
            )
            .into());
        }
        let mut labels = std::collections::BTreeSet::new();
        let mut ranges = Vec::new();
        for name in &plan.names {
            let _ = ResolvedSet::new(name.first, plan.config.max_addresses)?;
            if name.label.is_empty()
                || name.label.contains('\0')
                || name.source.name().is_empty()
                || !labels.insert(&name.label)
                || matches!(
                    &name.source,
                    Name::Host { port: 0, .. } | Name::Host { weight: 0, .. }
                )
            {
                return Err(invalid(
                    "invalid or duplicate named resolver binding",
                )
                .into());
            }
            ranges.push((
                name.first.index(),
                name.first.index() + plan.config.max_addresses,
            ));
        }
        ranges.sort_unstable();
        if ranges.windows(2).any(|pair| pair[0].1 > pair[1].0) {
            return Err(invalid(
                "named resolver ReservedOriginId ranges overlap",
            )
            .into());
        }
        let bindings = plan
            .names
            .into_iter()
            .map(|name| {
                // Full BLAKE3 digest, URL-safe encoding: no truncation to fit the
                // escaped-key ABI. Bind source/policy/budget/slot identity together;
                // a changed desired configuration cannot adopt old answer bytes.
                let bytes =
                    serde_json::to_vec(&(&name, &plan.config, plan.budget))
                        .map_err(PublicationError::from)?;
                let mut hash = blake3::Hasher::new_derive_key(
                    "claviron.upstream.resolve.binding.v1",
                );
                hash.update(&bytes);
                let identity = claviron_helpers::base64::base64_encode(
                    hash.finalize().as_bytes(),
                );
                let entry = table.install(&plan.profile, &identity)?;
                Ok(Binding {
                    name,
                    entry,
                    #[cfg(feature = "metrics")]
                    counters: None,
                })
            })
            .collect::<Result<Vec<_>, PublicationError>>()?;
        Ok(Self {
            profile: plan.profile,
            bindings,
            config: plan.config,
            budget: plan.budget,
            ttl,
            resolver,
            authority,
        })
    }

    /// Read current truth without election, DNS queries, or event replay. This
    /// is the late-peer path; serving boot policy remains the provider's owner.
    pub fn current(
        &self
    ) -> Result<Vec<Option<PublishedResolution>>, ResolverError> {
        self.bindings
            .iter()
            .map(|binding| {
                let current = binding.entry.read()?;
                if let Some(current) = &current {
                    self.restore(binding, current)?;
                }
                Ok(current)
            })
            .collect()
    }

    fn restore(
        &self,
        binding: &Binding,
        current: &PublishedResolution,
    ) -> Result<ResolvedSet, ResolverError> {
        let set = current.snapshot.restore()?;
        if set.first != binding.name.first
            || set.slots.len() != self.config.max_addresses
        {
            return Err(PublicationError::Invalid(
                "published reserved range differs from its binding",
            )
            .into());
        }
        Ok(set)
    }
}
