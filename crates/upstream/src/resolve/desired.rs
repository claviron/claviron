//! Immutable desired declarations. Preparation never fabricates sockets, queries
//! DNS or enables serving named origins through the ordinary compiler.
use std::collections::BTreeMap;
use std::sync::Arc;

use super::{ResolutionName, ResolverPlan};
use crate::compile::Validation;
use crate::compile::profile::Profile;
use crate::config::UpstreamYaml;
use crate::config::error::ConfigError;
pub(super) mod materialize;
mod name;
use name::Named;

#[derive(Debug, thiserror::Error)]
pub enum DesiredError {
    #[error(transparent)]
    Yaml(#[from] serde_yaml::Error),
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Publication(#[from] super::PublicationError),
    #[error(transparent)]
    Resolver(#[from] super::ResolverError),
}
#[derive(Clone, Debug, PartialEq, Eq)]
enum Member {
    Literal(usize),
    Named(Named),
}
/// Prepared policy with separate literal and named declarations. Its private
/// policy is not a serving profile; only materialization returns one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DesiredProfile {
    policy: Arc<Profile>,
    members: Vec<Member>,
    plan: Option<ResolverPlan>,
}
impl DesiredProfile {
    pub fn resolver_plan(&self) -> Option<ResolverPlan> {
        self.plan.clone()
    }
}
#[derive(Clone, Debug)]
pub struct DesiredUpstreams {
    pub(crate) profiles: BTreeMap<String, Arc<DesiredProfile>>,
    pub(crate) apps: BTreeMap<String, crate::app::CompiledUpstreamApp>,
}
impl DesiredUpstreams {
    /// Input is already expanded by the owning provider. TLS material is prepared
    /// cold; no driver/process execution occurs. Worker count is a capacity hint.
    pub fn prepare(
        yaml: &str,
        healthy_workers: Option<usize>,
    ) -> Result<Self, DesiredError> {
        Self::prepare_with(
            yaml,
            healthy_workers,
            &crate::compile::tls::TlsMaterial::default(),
        )
    }
    /// Certificate material comes from the process certificate manager.
    /// Preparation without it still declares plain and `verify: native` legs;
    /// a profile that names an identity fails instead.
    pub fn prepare_with(
        yaml: &str,
        healthy_workers: Option<usize>,
        material: &crate::compile::tls::TlsMaterial,
    ) -> Result<Self, DesiredError> {
        let parsed: UpstreamYaml = serde_yaml::from_str(yaml)?;
        Self::from_config(
            &parsed,
            &Validation { healthy_worker_count: healthy_workers },
            material,
        )
    }
    pub fn profile(
        &self,
        name: &str,
    ) -> Option<Arc<DesiredProfile>> {
        self.profiles.get(name).cloned()
    }
    pub fn app_names(&self) -> impl Iterator<Item = &str> {
        self.apps.keys().map(String::as_str)
    }
    pub(crate) fn from_config(
        parsed: &UpstreamYaml,
        validation: &Validation,
        material: &crate::compile::tls::TlsMaterial,
    ) -> Result<Self, DesiredError> {
        let mut profiles = BTreeMap::new();
        let mut policies = BTreeMap::new();
        for (profile_name, config) in &parsed.upstreams {
            config.resolve.validate().map_err(|error| {
                ConfigError::InvalidResolve {
                    profile: profile_name.clone(),
                    detail: error.to_string(),
                }
            })?;
            let mut members = Vec::new();
            let mut literals = Vec::new();
            let mut names: Vec<ResolutionName> = Vec::new();
            let mut first = 0usize;
            for origin in &config.origins {
                if let Some(named) =
                    Named::prepare(profile_name, config, origin, first)?
                {
                    if names
                        .iter()
                        .any(|previous| previous.label == named.name.label)
                    {
                        return Err(ConfigError::InvalidOrigin {
                            profile: profile_name.clone(),
                            origin: origin.target().into(),
                            detail: "named origins require unique labels"
                                .into(),
                        }
                        .into());
                    }
                    first = first
                        .checked_add(config.resolve.max_addresses)
                        .filter(|end| *end <= usize::from(u16::MAX) + 1)
                        .ok_or_else(|| ConfigError::InvalidResolve {
                            profile: profile_name.clone(),
                            detail:
                                "named reservations exceed ReservedOriginId"
                                    .into(),
                        })?;
                    names.push(named.name.clone());
                    members.push(Member::Named(named));
                } else {
                    let id = u16::try_from(literals.len()).map_err(|_| {
                        ConfigError::InvalidOrigin {
                            profile: profile_name.clone(),
                            origin: origin.target().into(),
                            detail: "too many origins for OriginId".into(),
                        }
                    })?;
                    let literal = crate::compile::origin::compile_origin(
                        profile_name,
                        origin,
                        config.authority.as_deref(),
                        crate::origin::OriginId(id),
                    )?;
                    members.push(Member::Literal(literals.len()));
                    literals.push(literal);
                }
            }
            if literals.len().saturating_add(first) > usize::from(u16::MAX) + 1
            {
                return Err(ConfigError::InvalidResolve {
                    profile: profile_name.clone(),
                    detail:
                        "literal plus named reservations exceed dense OriginId"
                            .into(),
                }
                .into());
            }
            let policy = parsed.compile_profile_with_origins(
                profile_name,
                config,
                validation,
                Some(literals),
                material,
            )?;
            let mut policy = policy;
            if policy.connector.is_none()
                && members.iter().any(
                    |member| matches!(member, Member::Named(named) if named.tls)
                )
            {
                policy.connector = Some(crate::compile::tls::prepare(
                    profile_name,
                    &config.tls,
                    config.protocol,
                    material
                )?);
            }
            // The resolver's total budget is the connection's: a name that
            // cannot resolve in it cannot connect in it either. It needs a
            // number, so a resolving profile cannot turn it off.
            let budget = match (
                names.is_empty(),
                policy.connection.connect_timeout,
            ) {
                (true, _) => None,
                (false, Some(budget)) => Some(budget),
                (false, None) => {
                    return Err(ConfigError::InvalidResolve {
                        profile: profile_name.clone(),
                        detail: "connection.connect_timeout cannot be off for a profile that resolves names: it is the resolver's budget".into()
                    }
                    .into());
                }
            };
            let plan = budget.map(|budget| ResolverPlan {
                profile: profile_name.clone(),
                names,
                config: config.resolve.clone(),
                budget,
            });
            let policy = Arc::new(policy);
            policies.insert(profile_name.clone(), policy.clone());
            profiles.insert(
                profile_name.clone(),
                Arc::new(DesiredProfile { policy, members, plan }),
            );
        }
        let apps = parsed.compile_apps(&policies)?;
        Ok(Self { profiles, apps })
    }
}
