//! Cold observations: authoritative truth and local adoption are distinct.
use serde::Serialize;

use super::{Address, Name, ResolutionGeneration, ResolverError};
use crate::UpstreamProvider;

#[derive(Clone, Debug, Serialize)]
pub struct ProfileResolutionStatus {
    pub profile: String,
    pub names: Vec<NameResolutionStatus>,
    /// This process's immutable serving view, sampled separately from truth.
    pub serving: Vec<ServingOriginStatus>,
}
#[derive(Clone, Debug, Serialize)]
pub struct NameResolutionStatus {
    pub label: String,
    pub source: Name,
    pub revision: Option<u64>,
    pub generation: Option<ResolutionGeneration>,
    pub active: Vec<Address>,
    pub pending: Vec<Address>,
    pub retiring: usize,
    pub stale: bool,
    pub truncated: bool,
    #[cfg(feature = "metrics")]
    pub queries: super::metrics::QueryCounts,
}
#[derive(Clone, Debug, Serialize)]
pub struct ServingOriginStatus {
    pub label: String,
    pub source: String,
    pub socket: Option<std::net::SocketAddr>,
}
impl UpstreamProvider {
    /// Reads current table truth and the local serving view without queries,
    /// election or event replay. This observation is not an adoption receipt;
    /// concurrent publication may advance either separately sampled projection.
    pub fn resolution_status(
        &self
    ) -> Result<Vec<ProfileResolutionStatus>, ResolverError> {
        self.resolution
            .bindings()
            .into_iter()
            .map(|(profile, binding)| {
                let plan = binding
                    .desired
                    .resolver_plan()
                    .expect("named binding has a plan");
                let names = plan
                    .names
                    .into_iter()
                    .zip(binding.owner.publications()?)
                    .map(|(name, (_, publication))| {
                        let set = publication
                            .as_ref()
                            .map(|p| p.snapshot.restore())
                            .transpose()?;
                        Ok(NameResolutionStatus {
                            label: name.label.clone(),
                            source: name.source,
                            revision: publication.as_ref().map(|p| p.revision),
                            generation: set.as_ref().map(|s| s.generation()),
                            active: set
                                .as_ref()
                                .map(|s| {
                                    s.active().map(|a| a.address).collect()
                                })
                                .unwrap_or_default(),
                            pending: set
                                .as_ref()
                                .map(|s| s.pending().collect())
                                .unwrap_or_default(),
                            retiring: set
                                .as_ref()
                                .map_or(0, |s| s.retiring().count()),
                            stale: set.as_ref().is_some_and(|s| s.stale()),
                            truncated: set
                                .as_ref()
                                .is_some_and(|s| s.truncated()),
                            #[cfg(feature = "metrics")]
                            queries: binding.owner.query_counts(&name.label)?,
                        })
                    })
                    .collect::<Result<Vec<_>, super::PublicationError>>()?;
                let serving = self
                    .registry
                    .service(&profile)
                    .map(|service| {
                        service
                            .inner
                            .profile
                            .origins
                            .iter()
                            .map(|origin| ServingOriginStatus {
                                label: origin.label.to_string(),
                                source: origin.source.to_string(),
                                socket: match origin.connect {
                                    crate::origin::ConnectTarget::Tcp(
                                        socket,
                                    ) => Some(socket),
                                    crate::origin::ConnectTarget::Tls(
                                        socket,
                                    ) => Some(socket),
                                    crate::origin::ConnectTarget::Unix(_) => {
                                        None
                                    }
                                },
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                Ok(ProfileResolutionStatus { profile, names, serving })
            })
            .collect()
    }
}
