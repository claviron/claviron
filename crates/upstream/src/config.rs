//! Owns upstream file configuration and its typed option groups.

use std::collections::BTreeMap;

#[cfg(feature = "metrics")]
use metrics::UpstreamMetricsConfig;
use serde::Deserialize;
pub(crate) mod app;
pub(crate) mod error;
#[cfg(feature = "metrics")]
pub(crate) mod metrics;
pub(crate) mod origin;
pub(crate) mod overrides;
pub(crate) mod profile;
use crate::config::app::UpstreamAppConfig;
use crate::config::profile::ProfileConfig;
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct UpstreamYaml {
    #[cfg(feature = "metrics")]
    #[serde(default)]
    pub metrics: UpstreamMetricsConfig,
    #[cfg(not(feature = "metrics"))]
    #[serde(default, rename = "metrics")]
    _metrics: serde::de::IgnoredAny,
    #[serde(default)]
    pub upstreams: BTreeMap<String, ProfileConfig>,
    #[serde(default)]
    pub apps: BTreeMap<String, UpstreamAppConfig>,
}

impl UpstreamYaml {
    /// No profiles and no apps: the document declares that there is no
    /// upstream, which is also what an absent file declares.
    pub fn is_empty(&self) -> bool {
        self.upstreams.is_empty() && self.apps.is_empty()
    }
}
