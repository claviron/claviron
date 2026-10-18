//! Owns compact and detailed origin file declarations.

use serde::Deserialize;

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum OriginConfig {
    Url(String),
    Detailed(DetailedOriginConfig),
}

impl OriginConfig {
    pub(crate) fn target(&self) -> &str {
        match self {
            Self::Url(target) => target,
            Self::Detailed(origin) => &origin.target,
        }
    }

    pub(crate) fn authority(&self) -> Option<&str> {
        match self {
            Self::Url(_) => None,
            Self::Detailed(origin) => origin.authority.as_deref(),
        }
    }

    pub(crate) fn weight(&self) -> u32 {
        match self {
            Self::Url(_) => default_weight(),
            Self::Detailed(origin) => origin.weight,
        }
    }
    pub(crate) fn max_live(&self) -> Option<usize> {
        match self {
            Self::Url(_) => None,
            Self::Detailed(origin) => origin.max_live,
        }
    }

    pub(crate) fn name(&self) -> Option<&str> {
        match self {
            Self::Url(_) => None,
            Self::Detailed(origin) => origin.name.as_deref(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DetailedOriginConfig {
    #[serde(default)]
    pub name: Option<String>,
    pub target: String,
    #[serde(default)]
    pub authority: Option<String>,
    #[serde(default = "default_weight")]
    pub weight: u32,
    /// Requests this worker may have in flight to this origin before `first`
    /// moves on to the next one. Absent, the profile's `capacity.max_live` is
    /// the limit — the pool is full, so the next origin — which is what
    /// HAProxy's `maxconn` is for `balance first`. Read by `first` only.
    #[serde(default)]
    pub max_live: Option<usize>,
}

fn default_weight() -> u32 {
    1
}
