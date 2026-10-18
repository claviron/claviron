//! Owns upstream app route selectors and exclusions from file configuration.

use claviron_helpers::de::{one_or_many, opt_one_or_many};
use serde::Deserialize;

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct UpstreamAppConfig {
    #[serde(default, deserialize_with = "one_or_many")]
    pub routes: Vec<UpstreamRouteConfig>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct UpstreamRouteConfig {
    #[serde(default, alias = "pool")]
    pub profile: Option<String>,
    #[serde(default)]
    pub exact: Option<String>,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default, deserialize_with = "opt_one_or_many")]
    pub paths: Option<Vec<String>>,
    #[serde(default)]
    pub regex: Option<String>,
    #[serde(default, deserialize_with = "one_or_many")]
    pub excludes: Vec<String>,
}
