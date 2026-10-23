//! Filesystem routing declarations, without a host or execution engine.

use super::spa::SpaConfig;
use super::statics::StaticConfig;
use serde::Deserialize;
use std::path::PathBuf;

/// The complete file-serving view of a site or selected route.
/// Relative roots remain process-working-directory relative; consumers anchor
/// them when preparing a runtime snapshot, not on every request.
#[derive(Clone, Debug, Deserialize)]
pub struct FilesConfig {
    pub root: PathBuf,
    /// Resolved by the route owner, never deserialized as another root.
    #[serde(skip)]
    pub mapping: Option<super::mapping::RouteMapping>,
    #[serde(default, deserialize_with = "claviron_helpers::de::one_or_many")]
    pub index: Vec<String>,
    #[serde(default)]
    pub try_files: bool,
    pub spa: Option<SpaConfig>,
    pub statics: Option<Vec<StaticConfig>>,
}
