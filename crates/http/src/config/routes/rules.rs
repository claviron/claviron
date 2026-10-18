//! Pure route-entry declarations shared by edge and application adapters.

use crate::ext::PathPrefix;
use serde::Deserialize;

/// Legacy route-entry declaration retained only for configuration migration.
/// Current route conditions are typed extensions registered by the consumer.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteRule {
    /// Segment prefix; omission matches the whole route group.
    #[serde(default = "PathPrefix::root")]
    pub path: PathPrefix,
    /// A dispatch condition, not an authentication or authorization boundary.
    #[serde(default)]
    pub ajax_only: bool,
}
