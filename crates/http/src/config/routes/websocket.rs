use crate::ext::PathPrefix;
use serde::Deserialize;

/// One exact WebSocket endpoint owned by a persistent JavaScript application.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JavaScriptWebSocketConfig {
    /// Exact path relative to the application's optional mount prefix.
    pub path: PathPrefix,
}
