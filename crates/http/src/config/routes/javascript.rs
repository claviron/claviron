use super::JavaScriptWebSocketConfig;
use crate::ext::PathPrefix;
use claviron_helpers::de::opt_one_or_many;
use serde::Deserialize;

/// One route mapping to a JavaScript application configured by `js.yaml`.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JavaScriptRouteConfig {
    /// Optional segment-safe request prefix. Omission claims the whole host.
    pub path: Option<PathPrefix>,
    /// Stable application name resolved by the worker-local runtime service.
    #[serde(deserialize_with = "deserialize_javascript_application_name")]
    pub application: String,
    /// Exact WebSocket endpoints relative to this application's mount.
    #[serde(
        default,
        alias = "websocket",
        deserialize_with = "opt_one_or_many"
    )]
    websockets: Option<Vec<JavaScriptWebSocketConfig>>,
    /// This route's request policy, narrowing its host's and listener's: the
    /// wait for an isolate is `requests.queue_timeout`.
    #[serde(default)]
    pub requests: crate::policy::RequestPolicyOverride,
    /// This route's reply policy.
    #[serde(default)]
    pub responses: crate::policy::ResponsePolicyOverride,
}

fn deserialize_javascript_application_name<'de, D>(
    deserializer: D
) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let name = String::deserialize(deserializer)?;
    if name.trim().is_empty() {
        return Err(serde::de::Error::custom(
            "JavaScript route application must not be empty",
        ));
    }
    Ok(name)
}

impl JavaScriptRouteConfig {
    pub fn websockets(&self) -> &[JavaScriptWebSocketConfig] {
        self.websockets.as_deref().unwrap_or(&[])
    }
}
