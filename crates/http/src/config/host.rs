//! What a host declares beside its handlers: the response headers it
//! rewrites and the upstream it names. Plain data, so a
//! server that is not the edge reads it the same way.

use claviron_helpers::de::one_or_many;
use http::{HeaderName, HeaderValue};
use serde::Deserialize;

/// Default `Server:` header value when a host doesn't override.
///
/// Goes through every response leaving the host pipeline. Lowercase on
/// purpose — nginx historically did not let operators rename their
/// `Server:` header at all; claviron lifts that restriction down to
/// per-vhost scope (a tiny but real security knob — fingerprinters key
/// off the `Server:` token to pick exploit payloads). The default is the
/// product name in plain casing; vhosts can swap in a generic value
/// (`"web"`, `"caddy"`, `"nginx"`) to throw scanners off.
pub const DEFAULT_SERVER_HEADER: &str = "claviron";

/// Server-wide defaults or per-vhost response-header configuration — nginx
/// `headers-more` semantics (`more_set_headers` + `more_clear_headers`)
/// reused at server and host scope.
///
/// `set:` overrides the named response headers; `unset:` removes them
/// from whatever the inner pipeline (axum handlers, file handler, PHP
/// middleware) produced. The layer applies **unset first, then set**,
/// so an explicit `set:` of a name that was also listed in `unset:`
/// resolves to the operator's set value.
///
/// Both maps are RFC-7230-validated at boot via `http::HeaderName` /
/// `HeaderValue::from_str`. Invalid names (whitespace, control chars)
/// or invalid values surface as a yaml parse error with the offending
/// line, never reach the runtime.
///
/// Scope note: Edge's common exchange applies this policy outside application
/// routing, whenever explicitly configured, regardless of whether the owner
/// is a configured host plan, Axum or another Rust handler.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
pub struct HostHeadersConfig {
    /// Override the `Server:` response header for this vhost. When unset,
    /// ordinary edge owners use the server-wide `http.yaml` default, or
    /// [`DEFAULT_SERVER_HEADER`]. Standalone owners retain their own identity
    /// unless a server-wide or vhost override was explicitly configured.
    #[serde(default, deserialize_with = "deserialize_server")]
    pub server: Option<String>,
    /// Override (set/replace) named headers on every response. Each
    /// (name, value) pair is validated at parse time.
    #[serde(default, deserialize_with = "deserialize_header_set")]
    pub set: Vec<(HeaderName, HeaderValue)>,
    /// Remove named headers from every response — silences upstream
    /// fingerprints (`X-Powered-By`, `X-AspNet-Version`, …) and
    /// debug-trace headers leaked by app code.
    #[serde(default, deserialize_with = "deserialize_header_names")]
    pub unset: Vec<HeaderName>,
}

fn deserialize_server<'de, D>(
    deserializer: D
) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<String>::deserialize(deserializer)?;
    if let Some(value) = &value {
        HeaderValue::from_str(value).map_err(serde::de::Error::custom)?;
    }
    Ok(value)
}

impl HostHeadersConfig {
    pub fn is_empty(&self) -> bool {
        self.server.is_none() && self.set.is_empty() && self.unset.is_empty()
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(transparent)]
pub struct HostUpstreamConfig {
    /// Named upstream app/profile in `config/upstream.yaml`.
    name: String,
}

impl HostUpstreamConfig {
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Deserialize a yaml mapping of `header-name: value` pairs into a
/// validated `Vec<(HeaderName, HeaderValue)>`. Yaml ordering is not
/// preserved across all yaml libs — duplicates collapse last-wins.
fn deserialize_header_set<'de, D>(
    deserializer: D
) -> Result<Vec<(HeaderName, HeaderValue)>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error as DeError;
    let raw: std::collections::HashMap<String, String> =
        std::collections::HashMap::deserialize(deserializer)?;
    let mut out = Vec::with_capacity(raw.len());
    for (k, v) in raw {
        let name = HeaderName::try_from(k.as_str()).map_err(|err| {
            DeError::custom(format!("invalid header name '{k}': {err}"))
        })?;
        let value = HeaderValue::from_str(v.as_str()).map_err(|err| {
            DeError::custom(format!("invalid header value for '{k}': {err}"))
        })?;
        out.push((name, value));
    }
    // Mapping order is not policy; stable snapshots must compare equally.
    out.sort_unstable_by(|(a, _), (b, _)| a.as_str().cmp(b.as_str()));
    Ok(out)
}

/// Deserialize a yaml sequence of header names into validated
/// `HeaderName`s. Single string or list both accepted.
fn deserialize_header_names<'de, D>(
    deserializer: D
) -> std::result::Result<Vec<HeaderName>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error as DeError;
    let raw: Vec<String> = one_or_many(deserializer)?;
    raw.into_iter()
        .map(|name| {
            HeaderName::try_from(name.as_str()).map_err(|err| {
                DeError::custom(format!("invalid header name '{name}': {err}"))
            })
        })
        .collect()
}
