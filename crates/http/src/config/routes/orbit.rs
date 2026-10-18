use crate::ext::PathPrefix;
use serde::Deserialize;

/// One route reaching an application process over Orbit shared memory.
///
/// `fleet_name` selects the shared-memory namespace; `application` selects
/// the service advertised inside it. No socket or raw SHM path is required.
/// The edge reads the lanes serving that name and writes to the idlest.
///
/// Deliberately named for the transport rather than for a language: what is on
/// the other side may be Python today and something else tomorrow, and the
/// edge cannot tell the difference.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrbitRouteConfig {
    /// Optional segment-safe request prefix. Omission claims the whole host.
    pub path: Option<PathPrefix>,
    /// The destination namespace, not this edge's own process-family fleet.
    /// Must match the runner's `fleet_name`.
    #[serde(alias = "fleet", deserialize_with = "deserialize_fleet_name")]
    pub fleet_name: claviron_runtime::FleetName,
    /// The name the application process advertises in its lane.
    #[serde(deserialize_with = "deserialize_orbit_application_name")]
    pub application: String,
    /// This application's request policy, narrowing its host's and its
    /// listener's (`docs/REVIEW.md` §3e).
    #[serde(default)]
    pub requests: crate::policy::RequestPolicyOverride,
}

#[cfg(test)]
#[path = "../../../tests/config/routes/orbit.rs"]
mod tests;

fn deserialize_fleet_name<'de, D>(
    deserializer: D
) -> Result<claviron_runtime::FleetName, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let name = String::deserialize(deserializer)?;
    claviron_runtime::FleetName::try_new(&name)
        .map_err(serde::de::Error::custom)
}

fn deserialize_orbit_application_name<'de, D>(
    deserializer: D
) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let name = String::deserialize(deserializer)?;
    let name = name.trim();
    if name.is_empty() {
        return Err(serde::de::Error::custom(
            "Orbit route application must not be empty",
        ));
    }
    // A lane advertises the name in a fixed field, so a name that does not fit
    // one is a configuration error here rather than a truncation there.
    if name.len() > 47 {
        return Err(serde::de::Error::custom(
            "Orbit route application must be at most 47 bytes: it has to fit a lane",
        ));
    }
    Ok(name.to_owned())
}
