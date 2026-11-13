use serde::Deserialize;

pub use claviron_types::link::LinkBalance;

/// One route reaching an application process over Orbit shared memory.
///
/// `fleet_name` selects the shared-memory namespace; `application` selects
/// the service advertised inside it. No socket or raw SHM path is required.
/// `balance` selects a receiver among the lanes serving that name.
///
/// Deliberately named for the transport rather than for a language: what is on
/// the other side may be Python today and something else tomorrow, and the
/// edge cannot tell the difference.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[expect(
    clippy::manual_non_exhaustive,
    reason = "the private serde field rejects a removed config key with a migration diagnostic"
)]
pub struct OrbitRouteConfig {
    /// The destination namespace, not this edge's own process-family fleet.
    /// Must match the runner's `fleet_name`.
    #[serde(alias = "fleet", deserialize_with = "deserialize_fleet_name")]
    pub fleet_name: claviron_runtime::FleetName,
    /// The name the application process advertises in its lane.
    #[serde(deserialize_with = "deserialize_orbit_application_name")]
    pub application: String,
    /// Per-exchange receiver selection; independent of the worker policy.
    #[serde(default)]
    pub balance: LinkBalance,
    /// Bodyless GET replies on this mount fit in a single Link frame.
    #[serde(default)]
    pub inline_response: bool,
    /// Removed; read only to say so. The route's receiver selection is
    /// `balance`; `policy` now names only the owner's worker policy.
    #[serde(default, rename = "policy", deserialize_with = "removed_policy")]
    #[expect(
        dead_code,
        reason = "never read: it exists only for its deserialization error"
    )]
    removed_policy: (),
}

impl OrbitRouteConfig {
    /// A route declared in code, as its YAML would declare it.
    pub fn new(
        fleet_name: claviron_runtime::FleetName,
        application: String,
        balance: LinkBalance,
    ) -> Self {
        Self {
            fleet_name,
            application,
            balance,
            inline_response: false,
            removed_policy: (),
        }
    }
}

fn removed_policy<'de, D: serde::Deserializer<'de>>(
    _: D
) -> Result<(), D::Error> {
    Err(serde::de::Error::custom(
        "an Orbit route's `policy` is now `balance: first | round_robin`",
    ))
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
