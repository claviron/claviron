//! Owns duration-shaped sparse configuration deserialization.
use std::time::Duration;

pub use claviron_helpers::overrides::Override;
use serde::Deserializer;
pub(crate) fn duration<'de, D: Deserializer<'de>>(
    de: D
) -> Result<Override<Duration>, D::Error> {
    claviron_helpers::de::deserialize_optional_duration(de)?
        .map(Override::Value)
        .ok_or_else(|| {
            serde::de::Error::custom("expected a duration, not null")
        })
}
pub(crate) fn optional_duration<'de, D: Deserializer<'de>>(
    de: D
) -> Result<Override<Option<Duration>>, D::Error> {
    claviron_helpers::de::deserialize_optional_duration(de).map(Override::Value)
}

/// A duration or `off`, for a field every profile must decide: `off` is
/// `None`, an explicit "no limit" — the field left out stays `Inherit`, so
/// compilation can tell the two apart.
pub(crate) fn duration_or_off<'de, D: Deserializer<'de>>(
    de: D
) -> Result<Override<Option<Duration>>, D::Error> {
    plain_duration_or_off(de).map(Override::Value)
}

/// The same, for the resolved field itself.
pub(crate) fn plain_duration_or_off<'de, D: Deserializer<'de>>(
    de: D
) -> Result<Option<Duration>, D::Error> {
    use serde::Deserialize;
    let text = String::deserialize(de)?;
    if text.trim().eq_ignore_ascii_case("off") {
        return Ok(None);
    }
    claviron_helpers::de::parse_duration(&text).map(Some).map_err(|error| {
        serde::de::Error::custom(format!("a duration, or off: {error}"))
    })
}
