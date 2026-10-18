//! Sparse configuration overrides shared by declaration crates.

use std::time::Duration;

use serde::{Deserialize, Deserializer};

/// An omitted field inherits; a present field replaces the inherited value.
///
/// Use `Override<Option<T>>` for nullable settings: `Value(None)` explicitly
/// disables them. For non-nullable settings, `Override<T>` rejects YAML null
/// through `T`'s deserializer. Lists replace, so `Value(vec![])` clears.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Override<T> {
    #[default]
    Inherit,
    Value(T),
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Override<T> {
    fn deserialize<D: Deserializer<'de>>(
        deserializer: D
    ) -> Result<Self, D::Error> {
        T::deserialize(deserializer).map(Self::Value)
    }
}

impl<T: Clone> Override<T> {
    pub fn resolve(
        &self,
        inherited: &T,
    ) -> T {
        match self {
            Self::Inherit => inherited.clone(),
            Self::Value(value) => value.clone(),
        }
    }
}

pub fn optional_duration<'de, D>(
    deserializer: D
) -> Result<Override<Option<Duration>>, D::Error>
where
    D: Deserializer<'de>,
{
    crate::de::deserialize_optional_duration(deserializer).map(Override::Value)
}

pub fn duration<'de, D>(deserializer: D) -> Result<Override<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    required_duration(deserializer).map(Override::Value)
}

pub fn required_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    crate::de::deserialize_optional_duration(deserializer)?.ok_or_else(|| {
        serde::de::Error::custom("expected a duration, not null")
    })
}

impl<T> Override<T> {
    pub fn is_inherit(&self) -> bool {
        matches!(self, Self::Inherit)
    }
}
