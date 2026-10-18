//! Prefix-keyed route declarations and immutable lookup tables.

use crate::ext::PathPrefix;
use serde::de::{Error, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use std::fmt;

/// A prepared prefix-to-route mapping. Longest segment prefix wins.
///
/// Configuration is a map keyed by `PathPrefix`, never a regex. Deserialization
/// rejects duplicate normalized prefixes before a candidate can be published.
/// The value belongs to the consumer: declarations and compiled handlers use
/// the same table without coupling HTTP to any execution engine.
#[derive(Clone, Debug)]
pub struct RouteTable<T> {
    entries: Vec<(PathPrefix, T)>,
}

impl<T> Default for RouteTable<T> {
    fn default() -> Self {
        Self { entries: Vec::new() }
    }
}

impl<T> RouteTable<T> {
    /// Prepare already-validated entries; the compiler owns ownership checks.
    pub fn new(entries: impl IntoIterator<Item = (PathPrefix, T)>) -> Self {
        let mut entries: Vec<_> = entries.into_iter().collect();
        entries.sort_by_key(|(prefix, _)| {
            std::cmp::Reverse(prefix.as_str().len())
        });
        Self { entries }
    }

    pub fn find(
        &self,
        path: &str,
    ) -> Option<&T> {
        self.entries
            .iter()
            .find(|(prefix, _)| prefix.has_path_segment(path))
            .map(|(_, value)| value)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&PathPrefix, &T)> {
        self.entries.iter().map(|(prefix, value)| (prefix, value))
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for RouteTable<T> {
    fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        struct Routes<T>(std::marker::PhantomData<T>);
        impl<'de, T: Deserialize<'de>> Visitor<'de> for Routes<T> {
            type Value = RouteTable<T>;

            fn expecting(
                &self,
                formatter: &mut fmt::Formatter,
            ) -> fmt::Result {
                formatter.write_str("a map of path prefixes to routes")
            }

            fn visit_map<M: MapAccess<'de>>(
                self,
                mut map: M,
            ) -> Result<Self::Value, M::Error> {
                let mut entries = Vec::new();
                while let Some((prefix, value)) =
                    map.next_entry::<PathPrefix, T>()?
                {
                    if entries.iter().any(|(existing, _)| existing == &prefix) {
                        return Err(M::Error::custom(format!(
                            "duplicate route prefix '{prefix}'"
                        )));
                    }
                    entries.push((prefix, value));
                }
                Ok(RouteTable::new(entries))
            }
        }
        de.deserialize_map(Routes(std::marker::PhantomData))
    }
}

#[cfg(feature = "js")]
pub mod javascript;
#[cfg(feature = "lua")]
pub mod lua;
#[cfg(feature = "orbit")]
pub mod orbit;
pub mod rules;
#[cfg(any(feature = "js", feature = "lua"))]
pub mod websocket;

#[cfg(feature = "js")]
pub use javascript::JavaScriptRouteConfig;
#[cfg(feature = "lua")]
pub use lua::LuaRouteConfig;
#[cfg(feature = "orbit")]
pub use orbit::OrbitRouteConfig;
pub use rules::RouteRule;
#[cfg(any(feature = "js", feature = "lua"))]
pub use websocket::JavaScriptWebSocketConfig;
