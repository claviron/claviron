use std::time::Duration;

use serde::de::{IntoDeserializer, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};

#[cfg(test)]
#[path = "../tests/de.rs"]
mod tests;

/// A duration as configuration writes it (`"30s"`, `"1m 30s"`), for a
/// deserializer that has already read the string itself — one that also
/// accepts a word such as `off` in the same place.
pub fn parse_duration(
    text: &str
) -> Result<Duration, humantime::DurationError> {
    humantime::parse_duration(text.trim())
}

pub fn deserialize_duration<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: From<Duration> + Default,
{
    let s: Option<String> = Option::deserialize(deserializer)?;
    match s {
        Some(duration_str) => parse_duration(&duration_str)
            .map(T::from)
            .map_err(serde::de::Error::custom),
        None => Ok(T::default()),
    }
}
pub fn deserialize_optional_duration<'de, D>(
    deserializer: D
) -> Result<Option<Duration>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(match Option::<String>::deserialize(deserializer)? {
        Some(s) => Some(parse_duration(&s).map_err(serde::de::Error::custom)?),
        None => None,
    })
}

pub fn deserialize_lowercase_vec<'de, D>(
    deserializer: D
) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let v = Vec::<String>::deserialize(deserializer)?;
    Ok(v.into_iter().map(|h| h.to_ascii_lowercase()).collect())
}

use std::net::{IpAddr, SocketAddr, ToSocketAddrs};

use regex::{Regex, RegexSet};

pub fn deserialize_duration_secs<'de, D>(
    deserializer: D
) -> Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(deserializer)?;
    match s {
        None => Ok(None),
        Some(duration_str) => humantime::parse_duration(&duration_str)
            .map(|d| Some(d.as_secs()))
            .map_err(serde::de::Error::custom),
    }
}

pub fn deserialize_byte_size<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    use byte_unit::Byte;
    let s: String = String::deserialize(deserializer)?;
    let byte_val = s.parse::<Byte>().map_err(|e| {
        serde::de::Error::custom(format!("invalid byte size: {}", e))
    })?;
    Ok(byte_val.as_u64())
}

pub fn deserialize_optional_socket_addr<'de, D>(
    deserializer: D
) -> Result<Option<SocketAddr>, D::Error>
where
    D: Deserializer<'de>,
{
    // First, try to deserialize the field into an `Option<String>`.
    // This correctly handles cases where the value is `null`.
    let s: Option<String> = Option::deserialize(deserializer)?;

    // If the value is `None`, return `Ok(None)`.
    let s = match s {
        Some(s) => s,
        None => return Ok(None),
    };

    // If a string value exists, proceed with parsing or hostname resolution.
    if let Ok(addr) = s.parse::<SocketAddr>() {
        return Ok(Some(addr));
    }

    // Resolve the hostname if parsing as a SocketAddr fails.
    let mut addresses =
        s.to_socket_addrs().map_err(serde::de::Error::custom)?;

    // Return the first resolved address or an error if none are found.
    addresses.next().map(Some).ok_or_else(|| {
        serde::de::Error::custom("failed to resolve socket address")
    })
}

pub fn deserialize_optional_regex<'de, D>(
    deserializer: D
) -> Result<Option<Regex>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt = Option::<String>::deserialize(deserializer)?;
    opt.map(|s| Regex::new(&s).map_err(serde::de::Error::custom)).transpose()
}

pub fn deserialize_regex_set<'de, D>(de: D) -> Result<RegexSet, D::Error>
where
    D: Deserializer<'de>,
{
    let pats: Vec<String> = Vec::<String>::deserialize(de)?;
    RegexSet::new(&pats).map_err(serde::de::Error::custom)
}

/// Allow a field to be specified as either a single item or a list of items during deserialization.
/// Example YAML:
///   tls:
///     key_path: ...
///     cert_path: ...
///     names: [ example.com ]
/// or:
///   tls:
///     - key_path: ...
///       cert_path: ...
///       names: [ example.com ]
pub fn one_or_many<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    OneOrMany::<T>::deserialize(deserializer).map(|value| value.0)
}

/// Allow a field to be omitted, a single item, or a list of items.
/// Example YAML:
///   redirect: null
///   redirect:
///     path: /old
///     code: 301
///     to: /new
///   redirect:
///     - path: /a
///       code: 302
///       to: /b
///     - path: /c
///       code: 308
///       to: /d
pub fn opt_one_or_many<'de, D, T>(
    deserializer: D
) -> Result<Option<Vec<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<OneOrMany<T>>::deserialize(deserializer)
        .map(|value| value.map(|value| value.0))
}

struct OneOrMany<T>(Vec<T>);

impl<'de, T: Deserialize<'de>> Deserialize<'de> for OneOrMany<T> {
    fn deserialize<D: Deserializer<'de>>(
        deserializer: D
    ) -> Result<Self, D::Error> {
        struct ItemsVisitor<T>(std::marker::PhantomData<T>);

        // Dispatch by input shape instead of retrying untagged alternatives:
        // an item's schema/validation error must reach the caller unchanged.
        impl<'de, T: Deserialize<'de>> Visitor<'de> for ItemsVisitor<T> {
            type Value = OneOrMany<T>;

            fn expecting(
                &self,
                formatter: &mut std::fmt::Formatter,
            ) -> std::fmt::Result {
                formatter.write_str("a single item or a list of items")
            }

            fn visit_map<A: MapAccess<'de>>(
                self,
                map: A,
            ) -> Result<Self::Value, A::Error> {
                T::deserialize(serde::de::value::MapAccessDeserializer::new(
                    map,
                ))
                .map(|item| OneOrMany(vec![item]))
            }

            fn visit_seq<A: SeqAccess<'de>>(
                self,
                sequence: A,
            ) -> Result<Self::Value, A::Error> {
                Vec::<T>::deserialize(
                    serde::de::value::SeqAccessDeserializer::new(sequence),
                )
                .map(OneOrMany)
            }

            fn visit_borrowed_str<E: serde::de::Error>(
                self,
                value: &'de str,
            ) -> Result<Self::Value, E> {
                T::deserialize(serde::de::value::BorrowedStrDeserializer::new(
                    value,
                ))
                .map(|item| OneOrMany(vec![item]))
            }

            fn visit_str<E: serde::de::Error>(
                self,
                value: &str,
            ) -> Result<Self::Value, E> {
                T::deserialize(value.into_deserializer())
                    .map(|item| OneOrMany(vec![item]))
            }

            fn visit_string<E: serde::de::Error>(
                self,
                value: String,
            ) -> Result<Self::Value, E> {
                T::deserialize(value.into_deserializer())
                    .map(|item| OneOrMany(vec![item]))
            }

            fn visit_bool<E: serde::de::Error>(
                self,
                value: bool,
            ) -> Result<Self::Value, E> {
                T::deserialize(value.into_deserializer())
                    .map(|item| OneOrMany(vec![item]))
            }

            fn visit_i64<E: serde::de::Error>(
                self,
                value: i64,
            ) -> Result<Self::Value, E> {
                T::deserialize(value.into_deserializer())
                    .map(|item| OneOrMany(vec![item]))
            }

            fn visit_u64<E: serde::de::Error>(
                self,
                value: u64,
            ) -> Result<Self::Value, E> {
                T::deserialize(value.into_deserializer())
                    .map(|item| OneOrMany(vec![item]))
            }

            fn visit_f64<E: serde::de::Error>(
                self,
                value: f64,
            ) -> Result<Self::Value, E> {
                T::deserialize(value.into_deserializer())
                    .map(|item| OneOrMany(vec![item]))
            }

            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                T::deserialize(serde::de::value::UnitDeserializer::new())
                    .map(|item| OneOrMany(vec![item]))
            }
        }

        deserializer.deserialize_any(ItemsVisitor(std::marker::PhantomData))
    }
}

pub fn deserialize_bind_address<'de, D>(
    deserializer: D
) -> Result<SocketAddr, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum BindAddr {
        Socket(SocketAddr),
        Ip(IpAddr),
        Text(String),
    }

    let value = BindAddr::deserialize(deserializer)?;
    match value {
        BindAddr::Socket(addr) => Ok(addr),
        BindAddr::Ip(ip) => Ok(SocketAddr::new(ip, 50051)),
        BindAddr::Text(raw) => {
            if let Ok(addr) = raw.parse::<SocketAddr>() {
                return Ok(addr);
            }
            if let Ok(ip) = raw.parse::<IpAddr>() {
                return Ok(SocketAddr::new(ip, 50051));
            }
            Err(serde::de::Error::custom(format!(
                "invalid gRPC bind address '{raw}', expected 'IP:PORT'"
            )))
        }
    }
}
