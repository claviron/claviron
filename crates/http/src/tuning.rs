//! What a host's requests are admitted under, and the path deny set.
//!
//! Declared here so a server that is not the edge can read them; the edge
//! composes them into its HTTP configuration and a host's own value.

use claviron_helpers::de::deserialize_regex_set;
use regex::RegexSet;
use serde::{Deserialize, Deserializer};

use crate::policy::Limit;

/// Host defaults under `defaults.request`, read by every host. Proxy trust
/// belongs to listeners, not to the host a request selects.
#[derive(Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
#[expect(
    clippy::manual_non_exhaustive,
    reason = "the private serde field rejects a removed config key with a migration diagnostic"
)]
pub struct RouterConfig {
    /// Requests one host serves at once in a worker, across every path to
    /// it — router, bypass, a relay. Capacity, not policy: a request past it
    /// waits for a place up to its `requests.queue_timeout`, then is refused
    /// with 503. Unwritten is no limit, and nothing is kept for it.
    #[serde(default, deserialize_with = "optional_count")]
    pub max_concurrent_requests: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_regex_set")]
    pub deny: RegexSet,
    /// Removed; read only to say where each field went.
    #[allow(dead_code, reason = "read only to refuse the removed key")]
    #[serde(default, rename = "tuning", deserialize_with = "removed_tuning")]
    removed_tuning: (),
}

impl PartialEq for RouterConfig {
    fn eq(
        &self,
        other: &Self,
    ) -> bool {
        self.max_concurrent_requests == other.max_concurrent_requests
            && self.deny.patterns() == other.deny.patterns()
    }
}

impl Eq for RouterConfig {}

impl RouterConfig {
    pub fn deny_set(&self) -> &RegexSet {
        &self.deny
    }

    /// The limit a host is admitted under: its own when it writes one — a
    /// number, or `off` for none — and this default otherwise. A host's value
    /// replaces rather than narrows: it is that host's capacity, not a
    /// guarantee the listener gives every host.
    pub fn max_concurrent_requests_for(
        &self,
        host: Option<Limit<usize>>,
    ) -> Option<usize> {
        match host {
            Some(limit) => limit.value(),
            None => self.max_concurrent_requests,
        }
    }
}

/// `tuning:` said nothing about what it held, and half of it was the request
/// policy under another name.
pub fn removed_tuning<'de, D: Deserializer<'de>>(_: D) -> Result<(), D::Error> {
    Err(serde::de::Error::custom(
        "tuning is gone: max_concurrent_requests sits directly under defaults.request (and on a \
         host), and request_timeout is the listener's or host's requests.head_timeout",
    ))
}

/// A count, or absent.
fn optional_count<'de, D: Deserializer<'de>>(
    deserializer: D
) -> Result<Option<usize>, D::Error> {
    Option::<usize>::deserialize(deserializer)
}

/// A count or `off`, for a host that replaces the default.
pub fn optional_count_limit<'de, D>(
    deserializer: D
) -> Result<Option<Limit<usize>>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Raw {
        Count(usize),
        Text(String),
    }
    match Option::<Raw>::deserialize(deserializer)? {
        None => Ok(None),
        Some(Raw::Count(count)) => Ok(Some(Limit::Value(count))),
        Some(Raw::Text(text)) if text.trim().eq_ignore_ascii_case("off") => {
            Ok(Some(Limit::Off))
        }
        Some(Raw::Text(text)) => Err(serde::de::Error::custom(format!(
            "expected a count or off, found {text:?}"
        ))),
    }
}
