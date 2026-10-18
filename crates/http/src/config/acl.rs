//! Desired per-host ACL declarations. Runtime compilation belongs to vhosts.

use std::collections::HashMap;
use std::fmt;
use std::net::IpAddr;

use ipnet::IpNet;
use serde::de::{Error, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};

use crate::ext::PathPrefix;

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AccessPolicy {
    Allow,
    Deny,
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccessLimiter {
    pub policy: AccessPolicy,
    #[serde(default)]
    pub allowed: Vec<IpNet>,
    #[serde(default)]
    pub denied: Vec<IpNet>,
}

impl AccessLimiter {
    /// Deny-by-default consults only `allowed`; allow-by-default consults
    /// only `denied`. Neither rule type merges with any parent ACL.
    pub fn is_allowed(
        &self,
        ip: IpAddr,
    ) -> bool {
        match self.policy {
            AccessPolicy::Deny => {
                self.allowed.iter().any(|network| network.contains(&ip))
            }
            AccessPolicy::Allow => {
                !self.denied.iter().any(|network| network.contains(&ip))
            }
        }
    }
}

/// Prefix-keyed rules. `/` is the host default; the longest matching prefix wins.
/// No implicit merging of parent/child allowlists or denylists takes place.
#[derive(Clone, Debug, Default)]
pub struct AclConfig {
    rules: HashMap<PathPrefix, AccessLimiter>,
}

impl AclConfig {
    pub fn rules(
        &self
    ) -> impl ExactSizeIterator<Item = (&PathPrefix, &AccessLimiter)> {
        self.rules.iter()
    }
}

impl<'de> Deserialize<'de> for AclConfig {
    fn deserialize<D: Deserializer<'de>>(
        deserializer: D
    ) -> Result<Self, D::Error> {
        struct Rules;
        impl<'de> Visitor<'de> for Rules {
            type Value = AclConfig;

            fn expecting(
                &self,
                formatter: &mut fmt::Formatter<'_>,
            ) -> fmt::Result {
                formatter.write_str("an ACL map keyed by path prefixes")
            }

            fn visit_map<M: MapAccess<'de>>(
                self,
                mut map: M,
            ) -> Result<Self::Value, M::Error> {
                let mut rules = HashMap::new();
                while let Some((prefix, rule)) =
                    map.next_entry::<PathPrefix, AccessLimiter>()?
                {
                    let path = prefix.as_str();
                    if path.contains(['%', '?', '#', '\\'])
                        || path.contains("//")
                        || path
                            .split('/')
                            .any(|part| matches!(part, "." | ".."))
                    {
                        return Err(M::Error::custom(format!(
                            "ambiguous ACL prefix '{prefix}'"
                        )));
                    }
                    if rules.contains_key(&prefix) {
                        return Err(M::Error::custom(format!(
                            "duplicate ACL prefix '{prefix}'"
                        )));
                    }
                    rules.insert(prefix, rule);
                }
                Ok(AclConfig { rules })
            }
        }
        deserializer.deserialize_map(Rules)
    }
}
