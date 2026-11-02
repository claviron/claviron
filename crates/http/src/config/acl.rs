//! An access rule for one selected scope. Route ownership supplies its prefix.

use std::net::IpAddr;

use ipnet::IpNet;
use serde::Deserialize;

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
