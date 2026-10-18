//! Owns outbound host and forwarding-header policy.

use serde::Deserialize;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForwardConfig {
    #[serde(default = "default_host")]
    pub host: HostPolicy,
    #[serde(default = "default_via")]
    pub via: bool,
    #[serde(default = "default_forwarded")]
    pub forwarded: ForwardedPolicy,
    #[serde(default = "default_set")]
    pub set: std::collections::BTreeMap<String, String>,
    #[serde(default = "default_unset")]
    pub unset: Vec<String>,
}
impl Default for ForwardConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            via: default_via(),
            forwarded: default_forwarded(),
            set: default_set(),
            unset: default_unset(),
        }
    }
}
fn default_host() -> HostPolicy {
    HostPolicy::Origin
}
fn default_via() -> bool {
    true
}
fn default_forwarded() -> ForwardedPolicy {
    ForwardedPolicy::Xff
}
fn default_set() -> std::collections::BTreeMap<String, String> {
    Default::default()
}
fn default_unset() -> Vec<String> {
    Vec::new()
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostPolicy {
    Origin,
    Preserve,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForwardedPolicy {
    Xff,
    Rfc7239,
    None,
}
