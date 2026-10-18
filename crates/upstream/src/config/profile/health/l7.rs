//! Owns serving-axis HTTP probe declaration.

use std::time::Duration;

use claviron_macros::Overridable;
use serde::Deserialize;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Overridable)]
#[serde(deny_unknown_fields)]
pub struct L7Config {
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default = "default_path")]
    pub path: String,
    #[serde(default = "default_method")]
    pub method: String,
    #[overridable(nested)]
    #[serde(default = "default_expect")]
    pub expect: ExpectedResponseConfig,
    #[serde(default = "default_interval", with = "humantime_serde")]
    pub interval: Duration,
    #[serde(default = "default_timeout", with = "humantime_serde")]
    pub timeout: Duration,
    #[serde(default = "default_failure_threshold")]
    pub failure_threshold: u32,
    #[serde(default = "default_success_threshold")]
    pub success_threshold: u32,
}
impl Default for L7Config {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            path: default_path(),
            method: default_method(),
            expect: default_expect(),
            interval: default_interval(),
            timeout: default_timeout(),
            failure_threshold: default_failure_threshold(),
            success_threshold: default_success_threshold(),
        }
    }
}
fn default_enabled() -> bool {
    false
}
fn default_path() -> String {
    "/healthz".into()
}
fn default_method() -> String {
    "GET".into()
}
fn default_expect() -> ExpectedResponseConfig {
    ExpectedResponseConfig::default()
}
fn default_interval() -> Duration {
    Duration::from_secs(10)
}
fn default_timeout() -> Duration {
    Duration::from_secs(2)
}
fn default_failure_threshold() -> u32 {
    3
}
fn default_success_threshold() -> u32 {
    1
}
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Overridable)]
#[serde(deny_unknown_fields)]
pub struct ExpectedResponseConfig {
    #[serde(default = "default_status")]
    pub status: [u16; 2],
    #[serde(default = "default_body_contains")]
    pub body_contains: Option<String>,
}
impl Default for ExpectedResponseConfig {
    fn default() -> Self {
        Self {
            status: default_status(),
            body_contains: default_body_contains(),
        }
    }
}
fn default_status() -> [u16; 2] {
    [200, 299]
}
fn default_body_contains() -> Option<String> {
    None
}
