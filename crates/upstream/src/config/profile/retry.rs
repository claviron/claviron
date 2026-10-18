//! Owns explicit retry admission and budget policy.

use std::time::Duration;

use serde::Deserialize;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryConfig {
    #[serde(default = "default_attempts")]
    pub attempts: usize,
    #[serde(default = "default_on")]
    pub on: Vec<RetryCause>,
    #[serde(default = "default_methods")]
    pub methods: Vec<String>,
    #[serde(default = "default_budget_percent")]
    pub budget_percent: u8,
    #[serde(default = "default_interval", with = "humantime_serde")]
    pub interval: Duration,
}
impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            attempts: default_attempts(),
            on: default_on(),
            methods: default_methods(),
            budget_percent: default_budget_percent(),
            interval: default_interval(),
        }
    }
}
fn default_attempts() -> usize {
    0
}
fn default_on() -> Vec<RetryCause> {
    vec![
        RetryCause::Connect,
        RetryCause::Refused,
        RetryCause::ResetBeforeResponse,
        RetryCause::ResponseHeaderTimeout,
    ]
}
fn default_methods() -> Vec<String> {
    vec!["GET".into(), "HEAD".into(), "OPTIONS".into()]
}
fn default_budget_percent() -> u8 {
    20
}
fn default_interval() -> Duration {
    Duration::from_secs(10)
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryCause {
    Connect,
    Refused,
    ResetBeforeResponse,
    ResponseHeaderTimeout,
}

impl RetryCause {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Connect => "connect",
            Self::Refused => "refused",
            Self::ResetBeforeResponse => "reset_before_response",
            Self::ResponseHeaderTimeout => "response_header_timeout",
        }
    }
    #[cfg(feature = "metrics")]
    pub(crate) const fn metric_index(self) -> usize {
        // Fixed metric array ordinal tracks the schema variants, not capacity.
        match self {
            Self::Connect => 0,
            Self::Refused => 1,
            Self::ResetBeforeResponse => 2,
            Self::ResponseHeaderTimeout => 3,
        }
    }
}
