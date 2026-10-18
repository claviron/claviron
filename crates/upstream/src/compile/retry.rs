//! Prepared retry method eligibility; disabled policy leaves unused values inert.
use http::Method;

use crate::config::error::ConfigError;
use crate::config::profile::retry::RetryConfig;
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Prepared {
    methods: Box<[Method]>,
}
impl Prepared {
    pub(crate) fn compile(
        profile: &str,
        config: &RetryConfig,
    ) -> Result<Option<Self>, ConfigError> {
        if config.attempts == 0 {
            return Ok(None);
        }
        let invalid = |detail: String| ConfigError::InvalidOrigin {
            profile: profile.into(),
            origin: "retry".into(),
            detail,
        };
        if config.interval.is_zero() || config.budget_percent > 100 {
            return Err(invalid(
                "enabled retry requires interval > 0 and budget_percent <= 100"
                    .into(),
            ));
        }
        let methods = config
            .methods
            .iter()
            .map(|method| {
                method.parse::<Method>().map_err(|e| invalid(e.to_string()))
            })
            .collect::<Result<_, _>>()?;
        Ok(Some(Self { methods }))
    }
    pub(crate) fn eligible(
        &self,
        method: &Method,
    ) -> bool {
        self.methods.contains(method)
    }
}
