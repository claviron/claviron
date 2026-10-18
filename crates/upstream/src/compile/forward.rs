//! Boot-prepared static request header rewrites.
use http::header::{HeaderName, HeaderValue};

use crate::config::error::ConfigError;
use crate::config::profile::forward::ForwardConfig;
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Prepared {
    pub(crate) set: Box<[(HeaderName, HeaderValue)]>,
    pub(crate) unset: Box<[HeaderName]>,
}
impl Prepared {
    pub(crate) fn compile(
        profile: &str,
        config: &ForwardConfig,
    ) -> Result<Self, ConfigError> {
        let invalid = |detail: String| ConfigError::InvalidOrigin {
            profile: profile.into(),
            origin: "forward".into(),
            detail,
        };
        let set = config
            .set
            .iter()
            .map(|(name, value)| {
                Ok((
                    name.parse::<HeaderName>()
                        .map_err(|e| invalid(e.to_string()))?,
                    value
                        .parse::<HeaderValue>()
                        .map_err(|e| invalid(e.to_string()))?,
                ))
            })
            .collect::<Result<_, ConfigError>>()?;
        let unset = config
            .unset
            .iter()
            .map(|name| {
                name.parse::<HeaderName>().map_err(|e| invalid(e.to_string()))
            })
            .collect::<Result<_, _>>()?;
        Ok(Self { set, unset })
    }
}
