//! Validated host response declarations; matching/rendering belongs to vhosts.

use http::{HeaderValue, StatusCode};
use serde::Deserialize;

use crate::ext::PathPrefix;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize)]
#[serde(try_from = "u16")]
pub enum RedirectStatus {
    MovedPermanently,
    Found,
    SeeOther,
    TemporaryRedirect,
    PermanentRedirect,
}

impl RedirectStatus {
    pub fn status(self) -> StatusCode {
        match self {
            Self::MovedPermanently => StatusCode::MOVED_PERMANENTLY,
            Self::Found => StatusCode::FOUND,
            Self::SeeOther => StatusCode::SEE_OTHER,
            Self::TemporaryRedirect => StatusCode::TEMPORARY_REDIRECT,
            Self::PermanentRedirect => StatusCode::PERMANENT_REDIRECT,
        }
    }
}

impl TryFrom<u16> for RedirectStatus {
    type Error = &'static str;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            301 => Ok(Self::MovedPermanently),
            302 => Ok(Self::Found),
            303 => Ok(Self::SeeOther),
            307 => Ok(Self::TemporaryRedirect),
            308 => Ok(Self::PermanentRedirect),
            _ => Err("redirect code must be 301, 302, 303, 307 or 308"),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(try_from = "String")]
pub struct LocationTemplate(String);

impl LocationTemplate {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for LocationTemplate {
    type Error = &'static str;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.is_empty() || value.chars().any(char::is_whitespace) {
            return Err("redirect target must be nonempty and URI-escaped");
        }
        HeaderValue::from_str(&value)
            .map_err(|_| "invalid redirect Location header")?;
        Ok(Self(value))
    }
}

fn default_redirect_code() -> RedirectStatus {
    RedirectStatus::TemporaryRedirect
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedirectConfig {
    #[serde(default = "PathPrefix::root")]
    pub path: PathPrefix,
    #[serde(default = "default_redirect_code")]
    pub code: RedirectStatus,
    pub to: LocationTemplate,
}
