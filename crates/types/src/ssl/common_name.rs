use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// X.509 subject common name, not a DNS hostname or an SNI routing key.
/// Preserves spaces, Unicode and case; rejects empty names and control characters.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(try_from = "String", into = "String")]
pub struct CommonName(String);

impl CommonName {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl TryFrom<String> for CommonName {
    type Error = String;

    fn try_from(name: String) -> Result<Self, Self::Error> {
        if name.trim().is_empty() || name.chars().any(char::is_control) {
            return Err("common name must be non-empty and contain no control characters".into());
        }
        Ok(Self(name))
    }
}

impl FromStr for CommonName {
    type Err = String;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Self::try_from(name.to_owned())
    }
}

impl From<CommonName> for String {
    fn from(name: CommonName) -> Self {
        name.into_string()
    }
}

impl fmt::Display for CommonName {
    fn fmt(
        &self,
        formatter: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[cfg(test)]
#[path = "../../tests/ssl/common_name.rs"]
mod tests;
