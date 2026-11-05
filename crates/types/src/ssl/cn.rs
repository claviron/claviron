use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use x509_parser::x509::X509Name;

use super::Error;
use crate::{Host, KeyString};

/// X.509 subject common name, not a DNS hostname or an SNI routing key.
/// Preserves spaces, Unicode and case; rejects empty names and control characters.
///
/// The text is exact-size and immutable. It moves into a certificate under
/// construction without a copy (`String::from`), and is read from a parsed
/// certificate with [`CommonName::read`] without one either.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(try_from = "String", into = "String")]
pub struct CommonName(Box<str>);

impl CommonName {
    /// The rule every common name passes, stated once for owned and borrowed
    /// text alike.
    fn check(name: &str) -> Result<(), String> {
        if name.trim().is_empty() || name.chars().any(char::is_control) {
            return Err("common name must be non-empty and contain no control characters".into());
        }
        Ok(())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0.into_string()
    }

    /// The first common name of a parsed X.509 name, borrowed from its DER.
    /// `None` when the name carries no common name.
    pub fn read<'a>(name: &'a X509Name<'a>) -> Result<Option<&'a str>, Error> {
        let Some(attribute) = name.iter_common_name().next() else {
            return Ok(None);
        };
        let text = attribute.as_str()?;
        Self::check(text).map_err(Error::Other)?;
        Ok(Some(text))
    }
}

impl TryFrom<String> for CommonName {
    type Error = String;

    fn try_from(name: String) -> Result<Self, Self::Error> {
        Self::check(&name)?;
        Ok(Self(name.into_boxed_str()))
    }
}

impl FromStr for CommonName {
    type Err = String;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Self::check(name)?;
        Ok(Self(name.into()))
    }
}

/// A parsed name must carry a common name to become one.
impl<'a> TryFrom<&'a X509Name<'a>> for CommonName {
    type Error = Error;

    fn try_from(name: &'a X509Name<'a>) -> Result<Self, Self::Error> {
        Self::read(name)?
            .map(|text| Self(text.into()))
            .ok_or_else(|| Error::Other("missing subject CN".into()))
    }
}

/// A host name is always a valid common name; the reverse does not hold.
impl From<&Host> for CommonName {
    fn from(host: &Host) -> Self {
        Self(host.key_str().into())
    }
}

impl From<CommonName> for String {
    fn from(name: CommonName) -> Self {
        name.into_string()
    }
}

impl From<&CommonName> for String {
    fn from(name: &CommonName) -> Self {
        name.as_str().to_owned()
    }
}

impl AsRef<str> for CommonName {
    fn as_ref(&self) -> &str {
        self.as_str()
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
#[path = "../../tests/ssl/cn.rs"]
mod tests;
