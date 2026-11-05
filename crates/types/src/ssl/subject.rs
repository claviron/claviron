//! Application-owned URI identities carried in a certificate's SAN extension.
use std::{fmt, str::FromStr};

use super::SSLError;

/// An absolute ASCII URI. Namespace meaning and access policy belong to callers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubjectUri(Box<str>);

impl SubjectUri {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for SubjectUri {
    type Err = SSLError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let invalid = || {
            SSLError::Other(
                "subject identity must be an absolute ASCII URI".into(),
            )
        };
        let (scheme, rest) = value.split_once(':').ok_or_else(invalid)?;
        if scheme.is_empty()
            || !scheme.as_bytes()[0].is_ascii_alphabetic()
            || !scheme
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"+.-".contains(&c))
            || rest.is_empty()
            || !value.bytes().all(|c| {
                c.is_ascii_alphanumeric()
                    || b"-._~:/?#[]@!$&'()*+,;=%".contains(&c)
            })
        {
            return Err(invalid());
        }
        let bytes = value.as_bytes();
        for (index, byte) in bytes.iter().enumerate() {
            if *byte == b'%'
                && (index + 2 >= bytes.len()
                    || !bytes[index + 1].is_ascii_hexdigit()
                    || !bytes[index + 2].is_ascii_hexdigit())
            {
                return Err(invalid());
            }
        }
        Ok(Self(value.into()))
    }
}

/// The URI as a subject alternative name of a certificate under construction.
impl TryFrom<&SubjectUri> for rcgen::SanType {
    type Error = SSLError;

    fn try_from(uri: &SubjectUri) -> Result<Self, Self::Error> {
        uri.as_str()
            .try_into()
            .map(rcgen::SanType::URI)
            .map_err(|_| SSLError::Other("subject URI must be ASCII".into()))
    }
}

impl fmt::Display for SubjectUri {
    fn fmt(
        &self,
        formatter: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}
