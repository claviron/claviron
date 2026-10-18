use std::borrow::Cow;
use std::str::FromStr;
use std::sync::Arc;

use serde::{Deserialize, Deserializer, de};

use crate::KeyString;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Sni(pub Arc<str>);

impl Sni {
    #[inline]
    pub fn norm(s: &str) -> String {
        s.to_ascii_lowercase().trim_end_matches('.').to_string()
    }

    pub fn validate(s: &str) -> bool {
        // Allow optional leading wildcard: "*.example.com"
        let s = if let Some(rest) = s.strip_prefix("*.") { rest } else { s };

        // Must be FQDN-like and reasonable length
        if !s.contains('.') || s.len() > 253 {
            return false;
        }
        // Require at least one letter to avoid purely numeric FQDNs
        if !s.bytes().any(|b| b.is_ascii_lowercase()) {
            return false;
        }

        s.split('.').all(|lab| {
            let n = lab.len();
            (1..=63).contains(&n)
                && lab.as_bytes().iter().all(|&b| {
                    b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'
                })
                && !lab.starts_with('-')
                && !lab.ends_with('-')
        })
    }

    /// Borrow as `&str` without allocation
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn key_string(&self) -> String {
        self.0.to_string()
    }
}

impl KeyString for Sni {
    #[inline]
    fn key_str(&self) -> Cow<'_, str> {
        Cow::Borrowed(self.as_str())
    }
}

impl std::fmt::Display for Sni {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Sni {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let normed = Sni::norm(s);
        if Sni::validate(&normed) {
            Ok(Sni(Arc::<str>::from(normed)))
        } else {
            Err(format!("invalid SNI: {s}"))
        }
    }
}

impl<'de> Deserialize<'de> for Sni {
    fn deserialize<D>(de: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(de)?;
        let s = Sni::norm(&raw);
        if Sni::validate(&s) {
            Ok(Sni(Arc::<str>::from(s)))
        } else {
            Err(de::Error::custom(format!("invalid SNI: {raw}")))
        }
    }
}

impl AsRef<str> for Sni {
    #[inline]
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::borrow::Borrow<str> for Sni {
    #[inline]
    fn borrow(&self) -> &str {
        self.as_str()
    }
}
