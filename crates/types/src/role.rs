//! Exact role names shared by application definitions and stored values.

use std::{borrow::Borrow, fmt, str::FromStr, sync::Arc};

/// A nonempty `[A-Za-z0-9][A-Za-z0-9._-]*` name. No wildcard semantics.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Role(Arc<str>);

impl Role {
    pub fn new(name: impl AsRef<str>) -> Result<Self, RoleError> {
        let name = name.as_ref();
        if !valid_name(name) {
            return Err(RoleError::InvalidName);
        }
        Ok(Self(Arc::from(name)))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn defined<T: RoleDefinition>() -> Result<Self, RoleError> {
        Self::new(T::NAME)
    }
}

impl<T: RoleDefinition> From<T> for Role {
    fn from(_: T) -> Self {
        const {
            assert!(valid_name(T::NAME), "invalid RoleDefinition::NAME");
        }
        Self(Arc::from(T::NAME))
    }
}

const fn valid_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    if bytes.is_empty() || !bytes[0].is_ascii_alphanumeric() {
        return false;
    }
    let mut index = 1;
    while index < bytes.len() {
        let byte = bytes[index];
        if !byte.is_ascii_alphanumeric()
            && byte != b'.'
            && byte != b'_'
            && byte != b'-'
        {
            return false;
        }
        index += 1;
    }
    true
}

impl FromStr for Role {
    type Err = RoleError;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Self::new(name)
    }
}

impl AsRef<str> for Role {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for Role {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for Role {
    fn fmt(
        &self,
        formatter: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// An application-owned role definition with a stable, case-sensitive name.
///
/// Definition values convert into [`Role`] through `From` or `Into`, validating
/// `NAME` at compile time. Equality, ordering and hashing use the exact name,
/// including when comparing with roles parsed from storage. A definition does
/// not assign a role to a user or establish an authenticated grant.
pub trait RoleDefinition {
    const NAME: &'static str;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RoleError {
    InvalidName,
}

impl fmt::Display for RoleError {
    fn fmt(
        &self,
        formatter: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match self {
            Self::InvalidName => formatter.write_str("role names must start with an ASCII letter or digit and contain only ASCII letters, digits, '.', '_' or '-'"),
        }
    }
}

impl std::error::Error for RoleError {}
