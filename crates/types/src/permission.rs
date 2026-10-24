//! Exact permission names and an application-owned, in-memory definition catalog.
//!
//! Names are case-sensitive ASCII identifiers, not grants or database keys.
//! Registration establishes vocabulary; the caller still authorizes delegation.
use std::{
    borrow::Borrow, collections::BTreeSet, fmt, str::FromStr, sync::Arc,
};

/// A nonempty `[A-Za-z0-9][A-Za-z0-9._-]*` name. No wildcard semantics.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Permission(Arc<str>);

impl Permission {
    pub fn new(name: impl AsRef<str>) -> Result<Self, PermissionError> {
        let name = name.as_ref();
        if !valid_name(name) {
            return Err(PermissionError::InvalidName);
        }
        Ok(Self(Arc::from(name)))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn defined<T: PermissionDefinition>() -> Result<Self, PermissionError> {
        Self::new(T::NAME)
    }
}

impl<T: PermissionDefinition> From<T> for Permission {
    fn from(_: T) -> Self {
        const {
            assert!(valid_name(T::NAME), "invalid PermissionDefinition::NAME");
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

impl FromStr for Permission {
    type Err = PermissionError;
    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Self::new(name)
    }
}

impl AsRef<str> for Permission {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for Permission {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for Permission {
    fn fmt(
        &self,
        formatter: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// A searchable code definition owned by the subsystem that uses this permission.
/// Implementing the trait does not register a definition automatically.
///
/// A definition value converts directly into [`Permission`] through `From` or
/// `Into`. The conversion validates `NAME` at compile time. Equality, ordering
/// and hashing use the name, including when comparing with parsed permissions.
///
/// ```
/// use claviron_types::permission::{Permission, PermissionDefinition};
///
/// struct ServiceRead;
/// impl PermissionDefinition for ServiceRead {
///     const NAME: &'static str = "service.read";
/// }
///
/// let permission: Permission = ServiceRead.into();
/// assert_eq!(permission, "service.read".parse::<Permission>().unwrap());
/// ```
pub trait PermissionDefinition {
    const NAME: &'static str;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PermissionError {
    InvalidName,
    Unknown(Permission),
}

impl fmt::Display for PermissionError {
    fn fmt(
        &self,
        formatter: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match self {
            Self::InvalidName => formatter.write_str("permission names must start with an ASCII letter or digit and contain only ASCII letters, digits, '.', '_' or '-'"),
            Self::Unknown(name) => write!(formatter, "permission '{name}' is not registered"),
        }
    }
}
impl std::error::Error for PermissionError {}

/// Build during composition, then share immutably. No global registry or I/O.
#[derive(Clone, Debug, Default)]
pub struct PermissionCatalog {
    names: BTreeSet<Permission>,
}

impl PermissionCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Repeated registration of the same exact name is idempotent.
    pub fn register<T: PermissionDefinition>(
        &mut self
    ) -> Result<&mut Self, PermissionError> {
        self.register_name(T::NAME)
    }

    /// Dynamic sources may register names without defining a Rust marker type.
    pub fn register_name(
        &mut self,
        name: impl AsRef<str>,
    ) -> Result<&mut Self, PermissionError> {
        self.names.insert(Permission::new(name)?);
        Ok(self)
    }

    pub fn definitions(&self) -> impl ExactSizeIterator<Item = &Permission> {
        self.names.iter()
    }

    /// Validate the complete selection before any credential is created.
    /// Results are deduplicated and sorted; an empty selection grants nothing.
    pub fn validate<I, S>(
        &self,
        names: I,
    ) -> Result<Permissions, PermissionError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut selected = BTreeSet::new();
        for name in names {
            let parsed = Permission::new(name)?;
            let registered = self
                .names
                .get(&parsed)
                .ok_or_else(|| PermissionError::Unknown(parsed.clone()))?;
            selected.insert(registered.clone());
        }
        Ok(Permissions(selected))
    }
}

/// Credential-declared permission names, not an authenticated authorization result.
/// Membership is exact; punctuation implies no inheritance.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Permissions(pub(crate) BTreeSet<Permission>);

impl Permissions {
    pub fn contains<T: PermissionDefinition>(&self) -> bool {
        self.0.contains(T::NAME)
    }

    pub fn contains_name(
        &self,
        name: &Permission,
    ) -> bool {
        self.0.contains(name)
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = &Permission> {
        self.0.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[cfg(test)]
#[path = "../tests/permission.rs"]
mod tests;
