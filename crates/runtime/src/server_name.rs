use std::{fmt, ops::Deref, str::FromStr, sync::Arc};

/// Server identity stored by hosting, distinct from the shorter fleet namespace.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct ServerName(Arc<str>);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InvalidServerName;

impl fmt::Display for InvalidServerName {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        f.write_str("server name must contain 1–20 characters without whitespace or control characters")
    }
}

impl std::error::Error for InvalidServerName {}

impl ServerName {
    /// Hosting identity contract; independent of platform SHM naming limits.
    pub const MAX_LEN: usize = 20;

    pub fn new(name: impl Into<Arc<str>>) -> Result<Self, InvalidServerName> {
        let name = name.into();
        if name.is_empty()
            || name.chars().count() > Self::MAX_LEN
            || name.chars().any(|c| c.is_whitespace() || c.is_control())
        {
            return Err(InvalidServerName);
        }
        Ok(Self(name))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for ServerName {
    type Err = InvalidServerName;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Self::new(name)
    }
}

impl From<&str> for ServerName {
    fn from(name: &str) -> Self {
        Self::new(name).expect("invalid server name")
    }
}

impl From<String> for ServerName {
    fn from(name: String) -> Self {
        Self::new(name).expect("invalid server name")
    }
}

impl From<&ServerName> for ServerName {
    fn from(name: &ServerName) -> Self {
        name.clone()
    }
}

impl From<ServerName> for Arc<str> {
    fn from(name: ServerName) -> Self {
        name.0
    }
}

impl AsRef<str> for ServerName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Deref for ServerName {
    type Target = str;

    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for ServerName {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
