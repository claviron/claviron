use std::fmt;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ApplicationRuntime(String);

impl ApplicationRuntime {
    pub fn new(
        value: impl Into<String>
    ) -> Result<Self, ApplicationIdentityError> {
        normalized(value, ApplicationIdentityError::EmptyRuntime).map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ApplicationRuntime {
    fn fmt(
        &self,
        formatter: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ApplicationName(String);

impl ApplicationName {
    pub fn new(
        value: impl Into<String>
    ) -> Result<Self, ApplicationIdentityError> {
        normalized(value, ApplicationIdentityError::EmptyName).map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ApplicationName {
    fn fmt(
        &self,
        formatter: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ApplicationId {
    runtime: ApplicationRuntime,
    name: ApplicationName,
}

impl ApplicationId {
    pub fn new(
        runtime: ApplicationRuntime,
        name: ApplicationName,
    ) -> Self {
        Self { runtime, name }
    }

    pub fn runtime(&self) -> &ApplicationRuntime {
        &self.runtime
    }

    pub fn name(&self) -> &ApplicationName {
        &self.name
    }
}

impl fmt::Display for ApplicationId {
    fn fmt(
        &self,
        formatter: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        write!(formatter, "{}:{}", self.runtime, self.name)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ApplicationIdentityError {
    #[error("application runtime cannot be empty")]
    EmptyRuntime,
    #[error("application name cannot be empty")]
    EmptyName,
}

fn normalized(
    value: impl Into<String>,
    empty: ApplicationIdentityError,
) -> Result<String, ApplicationIdentityError> {
    let value = value.into();
    let value = value.trim();
    if value.is_empty() { Err(empty) } else { Ok(value.to_owned()) }
}

#[cfg(test)]
#[path = "../tests/identity.rs"]
mod tests;
