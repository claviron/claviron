use std::fmt;
use std::ops::Deref;

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer};

/// Path prefix used for nesting routers (e.g., "/apps").
/// Invariants:
/// - must start with '/'
/// - no whitespace
/// - normalized to not end with '/' (except "/")
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PathPrefix(String);

impl PathPrefix {
    pub fn root() -> Self {
        Self("/".into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns true if the given absolute `path` is inside this prefix.
    /// Segment-safe: `/api` matches `/api` and `/api/...`, but not `/apix`.
    pub fn has_path_segment(
        &self,
        path: &str,
    ) -> bool {
        // We only consider absolute paths
        if !path.starts_with('/') {
            return false;
        }

        // Root prefix matches any absolute path
        if self.0 == "/" {
            return true;
        }

        // Must start with the prefix string
        if !path.starts_with(&self.0) {
            return false;
        }

        // Ensure a segment boundary (exact match or next char is '/')
        matches!(path.as_bytes().get(self.0.len()), Some(b'/') | None)
    }
}

impl Deref for PathPrefix {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'de> Deserialize<'de> for PathPrefix {
    fn deserialize<D>(de: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(de)?;
        let s = raw.trim(); // no leading/trailing spaces

        if s.is_empty() {
            return Err(D::Error::custom("path prefix cannot be empty"));
        }
        if !s.starts_with('/') {
            return Err(D::Error::custom("path prefix must start with '/'"));
        }
        if s.chars().any(char::is_whitespace) {
            return Err(D::Error::custom(
                "path prefix cannot contain whitespace",
            ));
        }

        // normalize: drop trailing '/' unless it's just "/"
        let trimmed = s.trim_end_matches('/');
        let norm = if trimmed.is_empty() { "/" } else { trimmed };

        Ok(PathPrefix(norm.to_string()))
    }
}

impl fmt::Display for PathPrefix {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        self.0.fmt(f)
    }
}

use serde::ser::{Serialize, Serializer};

impl Serialize for PathPrefix {
    fn serialize<S>(
        &self,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

#[cfg(test)]
#[path = "../../tests/ext/prefix.rs"]
mod tests;
