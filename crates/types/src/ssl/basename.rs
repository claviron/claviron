use std::fmt;
use std::path::Path;
use std::str::FromStr;

use serde::{Deserialize, Serialize, Serializer};

/// The short name an operator gives certificate material kept in the file
/// store: one path component under `ca/`, `servers/` or `clients/`.
///
/// It is Claviron's own convention, not an X.509 name or a DNS host: one to
/// eight letters `a` to `z`. That alphabet cannot spell a separator, a parent
/// directory or a hidden file, so joining it to the store directory needs no
/// further check. The fixed size keeps it inline and `Copy`.
#[derive(Clone, Copy, Deserialize, PartialEq, Eq, Hash)]
#[serde(try_from = "String")]
pub struct Basename([u8; Self::MAX]);

impl Basename {
    /// Longest basename, in letters.
    pub const MAX: usize = 8;

    /// `None` when the text is not a basename. A configured name that is not
    /// one names something outside the file store, which is a lookup
    /// elsewhere rather than an error.
    pub fn new(name: &str) -> Option<Self> {
        let text = name.as_bytes();
        if text.is_empty()
            || text.len() > Self::MAX
            || !text.iter().all(u8::is_ascii_lowercase)
        {
            return None;
        }
        let mut letters = [0; Self::MAX];
        letters[..text.len()].copy_from_slice(text);
        Some(Self(letters))
    }

    pub fn as_str(&self) -> &str {
        let length =
            self.0.iter().position(|letter| *letter == 0).unwrap_or(Self::MAX);
        std::str::from_utf8(&self.0[..length])
            .expect("a basename holds only the letters a to z")
    }
}

/// Where a basename is required, text that is not one is an error.
impl TryFrom<&str> for Basename {
    type Error = String;

    fn try_from(name: &str) -> Result<Self, Self::Error> {
        Self::new(name).ok_or_else(|| {
            format!(
                "'{name}' is not a store basename: expected one to {} letters a to z",
                Self::MAX
            )
        })
    }
}

impl TryFrom<String> for Basename {
    type Error = String;

    fn try_from(name: String) -> Result<Self, Self::Error> {
        Self::try_from(name.as_str())
    }
}

impl FromStr for Basename {
    type Err = String;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Self::try_from(name)
    }
}

impl AsRef<str> for Basename {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<Path> for Basename {
    fn as_ref(&self) -> &Path {
        Path::new(self.as_str())
    }
}

impl Serialize for Basename {
    fn serialize<S: Serializer>(
        &self,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl fmt::Display for Basename {
    fn fmt(
        &self,
        formatter: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl fmt::Debug for Basename {
    fn fmt(
        &self,
        formatter: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        formatter.debug_tuple("Basename").field(&self.as_str()).finish()
    }
}

#[cfg(test)]
#[path = "../../tests/ssl/basename.rs"]
mod tests;
