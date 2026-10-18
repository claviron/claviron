//! Shared listener identity. Socket ownership belongs to transport crates.

mod snapshots;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

// Reuse the protocol type already consumed by TLS and connection context;
// do not introduce a second H1/H2/H3 enum that adapters must translate.
pub use claviron_tls::Proto as HttpProtocol;
use serde::{Deserialize, Serialize};
pub use snapshots::{ActiveListeners, ListenerSnapshot, ListenerSnapshots};

/// Listener driver family. Security mode is a separate TCP setting;
/// QUIC always requires TLS.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ListenerTransport {
    Tcp,
    Quic,
}

/// Stable, case-sensitive listener identity, independent of bind addresses.
///
/// Names begin with an ASCII letter or digit, followed by ASCII letters,
/// digits, dots, underscores or hyphens. No trimming or case folding occurs:
/// declaration keys and host selectors must name exactly the same listener.
#[derive(
    Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
)]
#[serde(try_from = "String", into = "String")]
pub struct ListenerId(Arc<str>);

impl ListenerId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for ListenerId {
    type Err = InvalidListenerId;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut bytes = value.bytes();
        if !bytes.next().is_some_and(|b| b.is_ascii_alphanumeric())
            || !bytes.all(|b| {
                b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-')
            })
        {
            return Err(InvalidListenerId(value.to_owned()));
        }
        Ok(Self(Arc::from(value)))
    }
}

impl TryFrom<String> for ListenerId {
    type Error = InvalidListenerId;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<ListenerId> for String {
    fn from(value: ListenerId) -> Self {
        value.as_str().to_owned()
    }
}

impl AsRef<str> for ListenerId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for ListenerId {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvalidListenerId(String);

impl fmt::Display for InvalidListenerId {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        write!(
            f,
            "invalid listener id {:?}: expected an ASCII alphanumeric name with '.', '_' or '-' separators",
            self.0
        )
    }
}

impl std::error::Error for InvalidListenerId {}

#[cfg(test)]
#[path = "../tests/listener.rs"]
mod tests;
