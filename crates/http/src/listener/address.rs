//! Listener addresses, independent of the application behind the socket.

use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum ListenAddress {
    Socket(SocketAddr),
    Unix(PathBuf),
}

impl ListenAddress {
    pub fn socket(&self) -> Option<SocketAddr> {
        match self {
            Self::Socket(address) => Some(*address),
            Self::Unix(_) => None,
        }
    }

    pub fn is_ephemeral(&self) -> bool {
        matches!(self, Self::Socket(address) if address.port() == 0)
    }
}

impl From<SocketAddr> for ListenAddress {
    fn from(address: SocketAddr) -> Self {
        Self::Socket(address)
    }
}

impl FromStr for ListenAddress {
    type Err = String;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if let Some(path) = value.strip_prefix("unix:") {
            if path.is_empty() {
                return Err("unix listener needs a path".into());
            }
            return Ok(Self::Unix(path.into()));
        }
        value
            .parse()
            .map(Self::Socket)
            .map_err(|error| format!("invalid listener address: {error}"))
    }
}

impl TryFrom<String> for ListenAddress {
    type Error = String;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl fmt::Display for ListenAddress {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match self {
            Self::Socket(address) => address.fmt(f),
            Self::Unix(path) => write!(f, "unix:{}", path.display()),
        }
    }
}

impl From<ListenAddress> for String {
    fn from(value: ListenAddress) -> Self {
        value.to_string()
    }
}
