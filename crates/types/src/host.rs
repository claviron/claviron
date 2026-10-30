use std::borrow::Cow;
use std::fmt::Display;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;

use serde::{Deserialize, Deserializer};

use crate::{KeyString, Sni};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Host {
    Dns(Sni),         // strict DNS per spec (with dots)
    Ip(IpAddr),       // v4 or v6
    Single(Arc<str>), // e.g., "bison", "localhost"
}

impl Host {
    /// Return a string view of this host. For DNS/Single we borrow; for IP we allocate.
    #[inline]
    pub fn to_cow_str(&self) -> Cow<'_, str> {
        self.key_str()
    }

    /// Borrow a `&str` only for textual hosts (DNS/Single). Returns `None` for IPs.
    #[inline]
    pub fn as_str_if_textual(&self) -> Option<&str> {
        match self {
            Host::Dns(s) => Some(s.as_str()),
            Host::Single(s) => Some(s),
            Host::Ip(_) => None,
        }
    }

    /// Return true if this host is an IP address.
    #[inline]
    pub fn is_ip(&self) -> bool {
        matches!(self, Host::Ip(_))
    }

    /// Return true if this host is a DNS/SNI entry.
    #[inline]
    pub fn is_sni(&self) -> bool {
        matches!(self, Host::Dns(_))
    }

    /// Return true if this host is a single (non-DNS, non-IP) entry.
    #[inline]
    pub fn is_single(&self) -> bool {
        matches!(self, Host::Single(_))
    }

    /// Return a reference to the SNI if this host is a DNS/SNI entry.
    #[inline]
    pub fn as_sni(&self) -> Option<&Sni> {
        match self {
            Host::Dns(sni) => Some(sni),
            _ => None,
        }
    }

    /// Return name if host is Single
    #[inline]
    pub fn as_single(&self) -> Option<&str> {
        match self {
            Host::Single(name) => Some(name),
            _ => None,
        }
    }
    /// Return the IP if this host is an IP address.
    #[inline]
    pub fn as_ip(&self) -> Option<IpAddr> {
        match self {
            Host::Ip(ip) => Some(*ip),
            _ => None,
        }
    }
}

// Allow collecting hostnames as Vec<String> from Host or &Host
impl From<Host> for String {
    fn from(h: Host) -> Self {
        h.key_string()
    }
}

impl From<&Host> for String {
    fn from(h: &Host) -> Self {
        h.key_string()
    }
}

impl KeyString for Host {
    #[inline]
    fn key_str(&self) -> Cow<'_, str> {
        match self {
            Host::Dns(sni) => Cow::Borrowed(sni.as_str()),
            Host::Single(name) => Cow::Borrowed(name.as_ref()),
            Host::Ip(ip) => Cow::Owned(ip.to_string()),
        }
    }
}

impl Display for Host {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "{}", self.key_string())
    }
}

impl FromStr for Host {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Fast path: try raw IP first (allow bracketed IPv6 and trim controls)
        let raw = s.trim_matches(|c: char| {
            c.is_ascii_whitespace() || c.is_ascii_control()
        });

        // IP parsing uses the original spelling: stripping DNS root dots here
        // would admit IP forms that this parser previously rejected.
        let inner = if let Some(i) =
            raw.strip_prefix('[').and_then(|t| t.strip_suffix(']'))
        {
            i
        } else {
            raw
        };
        if !inner.is_empty()
            && let Ok(ip) = inner.parse::<IpAddr>()
        {
            return Ok(Host::Ip(ip));
        }

        // Borrow names that need no case conversion; only uppercase input
        // needs a temporary normalization buffer.
        let lowered = if raw.bytes().any(|byte| byte.is_ascii_uppercase()) {
            Cow::Owned(raw.to_ascii_lowercase())
        } else {
            Cow::Borrowed(raw)
        };
        let s = lowered.trim_end_matches('.');
        if s.is_empty() {
            return Err("invalid host alias: <empty>".to_string());
        }

        if Sni::validate(s) {
            return Ok(Host::Dns(Sni(Arc::from(s))));
        }

        // Single Name.
        if !s.is_empty()
            && s.len() <= 63
            && s.as_bytes().iter().all(|&b| {
                b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'
            })
            && !s.starts_with('-')
            && !s.ends_with('-')
        {
            return Ok(Host::Single(Arc::from(s)));
        }

        Err(format!("invalid host alias: {s}"))
    }
}

impl TryFrom<&[u8]> for Host {
    type Error = String;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        // Fast path: X.509 IPAddress SAN is raw octets (4 = IPv4, 16 = IPv6)
        match bytes.len() {
            4 => {
                let ip = std::net::Ipv4Addr::new(
                    bytes[0], bytes[1], bytes[2], bytes[3],
                );
                return Ok(Host::Ip(IpAddr::V4(ip)));
            }
            16 => {
                let arr: [u8; 16] = bytes.try_into().expect("len pre-checked");
                let ip = std::net::Ipv6Addr::from(arr);
                return Ok(Host::Ip(IpAddr::V6(ip)));
            }
            _ => { /* fall through to textual parse */ }
        }

        // Fallback: treat as textual (DNS/Single) if valid UTF‑8
        let s = std::str::from_utf8(bytes)
            .map_err(|e| format!("invalid UTF-8: {e}"))?;
        Host::from_str(s)
    }
}

impl<'de> Deserialize<'de> for Host {
    fn deserialize<D>(de: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(de)?;
        Host::from_str(&raw).map_err(serde::de::Error::custom)
    }
}

impl serde::Serialize for Host {
    fn serialize<S>(
        &self,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.key_string())
    }
}

impl From<Sni> for Host {
    fn from(h: Sni) -> Self {
        Host::Dns(h)
    }
}

impl From<&Sni> for Host {
    fn from(h: &Sni) -> Self {
        Host::Dns(h.clone())
    }
}
