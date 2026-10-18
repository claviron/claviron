use serde::Deserialize;

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Proto {
    H3,
    H2,
    H1,
}

impl Proto {
    /// Convenience: final + one common draft ALPNs for HTTP/3
    pub fn alpn_versions(&self) -> &'static [&'static [u8]] {
        use self::Proto::*;
        match self {
            H3 => &[b"h3", b"h3-29"],
            H2 => &[b"h2"],
            H1 => &[b"http/1.1"],
        }
    }

    pub fn version(&self) -> &'static str {
        use self::Proto::*;
        match self {
            H1 => "HTTP/1.1",
            H2 => "HTTP/2.0",
            H3 => "HTTP/3.0",
        }
    }
}
