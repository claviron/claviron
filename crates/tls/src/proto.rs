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

/// The HTTP version a session speaks once its ALPN protocol id is
/// negotiated; the id itself names none.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionWire {
    Http1,
    Http2,
    /// Either, decided by the connection preface: a client sending one
    /// request can speak HTTP/1.1 and a long-lived one HTTP/2.
    Either,
}

impl SessionWire {
    /// The HTTP versions the session's handshake is created for.
    pub fn protocols(self) -> &'static [Proto] {
        match self {
            Self::Http1 => &[Proto::H1],
            Self::Http2 => &[Proto::H2],
            Self::Either => &[Proto::H1, Proto::H2],
        }
    }

    /// The one version spoken, or `None` when the preface decides.
    pub fn fixed(self) -> Option<Proto> {
        match self {
            Self::Http1 => Some(Proto::H1),
            Self::Http2 => Some(Proto::H2),
            Self::Either => None,
        }
    }
}
