use std::net::{IpAddr, SocketAddr};

use claviron_tls::Proto;
use rustls::{CipherSuite, ProtocolVersion};

#[derive(Clone)]
pub struct TlsClientCertInfo {
    pub subject_dn: String,
    pub issuer_dn: String,
}

#[derive(Clone, Copy)]
pub enum ClientAuthStatus {
    NotProvided,
    Verified,
}

#[derive(Clone)]
pub struct TlsInfo {
    pub protocol_version: Option<ProtocolVersion>,
    pub cipher_suite: Option<CipherSuite>,
    pub client_auth: ClientAuthStatus,
    pub client_cert: Option<TlsClientCertInfo>,
}

impl TlsInfo {
    pub fn ssl_protocol(&self) -> Option<String> {
        self.protocol_version.map(format_protocol_version)
    }

    pub fn ssl_cipher(&self) -> Option<String> {
        self.cipher_suite.map(format_cipher_suite)
    }

    pub fn ssl_client_verify(&self) -> &'static str {
        match self.client_auth {
            ClientAuthStatus::NotProvided => "NONE",
            ClientAuthStatus::Verified => "SUCCESS",
        }
    }
}

fn format_protocol_version(version: ProtocolVersion) -> String {
    match version {
        ProtocolVersion::TLSv1_0 => "TLSv1".to_string(),
        ProtocolVersion::TLSv1_1 => "TLSv1.1".to_string(),
        ProtocolVersion::TLSv1_2 => "TLSv1.2".to_string(),
        ProtocolVersion::TLSv1_3 => "TLSv1.3".to_string(),
        _ => format!("{version:?}"),
    }
}

fn format_cipher_suite(suite: CipherSuite) -> String {
    let name = format!("{suite:?}");
    name.strip_prefix("TLS13_")
        .map(|rest| format!("TLS_{rest}"))
        .unwrap_or(name)
}

/// Connection context - pure connection information only
/// Vhost/SNI resolution is handled separately and passed via VhostCtx
#[derive(Clone)]
pub struct ConnCtx {
    /// Named listener that accepted this connection; never inferred from an IP.
    pub listener: crate::ListenerId,
    /// Application protocol selected for this connection.
    pub proto: Option<Proto>,

    /// Remote client socket address (peer)
    pub remote: SocketAddr,
    /// Local server socket address (the interface/port that accepted the connection)
    pub local_ip: IpAddr,

    pub local_port: u16,

    /// Fixed ingress identity if known; otherwise resolved per request from
    /// the accepted peer and trusted forwarding headers. Never cache one
    /// proxied request's identity on a reusable connection.
    pub real_ip: Option<IpAddr>,

    /// Optional ISO country code from GeoIP (e.g., "TR", "US"); None if disabled or non-routable
    pub country_code: Option<String>,

    /// TLS details captured by the terminating transport, if any.
    pub tls: Option<TlsInfo>,
}

impl ConnCtx {
    /// Scheme of the accepted transport, not an untrusted request URI or
    /// forwarding header. TLS metadata remains the physical connection facts.
    pub fn scheme(&self) -> http::uri::Scheme {
        if self.tls.is_some() {
            http::uri::Scheme::HTTPS
        } else {
            http::uri::Scheme::HTTP
        }
    }

    pub fn remote_ip(&self) -> IpAddr {
        self.remote.ip()
    }

    pub fn remote_port(&self) -> u16 {
        self.remote.port()
    }

    pub fn local_port(&self) -> u16 {
        self.local_port
    }

    pub fn real_ip(&self) -> Option<&IpAddr> {
        self.real_ip.as_ref()
    }

    pub fn local_ip(&self) -> &IpAddr {
        &self.local_ip
    }

    pub fn country_code(&self) -> Option<&str> {
        self.country_code.as_deref()
    }

    pub fn tls(&self) -> Option<&TlsInfo> {
        self.tls.as_ref()
    }
}

#[cfg(test)]
#[path = "../../tests/ext/conn.rs"]
mod tests;
