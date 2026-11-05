use std::any::{Any, TypeId};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use claviron_tls::Proto;
use rustls::{CipherSuite, ProtocolVersion};

#[derive(Clone, Copy)]
pub enum ClientAuthStatus {
    NotProvided,
    Verified,
}

/// TLS facts of an accepted connection, kept as the transport reported them.
/// Text is produced only where a consumer needs it, from static names.
#[derive(Clone)]
pub struct TlsInfo {
    pub protocol_version: Option<ProtocolVersion>,
    pub cipher_suite: Option<CipherSuite>,
    pub client_auth: ClientAuthStatus,
    /// Parsed leaf facts of the certificate the handshake verified. Absent
    /// when none was presented, or when its names could not be parsed; trust
    /// still comes from `client_auth`.
    pub client_cert: Option<Arc<claviron_types::PeerIdentity>>,
}

impl TlsInfo {
    /// The negotiated protocol under its usual server-variable name.
    pub fn ssl_protocol(&self) -> Option<&'static str> {
        match self.protocol_version? {
            ProtocolVersion::TLSv1_0 => Some("TLSv1"),
            ProtocolVersion::TLSv1_1 => Some("TLSv1.1"),
            ProtocolVersion::TLSv1_2 => Some("TLSv1.2"),
            ProtocolVersion::TLSv1_3 => Some("TLSv1.3"),
            _ => None,
        }
    }

    /// The negotiated suite under its registered name. TLS 1.3 suites carry
    /// no key exchange in theirs, so the library's `TLS13_` prefix is `TLS_`.
    pub fn ssl_cipher(&self) -> Option<&'static str> {
        match self.cipher_suite? {
            CipherSuite::TLS13_AES_128_GCM_SHA256 => {
                Some("TLS_AES_128_GCM_SHA256")
            }
            CipherSuite::TLS13_AES_256_GCM_SHA384 => {
                Some("TLS_AES_256_GCM_SHA384")
            }
            CipherSuite::TLS13_CHACHA20_POLY1305_SHA256 => {
                Some("TLS_CHACHA20_POLY1305_SHA256")
            }
            CipherSuite::TLS13_AES_128_CCM_SHA256 => {
                Some("TLS_AES_128_CCM_SHA256")
            }
            CipherSuite::TLS13_AES_128_CCM_8_SHA256 => {
                Some("TLS_AES_128_CCM_8_SHA256")
            }
            suite => suite.as_str(),
        }
    }

    pub fn ssl_client_verify(&self) -> &'static str {
        match self.client_auth {
            ClientAuthStatus::NotProvided => "NONE",
            ClientAuthStatus::Verified => "SUCCESS",
        }
    }
}

/// Values prepared while a connection was accepted and read by its requests.
///
/// Work whose inputs are complete at acceptance (the handshake's peer, the
/// selected host) is done once there and its result kept here, instead of
/// being repeated or looked up for every request. The values are written
/// before the first request and never afterwards, so reading takes no lock.
/// A connection nothing was prepared for holds nothing and allocates nothing.
///
/// Keys are Rust types, never client-supplied names. Cloning shares the same
/// values: a copy of the facts made for one request reads what the connection
/// holds.
#[derive(Clone, Default)]
pub struct ConnState(Option<Arc<[Value]>>);

struct Value {
    id: TypeId,
    value: Box<dyn Any + Send + Sync>,
}

impl ConnState {
    /// The value of this type, if one was prepared for the connection.
    pub fn get<T: Send + Sync + 'static>(&self) -> Option<&T> {
        let id = TypeId::of::<T>();
        self.0
            .as_deref()?
            .iter()
            .find(|entry| entry.id == id)
            .and_then(|entry| entry.value.downcast_ref())
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_none()
    }
}

/// Collects one connection's state while it is being accepted.
#[derive(Default)]
pub struct ConnStateBuilder(Vec<Value>);

impl ConnStateBuilder {
    /// Keep one value of this type for the connection. A second value of the
    /// same type replaces the first, which is returned.
    pub fn insert<T: Send + Sync + 'static>(
        &mut self,
        value: T,
    ) -> Option<T> {
        let id = TypeId::of::<T>();
        let value: Box<dyn Any + Send + Sync> = Box::new(value);
        match self.0.iter_mut().find(|entry| entry.id == id) {
            Some(entry) => {
                let previous = std::mem::replace(&mut entry.value, value);
                let previous: Box<dyn Any> = previous;
                previous.downcast().ok().map(|previous| *previous)
            }
            None => {
                self.0.push(Value { id, value });
                None
            }
        }
    }

    /// Freeze what was collected. Nothing collected allocates nothing.
    pub fn build(self) -> ConnState {
        ConnState((!self.0.is_empty()).then(|| self.0.into()))
    }
}

/// Connection facts and the values prepared at its acceptance.
/// Vhost/SNI resolution is handled separately and passed via VhostCtx
#[derive(Clone)]
pub struct ConnCtx {
    /// Prepared once while the connection was accepted; shared even when a
    /// request clones the facts to resolve forwarding.
    pub state: ConnState,
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

    /// Country code from GeoIP (for example "TR"), or the configured code
    /// for addresses it does not cover; `None` when GeoIP is not built in.
    /// Shared with the lookup cache rather than copied per connection.
    pub country_code: Option<Arc<str>>,

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
