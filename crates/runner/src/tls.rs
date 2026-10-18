//! Serving TLS on a runner's TCP listener.
//!
//! A certificate and its key are all that is required; supplying them is what
//! turns a TCP listener into an HTTPS one. There is no SNI here and no
//! certificate store: a runner serves one application, so it serves one
//! certificate.
//!
//! # Mutual TLS
//!
//! On a hop between a proxy and the application behind it, encryption alone
//! is usually ceremony — the link is a loopback or a private network, and a
//! Unix socket is already bounded by filesystem permission. What earns its
//! place is the other half: requiring the client to prove who it is, so only
//! your edge can connect at all. Give a CA and every connection must present a
//! certificate it signed.
//!
//! # Renewal
//!
//! The certificate is read once, at boot. Renewing it means restarting the
//! process — which is what a certificate renewal already does to most
//! upstreams, and is stated here rather than discovered.

use std::path::PathBuf;
use std::sync::Arc;

use claviron_http::ext::{ClientAuthStatus, TlsInfo};
use claviron_tls::TlsError;
use claviron_tls::auth::create_client_cert_verifier;
use claviron_tls::pem::{load_der_certs, load_private_key};
use rustls::ServerConfig;
use rustls::server::ServerConnection;
pub(crate) use tokio_rustls::TlsAcceptor;

/// The certificate a listener serves, and optionally the CA a client must be
/// signed by.
#[derive(Clone, Debug)]
pub struct Tls {
    pub certificate: PathBuf,
    pub key: PathBuf,
    /// Require a client certificate signed by this CA. Without it, clients
    /// are not asked for one.
    pub client_ca: Option<PathBuf>,
}

impl Tls {
    pub fn new(
        certificate: impl Into<PathBuf>,
        key: impl Into<PathBuf>,
    ) -> Self {
        Self {
            certificate: certificate.into(),
            key: key.into(),
            client_ca: None,
        }
    }

    /// Require a client certificate signed by this authority.
    pub fn client_ca(
        mut self,
        path: impl Into<PathBuf>,
    ) -> Self {
        self.client_ca = Some(path.into());
        self
    }

    /// Build the acceptor, reading every file named here. `alpn` is what the
    /// listener serves, in preference order — `h2` only if it speaks HTTP/2.
    pub(crate) fn acceptor(
        &self,
        alpn: &[&[u8]],
    ) -> Result<TlsAcceptor, Error> {
        let chain = load_der_certs(&self.certificate)?;
        if chain.is_empty() {
            return Err(Error::NoCertificate(self.certificate.clone()));
        }
        let key = load_private_key(&self.key)?;

        let builder = ServerConfig::builder_with_provider(Arc::new(
            claviron_tls::policy::provider(),
        ))
        .with_safe_default_protocol_versions()?;
        let builder = match &self.client_ca {
            None => builder.with_no_client_auth(),
            Some(path) => builder.with_client_cert_verifier(
                create_client_cert_verifier(path, None)?,
            ),
        };
        let mut config =
            builder.with_single_cert(chain, key).map_err(Error::KeyMismatch)?;
        config.alpn_protocols =
            alpn.iter().map(|protocol| protocol.to_vec()).collect();
        Ok(TlsAcceptor::from(Arc::new(config)))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Tls(#[from] TlsError),
    #[error("TLS protocol versions: {0}")]
    Versions(#[from] rustls::Error),
    #[error("{}: no certificate in this file", .0.display())]
    NoCertificate(PathBuf),
    #[error("certificate and key do not match: {0}")]
    KeyMismatch(rustls::Error),
}

/// What a handshake established, for the request's context.
pub(crate) fn connection_info(connection: &ServerConnection) -> TlsInfo {
    let verified =
        connection.peer_certificates().is_some_and(|certs| !certs.is_empty());
    TlsInfo {
        protocol_version: connection.protocol_version(),
        cipher_suite: connection
            .negotiated_cipher_suite()
            .map(|suite| suite.suite()),
        client_auth: if verified {
            ClientAuthStatus::Verified
        } else {
            ClientAuthStatus::NotProvided
        },
        client_cert: None,
    }
}

#[cfg(test)]
#[path = "../tests/tls.rs"]
mod tests;
