//! One certificate for a listener with one owner.
//!
//! A certificate and its key are all that is required; supplying them is what
//! turns a TCP listener into an HTTPS one. There is no SNI here and no
//! certificate store.
//!
//! # Mutual TLS
//!
//! On a hop between a proxy and the application behind it, encryption alone
//! is usually ceremony — the link is a loopback or a private network, and a
//! Unix socket is already bounded by filesystem permission. What earns its
//! place is the other half: requiring the client to prove who it is, so only
//! your edge can connect at all. Give it a [`ClientAuth`] — the same
//! declaration a virtual host makes — and every connection must present a
//! certificate that declaration verifies. A CA bundle is read here; a store
//! authority is resolved by the listener's owner, which holds the certificate
//! store, and handed in with [`Tls::authority`].
//!
//! # Renewal
//!
//! The certificate is read once, at boot. Renewing it means restarting the
//! process — which is what a certificate renewal already does to most
//! upstreams, and is stated here rather than discovered.

use std::path::PathBuf;
use std::sync::Arc;

use claviron_tls::auth::{ClientAuth, client_verifier};
use claviron_tls::pem::{load_der_certs, load_private_key};
use claviron_tls::{CertAuthority, TlsError};
use rustls::ServerConfig;

/// The certificate a listener serves, and optionally the client certificate
/// it requires.
#[derive(Clone, Debug)]
pub struct Tls {
    pub certificate: PathBuf,
    pub key: PathBuf,
    /// The client certificate every connection must present. Without it,
    /// clients are not asked for one.
    pub client_auth: Option<ClientAuth>,
    /// The store authority `client_auth` selects, resolved at boot by the
    /// owner of the certificate store with `CertificateStore::trust`. A CA
    /// bundle takes none; a store authority without it is refused when the
    /// configuration is built.
    pub authority: Option<Arc<CertAuthority>>,
}

impl Tls {
    pub fn new(
        certificate: impl Into<PathBuf>,
        key: impl Into<PathBuf>,
    ) -> Self {
        Self {
            certificate: certificate.into(),
            key: key.into(),
            client_auth: None,
            authority: None,
        }
    }

    /// Require the client certificate `auth` declares.
    pub fn client_auth(
        mut self,
        auth: ClientAuth,
    ) -> Self {
        self.client_auth = Some(auth);
        self
    }

    /// The store authority `client_auth` selects, already resolved.
    pub fn authority(
        mut self,
        authority: Arc<CertAuthority>,
    ) -> Self {
        self.authority = Some(authority);
        self
    }

    /// Build the configuration, reading every file named here. `alpn` is what
    /// the listener serves, in preference order — `h2` only if it speaks
    /// HTTP/2.
    pub(crate) fn config(
        &self,
        alpn: &[&[u8]],
    ) -> Result<Arc<ServerConfig>, Error> {
        let chain = load_der_certs(&self.certificate)?;
        if chain.is_empty() {
            return Err(Error::NoCertificate(self.certificate.clone()));
        }
        let key = load_private_key(&self.key)?;

        let provider = claviron_tls::policy::provider();
        let verifier = self
            .client_auth
            .as_ref()
            .map(|auth| {
                client_verifier(auth, self.authority.as_deref(), &provider)
            })
            .transpose()?;
        let builder = ServerConfig::builder_with_provider(Arc::new(provider))
            .with_safe_default_protocol_versions()?;
        let builder = match verifier {
            None => builder.with_no_client_auth(),
            Some(verifier) => builder.with_client_cert_verifier(verifier),
        };
        let mut config =
            builder.with_single_cert(chain, key).map_err(Error::KeyMismatch)?;
        config.alpn_protocols =
            alpn.iter().map(|protocol| protocol.to_vec()).collect();
        Ok(Arc::new(config))
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

#[cfg(test)]
#[path = "../../tests/tcp/tls.rs"]
mod tests;
