//! TLS for the `tls` mode: one authority and one server identity for each
//! serving process, made in memory. Nothing is read from or written to disk.

use std::sync::{Arc, OnceLock};

use claviron::ssl::{CertificateKind, generators};
use rustls::pki_types::{
    CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName,
};
use rustls::sign::CertifiedKey;
use tokio_rustls::{TlsAcceptor, TlsConnector};

use super::Result;
use crate::support::json;

/// How long the identity of one benchmark process is valid, in days.
const VALIDITY_DAYS: u32 = 1;
const PROTOCOL: &[u8] = b"http/1.1";

/// What a serving process presents, and the authority a client must trust
/// to accept it.
pub(super) struct Identity {
    pub(super) trust: CertificateDer<'static>,
    certificate: CertificateDer<'static>,
    key: PrivatePkcs8KeyDer<'static>,
}

impl Identity {
    /// The identity of this process, generated on first use.
    pub(super) fn get() -> &'static Self {
        static IDENTITY: OnceLock<Identity> = OnceLock::new();
        IDENTITY.get_or_init(|| {
            Self::generate().expect("generate the benchmark TLS identity")
        })
    }

    fn generate() -> Result<Self> {
        claviron::tls::resolver::install_aws_crypto_provider();
        let authority = generators::generate_ca_key()?;
        let trust = generators::generate_ca_cert(
            &"Benchmark CA".parse()?,
            &authority,
            VALIDITY_DAYS,
            &Default::default(),
        )?;
        let key = generators::generate_private_key()?;
        let certificate = generators::generate_leaf_cert(
            CertificateKind::Server,
            &json::HOST.parse()?,
            &key,
            &trust,
            &authority,
            VALIDITY_DAYS,
            &["alias.benchmark.test"],
        )?;
        Ok(Self { trust, certificate, key: key.serialize_der().into() })
    }

    fn key(&self) -> PrivateKeyDer<'static> {
        PrivateKeyDer::Pkcs8(self.key.clone_key())
    }

    /// The form the Edge certificate resolver serves.
    pub(super) fn certified(&self) -> Result<Arc<CertifiedKey>> {
        Ok(Arc::new(CertifiedKey::from_der(
            vec![self.certificate.clone()],
            self.key(),
            &rustls::crypto::aws_lc_rs::default_provider(),
        )?))
    }

    /// The acceptor of a native control: the same provider, certificate and
    /// protocol as the Edge listener, with rustls defaults otherwise.
    pub(super) fn acceptor(&self) -> Result<TlsAcceptor> {
        let mut config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![self.certificate.clone()], self.key())?;
        config.alpn_protocols = vec![PROTOCOL.to_vec()];
        Ok(TlsAcceptor::from(Arc::new(config)))
    }
}

/// A client that trusts exactly the authority its server reported.
pub(super) struct Client {
    connector: TlsConnector,
    name: ServerName<'static>,
}

impl Client {
    pub(super) fn trusting(authority: &[u8]) -> Result<Self> {
        claviron::tls::resolver::install_aws_crypto_provider();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(CertificateDer::from(authority))?;
        let mut config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        config.alpn_protocols = vec![PROTOCOL.to_vec()];
        Ok(Self {
            connector: TlsConnector::from(Arc::new(config)),
            name: ServerName::try_from(json::HOST)?,
        })
    }

    pub(super) async fn connect(
        &self,
        stream: tokio::net::TcpStream,
    ) -> Result<tokio_rustls::client::TlsStream<tokio::net::TcpStream>> {
        Ok(self.connector.connect(self.name.clone(), stream).await?)
    }
}
