use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::pki_types::{
    CertificateDer, PrivatePkcs8KeyDer, ServerName,
};
use tokio_rustls::rustls::{ClientConfig, RootCertStore};

use super::*;

struct Fixture {
    _dir: tempfile::TempDir,
    tls: Tls,
    server_cert: CertificateDer<'static>,
    client_cert: CertificateDer<'static>,
    client_key: Vec<u8>,
}
impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let server =
            rcgen::generate_simple_self_signed(vec!["app.test".into()])
                .unwrap();
        let mut ca = CertificateParams::new(Vec::<String>::new()).unwrap();
        ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca_key = KeyPair::generate().unwrap();
        let ca_cert = ca.self_signed(&ca_key).unwrap();
        let issuer = Issuer::new(ca, ca_key);
        let client_key = KeyPair::generate().unwrap();
        let client = CertificateParams::new(vec!["client.test".into()])
            .unwrap()
            .signed_by(&client_key, &issuer)
            .unwrap();
        let cert = dir.path().join("server.pem");
        let key = dir.path().join("server.key");
        let ca_path = dir.path().join("ca.pem");
        std::fs::write(&cert, server.cert.pem()).unwrap();
        std::fs::write(&key, server.signing_key.serialize_pem()).unwrap();
        std::fs::write(&ca_path, ca_cert.pem()).unwrap();
        Self {
            _dir: dir,
            tls: Tls::new(cert, key).client_ca(ca_path),
            server_cert: server.cert.der().clone(),
            client_cert: client.der().clone(),
            client_key: client_key.serialize_der(),
        }
    }

    fn client(
        &self,
        authenticated: bool,
        alpn: &[u8],
    ) -> TlsConnector {
        let mut roots = RootCertStore::empty();
        roots.add(self.server_cert.clone()).unwrap();
        let builder = ClientConfig::builder().with_root_certificates(roots);
        let mut config = if authenticated {
            builder
                .with_client_auth_cert(
                    vec![self.client_cert.clone()],
                    PrivatePkcs8KeyDer::from(self.client_key.clone()).into(),
                )
                .unwrap()
        } else {
            builder.with_no_client_auth()
        };
        config.alpn_protocols = vec![alpn.to_vec()];
        TlsConnector::from(Arc::new(config))
    }
}

#[tokio::test]
async fn tls_negotiates_http_and_mtls_requires_a_trusted_client() {
    let fixture = Fixture::new();
    for (mutual, authenticated, alpn) in [
        (false, false, b"http/1.1".as_slice()),
        (true, true, b"h2".as_slice()),
        (true, false, b"h2".as_slice()),
    ] {
        let mut tls = fixture.tls.clone();
        if !mutual {
            tls.client_ca = None;
        }
        let acceptor = tls.acceptor(&[b"h2", b"http/1.1"]).unwrap();
        let connector = fixture.client(authenticated, alpn);
        let (server, client) = tokio::io::duplex(65536);
        let (server, client) = tokio::join!(
            async {
                let mut stream = acceptor.accept(server).await?;
                assert_eq!(stream.get_ref().1.alpn_protocol(), Some(alpn));
                let info = connection_info(stream.get_ref().1);
                assert_eq!(
                    info.ssl_client_verify(),
                    if authenticated { "SUCCESS" } else { "NONE" }
                );
                assert!(info.protocol_version.is_some());
                let mut byte = [0];
                stream.read_exact(&mut byte).await?;
                stream.write_all(b"y").await?;
                Ok::<_, std::io::Error>(())
            },
            async {
                let mut stream = connector
                    .connect(ServerName::try_from("app.test").unwrap(), client)
                    .await?;
                stream.write_all(b"x").await?;
                stream.read_exact(&mut [0]).await.map(|_| ())
            },
        );
        if mutual && !authenticated {
            assert!(server.is_err());
        } else {
            server.unwrap();
            client.unwrap();
        }
    }
}
