use std::time::Duration;

use claviron_http::config::{HttpYaml, ListenerSettings};
use rustls::pki_types::{PrivatePkcs8KeyDer, ServerName};
use tokio::net::TcpListener;
use tokio_rustls::TlsConnector;

use super::*;

#[path = "../support/tls.rs"]
mod support;

#[path = "tls/rejections.rs"]
mod rejections;

#[path = "tls/framing.rs"]
mod framing;

#[path = "tls/authority.rs"]
mod authority;

use crate::framing_cases;

#[path = "tls/churn.rs"]
mod churn;

#[path = "tls/resilience.rs"]
mod resilience;

#[test]
fn host_alpn_preserves_explicit_preference_and_inherits_only_when_omitted() {
    let http = serde_yaml::from_str::<HttpYaml>("listeners: {tls: {transport: tcp, tls: true, protocols: [h2, h1], listen: '127.0.0.1:443', requests: {max_body_size: off, body_read_timeout: off}}}").unwrap().resolve().unwrap();
    let listener = http.listeners.values().next().unwrap();
    let ListenerSettings::Tcp(tcp) = &listener.settings else { unreachable!() };
    let mut host: claviron_edge::transport::HostConfig =
        serde_yaml::from_str("root: public").unwrap();
    assert_eq!(allowed_protocols(tcp, &host), [H2, H1]);
    host.transport.protocols = Some(vec![H1, H2]);
    assert_eq!(allowed_protocols(tcp, &host), [H1, H2]);
    let mut h1_first = tcp.clone();
    h1_first.protocols = vec![H1, H2];
    host.transport.protocols = Some(vec![H2, H1]);
    assert_eq!(allowed_protocols(&h1_first, &host), [H2, H1]);
    host.transport.protocols = None;
    assert_eq!(allowed_protocols(&h1_first, &host), [H1, H2]);
    host.transport.protocols = Some(vec![H1]);
    assert_eq!(allowed_protocols(tcp, &host), [H1]);
    host.transport.protocols = Some(vec![]);
    assert!(allowed_protocols(tcp, &host).is_empty());
    let mut h2_only = tcp.clone();
    h2_only.protocols = vec![H2];
    host.transport.protocols = Some(vec![H1]);
    assert!(
        allowed_protocols(&h2_only, &host).is_empty(),
        "host cannot add a protocol disabled on its listener"
    );
    host.transport.protocols = Some(vec![claviron_tls::Proto::H3, H1, H2]);
    assert_eq!(allowed_protocols(&h2_only, &host), [H2]);
}

#[tokio::test]
async fn host_alpn_preference_is_negotiated_per_sni() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let http = serde_yaml::from_str::<HttpYaml>(
            "listeners: {tls: {transport: tcp, tls: true, protocols: [h1, h2], listen: '127.0.0.1:0', requests: {max_body_size: off, body_read_timeout: off}}}"
        ).unwrap().resolve().unwrap();
        let ListenerSettings::Tcp(tcp) = &http.listeners.values().next().unwrap().settings else {
            unreachable!()
        };
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let cert = rcgen::generate_simple_self_signed(
            vec!["h2.test".into(), "h1.test".into(), "inherit.test".into()]
        ).unwrap();
        let live = Arc::new(VhostResolver::default());
        let mut by_host = HashMap::new();
        for (name, yaml) in [
            ("h2.test", "root: public\ntransport: {protocols: [h2, h1]}"),
            ("h1.test", "root: public\ntransport: {protocols: [h1, h2]}"),
            ("inherit.test", "root: public")
        ] {
            let host = Arc::new(serde_yaml::from_str(yaml).unwrap());
            let mut config = ServerConfig::builder_with_provider(provider.clone())
                .with_safe_default_protocol_versions().unwrap()
                .with_no_client_auth()
                .with_single_cert(
                    vec![cert.cert.der().clone()],
                    PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()).into()
                ).unwrap();
            config.alpn_protocols = allowed_protocols(tcp, &host).iter()
                .flat_map(|protocol| protocol.alpn_versions().iter().map(|alpn| alpn.to_vec()))
                .collect();
            live.upsert_host(name.parse().unwrap(), host);
            by_host.insert(name.parse().unwrap(), Arc::new(config));
        }
        let acceptor = TlsAccept(Arc::new(TlsConfigResolver {
            table: live.snapshot(), live, by_host, listener: None
        }));
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        let mut client = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions().unwrap()
            .with_root_certificates(roots).with_no_client_auth();
        client.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        let client = TlsConnector::from(Arc::new(client));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        for (name, expected) in [
            ("h2.test", b"h2".as_slice()),
            ("h1.test", b"http/1.1".as_slice()),
            ("inherit.test", b"http/1.1".as_slice())
        ] {
            let server = async {
                let (stream, _) = listener.accept().await.unwrap();
                acceptor.accept(stream).await.unwrap()
            };
            let client = async {
                let stream = TcpStream::connect(listener.local_addr().unwrap()).await.unwrap();
                client.connect(ServerName::try_from(name).unwrap(), stream).await.unwrap()
            };
            let (server, client) = tokio::join!(server, client);
            assert_eq!(server.stream.get_ref().1.alpn_protocol().map(AsRef::as_ref), Some(expected));
            assert_eq!(client.get_ref().1.alpn_protocol().map(AsRef::as_ref), Some(expected));
        }
    }).await.expect("bounded per-host ALPN handshakes");
}

#[tokio::test]
async fn alias_publication_reaches_tls_without_rebuilding_auth_config() {
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let cert = rcgen::generate_simple_self_signed(vec![
        "app.test".into(),
        "alias.test".into(),
    ])
    .unwrap();
    let key = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let config = Arc::new(
        ServerConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(vec![cert.cert.der().clone()], key.into())
            .unwrap(),
    );
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let client = TlsConnector::from(Arc::new(
        rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    ));
    let live = Arc::new(VhostResolver::default());
    live.upsert_host(
        "app.test".parse().unwrap(),
        Arc::new(serde_yaml::from_str("root: public").unwrap()),
    );
    let acceptor = TlsAccept(Arc::new(TlsConfigResolver {
        listener: None,
        table: live.snapshot(),
        live: live.clone(),
        by_host: HashMap::from([("app.test".parse().unwrap(), config)]),
    }));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let handshake = async |name: &'static str| {
        tokio::time::timeout(Duration::from_secs(5), async {
            let server = async {
                let (stream, _) = listener.accept().await.unwrap();
                acceptor.accept(stream).await
            };
            let client = async {
                let stream = TcpStream::connect(listener.local_addr().unwrap())
                    .await
                    .unwrap();
                client
                    .connect(ServerName::try_from(name).unwrap(), stream)
                    .await
            };
            let (server, client) = tokio::join!(server, client);
            drop(client);
            server
        })
        .await
        .unwrap()
    };
    assert!(handshake("alias.test").await.is_err());
    live.upsert_alias(
        "alias.test".parse().unwrap(),
        "app.test".parse().unwrap(),
    )
    .unwrap();
    let accepted = handshake("alias.test").await.unwrap();
    assert_eq!(accepted.host.vhost_key.0.to_string(), "app.test");
    live.delete_alias(&"alias.test".parse().unwrap());
    assert!(handshake("alias.test").await.is_err());
    assert_eq!(accepted.host.hostname.0.to_string(), "alias.test");
    live.upsert_host(
        "app.test".parse().unwrap(),
        Arc::new(serde_yaml::from_str("root: changed").unwrap()),
    );
    assert!(
        handshake("app.test").await.is_err(),
        "stale authentication epoch must reject"
    );
}
