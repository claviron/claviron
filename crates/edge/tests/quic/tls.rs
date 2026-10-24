use std::time::Duration;

use quinn::Endpoint;
use rustls::pki_types::PrivatePkcs8KeyDer;

use super::*;

#[tokio::test]
async fn alias_publication_reaches_quic_and_captures_one_selector_view() {
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let cert = rcgen::generate_simple_self_signed(vec![
        "app.test".into(),
        "alias.test".into(),
    ])
    .unwrap();
    let key = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let mut config =
        RustlsServerConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(vec![cert.cert.der().clone()], key.into())
            .unwrap();
    config.alpn_protocols = h3_alpns();
    let base = Arc::new(config);
    let mut rejected = base.as_ref().clone();
    rejected.cert_resolver = Arc::new(RejectCertificates);
    rejected.session_storage =
        Arc::new(rustls::server::NoServerSessionStorage {});
    let live = Arc::new(VhostResolver::default());
    live.upsert_host(
        "app.test".parse().unwrap(),
        Arc::new(serde_yaml::from_str("root: public").unwrap()),
    );
    let policy = Arc::new(H3TlsPolicy {
        base: base.clone(),
        rejected: Arc::new(rejected),
        table: live.snapshot(),
        by_host: Arc::new(HashMap::from([("app.test".parse().unwrap(), base)])),
        config: QuicConfig::default(),
        revocations: Arc::new(Revocations::default()),
    });
    let acceptor = H3TlsAccept {
        listener: None,
        current: Arc::new(ArcSwap::from_pointee(
            policy.handshake(live.snapshot()).unwrap(),
        )),
        policy,
        live: live.clone(),
    };
    let server = Endpoint::server(
        acceptor.base_config(),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = h3_alpns();
    let mut client = Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    client.set_default_client_config(quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(config).unwrap(),
    )));
    let handshake = async |name: &'static str| {
        tokio::time::timeout(Duration::from_secs(5), async {
            let accepted =
                async { acceptor.accept(server.accept().await.unwrap()).await };
            let connecting =
                client.connect(server.local_addr().unwrap(), name).unwrap();
            let (accepted, connected) = tokio::join!(accepted, connecting);
            if let Ok(connected) = connected {
                connected.close(0_u32.into(), b"test complete");
            }
            if let Ok(accepted) = &accepted {
                accepted.connection.close(0_u32.into(), b"test complete");
            }
            accepted
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
    assert_eq!(
        accepted
            .hosts
            .resolve_sni(Some("alias.test"))
            .unwrap()
            .vhost_key
            .0
            .to_string(),
        "app.test"
    );
    let cache = acceptor.current.load_full();
    assert!(handshake("app.test").await.is_ok());
    assert!(
        Arc::ptr_eq(&cache, &acceptor.current.load_full()),
        "unchanged name view reuses QUIC selector"
    );
    live.delete_alias(&"alias.test".parse().unwrap());
    assert!(handshake("alias.test").await.is_err());
    assert!(
        accepted.hosts.resolve_sni(Some("alias.test")).is_ok(),
        "accepted view is immutable"
    );
    live.upsert_host(
        "app.test".parse().unwrap(),
        Arc::new(serde_yaml::from_str("root: changed").unwrap()),
    );
    assert!(
        handshake("app.test").await.is_err(),
        "stale authentication epoch must reject"
    );
    drop(accepted);
    client.close(0_u32.into(), b"done");
    server.close(0_u32.into(), b"done");
    tokio::time::timeout(Duration::from_secs(5), async {
        client.wait_idle().await;
        server.wait_idle().await;
    })
    .await
    .unwrap();
}
