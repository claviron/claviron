use super::*;

#[path = "extensions/template.rs"]
mod template;

#[test]
fn sni_authority_guard_preserves_ipv6_and_rejects_different_hosts() {
    let table: VhostTable =
        serde_yaml::from_str("'[::1]': {root: public}").unwrap();
    let vctx = resolve_sni_from_table(&table, Some("[::1]")).unwrap();
    for authority in ["[::1]", "[::1]:443"] {
        let request =
            Request::builder().header("host", authority).body(()).unwrap();
        let authority = RequestAuthority::from_request(&request).unwrap();
        assert!(authority_matches_sni(authority.as_ref(), &vctx));
    }
    for authority in ["[::2]:443", "app.test"] {
        let request =
            Request::builder().header("host", authority).body(()).unwrap();
        let authority = RequestAuthority::from_request(&request).unwrap();
        assert!(!authority_matches_sni(authority.as_ref(), &vctx));
    }
}

#[test]
fn listener_identity_survives_per_request_proxy_resolution() {
    let table: VhostTable =
        serde_yaml::from_str("app.test: {root: public, listeners: internal}")
            .unwrap();
    let vctx = resolve_sni_from_table(&table, Some("app.test")).unwrap();
    let ctx = Arc::new(ConnCtx {
        state: Default::default(),
        listener: "internal".parse().unwrap(),
        proto: Some(claviron_tls::Proto::H2),
        remote: "10.0.0.1:1234".parse().unwrap(),
        local_ip: "127.0.0.1".parse().unwrap(),
        local_port: 443,
        real_ip: None,
        country_code: None,
        tls: None,
    });
    let proxy = Some(ProxyConfig {
        trusted: vec!["10.0.0.0/8".parse().unwrap()],
        headers: vec![http::HeaderName::from_static("x-real-ip")],
    });
    let template = ContextExtensions::new(&ctx, &vctx);
    for ip in ["203.0.113.1", "203.0.113.2"] {
        let mut request = Request::builder()
            .uri("https://app.test/")
            .header("x-real-ip", ip)
            .body(())
            .unwrap();
        template.inject(&mut request, &ctx, &vctx, proxy.as_ref());
        assert_eq!(
            request.extensions().get::<http::uri::Scheme>(),
            Some(&http::uri::Scheme::HTTP)
        );
        let resolved = request.extensions().get::<Arc<ConnCtx>>().unwrap();
        assert_eq!(resolved.listener, ctx.listener);
        assert_eq!(resolved.real_ip, Some(ip.parse().unwrap()));
        assert_eq!(
            request.extensions().get::<claviron_http::ListenerId>(),
            Some(&ctx.listener)
        );
    }
    assert!(
        ctx.real_ip.is_none(),
        "connection context must not retain a previous request's identity"
    );
    let mut direct =
        Request::builder().header("x-real-ip", "203.0.113.1").body(()).unwrap();
    template.inject(&mut direct, &ctx, &vctx, None);
    assert_eq!(
        direct.extensions().get::<Arc<ConnCtx>>().unwrap().real_ip,
        Some(ctx.remote.ip())
    );
}

#[test]
fn captured_host_table_and_authority_guard_survive_a_later_table() {
    let old: VhostTable =
        serde_yaml::from_str("app.test: {root: old, listeners: public}")
            .unwrap();
    let new: VhostTable =
        serde_yaml::from_str("app.test: {root: new, listeners: private}")
            .unwrap();
    let accepted = resolve_sni_from_table(&old, Some("app.test")).unwrap();
    assert!(accepted.vhost_config.allows_listener(&"public".parse().unwrap()));
    assert!(
        !resolve_sni_from_table(&new, Some("app.test"))
            .unwrap()
            .vhost_config
            .allows_listener(&"public".parse().unwrap())
    );
    for (host, expected) in [("app.test", true), ("other.test", false)] {
        let request = Request::builder().header("host", host).body(()).unwrap();
        let authority = RequestAuthority::from_request(&request).unwrap();
        assert_eq!(
            authority_matches_sni(authority.as_ref(), &accepted),
            expected
        );
    }
}

#[test]
fn a_direct_connection_shares_its_context_instead_of_copying_it() {
    let table: VhostTable =
        serde_yaml::from_str("app.test: {root: public}").unwrap();
    let vctx = resolve_sni_from_table(&table, Some("app.test")).unwrap();
    let ctx = Arc::new(ConnCtx {
        state: Default::default(),
        listener: "public".parse().unwrap(),
        proto: Some(claviron_tls::Proto::H2),
        remote: "198.51.100.7:4321".parse().unwrap(),
        local_ip: "127.0.0.1".parse().unwrap(),
        local_port: 443,
        real_ip: Some("198.51.100.7".parse().unwrap()),
        country_code: None,
        tls: None,
    });
    let template = ContextExtensions::new(&ctx, &vctx);
    for _ in 0..2 {
        // A forwarding header from a direct client is not trusted.
        let mut request = Request::builder()
            .uri("https://app.test/")
            .header("x-real-ip", "203.0.113.1")
            .body(())
            .unwrap();
        let shared = template.inject(&mut request, &ctx, &vctx, None);
        assert!(Arc::ptr_eq(&shared, &ctx), "no per-request copy");
        assert_eq!(
            request.extensions().get::<RealIpExt>().map(|ext| ext.0),
            ctx.real_ip
        );
    }
}
