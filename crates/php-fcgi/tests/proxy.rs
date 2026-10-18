use super::*;

#[test]
fn cgi_scheme_follows_connection_and_rejects_client_overrides() {
    use claviron_http::ext::ClientAuthStatus;
    use claviron_php::PhpConfig;
    use claviron_runtime::AppState;
    let state = AppState::standalone("fcgi-scheme-test", ".");
    let template: PhpConfig = serde_yaml::from_str("{}").unwrap();
    let runtime =
        PhpRuntime::from_config(&state, &template, PathBuf::from("."), &[]);
    for tls in [false, true] {
        let conn = ConnCtx {
            listener: "test".parse().unwrap(),
            proto: None,
            remote: "127.0.0.1:1234".parse().unwrap(),
            local_ip: "127.0.0.1".parse().unwrap(),
            local_port: 8080,
            real_ip: None,
            country_code: None,
            tls: tls.then_some(TlsInfo {
                protocol_version: None,
                cipher_suite: None,
                client_auth: ClientAuthStatus::NotProvided,
                client_cert: None,
            }),
        };
        let mut parts = Request::builder()
            .uri("/index.php?q=%2F")
            .header("x-forwarded-proto", "spoofed")
            .header("forwarded", "proto=spoofed")
            .body(())
            .unwrap()
            .into_parts()
            .0;
        parts.extensions.insert(RealIpExt(conn.remote.ip()));
        parts
            .extensions
            .insert(HostName(Arc::new("app.test".parse().unwrap())));
        parts.extensions.insert(Arc::new(conn));
        let ctx = RequestCtx::from_parts(
            &parts,
            &runtime,
            "/index.php",
            &runtime.dir.join("index.php"),
        )
        .unwrap();
        let params = build_params(&ctx, &runtime, 0);
        assert_eq!(
            params["REQUEST_SCHEME"],
            if tls { "https" } else { "http" }
        );
        assert_eq!(params["HTTP_X_FORWARDED_PROTO"], params["REQUEST_SCHEME"]);
        assert_eq!(params["HTTPS"], if tls { "on" } else { "off" });
        assert!(!params.contains_key("HTTP_FORWARDED"));
        assert_eq!(params["REQUEST_URI"], "/index.php?q=%2F");
        assert_eq!(params.contains_key("SSL_CLIENT_VERIFY"), tls);
    }
}
