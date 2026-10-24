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
            state: Default::default(),
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

#[tokio::test]
async fn file_routes_fcgi_preserves_public_names_and_pinned_entry_mapping() {
    use claviron_http::config::mapping::RouteMapping;
    use claviron_php::PhpConfig;
    use claviron_runtime::AppState;
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("app.js"), "script").unwrap();
    let state = AppState::standalone("fcgi-mapping", directory.path());
    let template: PhpConfig = serde_yaml::from_str("{}").unwrap();
    let mut runtime = PhpRuntime::from_config(
        &state,
        &template,
        directory.path().into(),
        &[],
    );
    runtime.mapping = Some(RouteMapping::new(
        serde_yaml::from_str("/admin").unwrap(),
        runtime.dir.clone(),
    ));
    assert!(try_files_candidate_exists(&runtime, "/admin/app.js").await);
    assert!(!try_files_candidate_exists(&runtime, "/other/app.js").await);
    for (path, entry, expected_name, expected_uri) in [
        (
            "/admin/index.php",
            None,
            "/admin/index.php",
            "/admin/index.php?q=%2F",
        ),
        ("/admin/", None, "/admin/index.php", "/admin/?q=%2F"),
        (
            "/admin/profile",
            Some("pinned.php"),
            "/pinned.php",
            "/admin/profile?q=%2F",
        ),
        (
            "/pinned.php/profile",
            Some("pinned.php"),
            "/pinned.php",
            "/pinned.php?q=%2F",
        ),
    ] {
        runtime.entry_script = entry.map(String::from);
        let resolution = determine_script_name(
            path,
            runtime.entry_script.as_ref(),
            &runtime.indexes,
        )
        .unwrap();
        let relative = runtime
            .script_relative_path(
                &resolution.script_name,
                resolution.from_entry_script,
            )
            .unwrap();
        let script_file = runtime.dir.join(relative);
        let mut parts = Request::builder()
            .uri(format!("{path}?q=%2F"))
            .body(())
            .unwrap()
            .into_parts()
            .0;
        let remote = "127.0.0.1:1234".parse().unwrap();
        parts.extensions.insert(RealIpExt("127.0.0.1".parse().unwrap()));
        parts
            .extensions
            .insert(HostName(Arc::new("app.example.test".parse().unwrap())));
        parts.extensions.insert(Arc::new(ConnCtx {
            state: Default::default(),
            listener: "test".parse().unwrap(),
            proto: None,
            remote,
            local_ip: "127.0.0.1".parse().unwrap(),
            local_port: 8080,
            real_ip: None,
            country_code: None,
            tls: None,
        }));
        let ctx = RequestCtx::from_parts(
            &parts,
            &runtime,
            &resolution.script_name,
            &script_file,
        )
        .unwrap();
        let params = build_params(&ctx, &runtime, 0);
        assert_eq!(params["SCRIPT_NAME"], expected_name);
        assert_eq!(
            params["SCRIPT_FILENAME"],
            runtime
                .dir
                .join(if entry.is_some() { "pinned.php" } else { "index.php" })
                .to_str()
                .unwrap()
        );
        assert_eq!(params["REQUEST_URI"], expected_uri);
        assert_eq!(params["QUERY_STRING"], "q=%2F");
        assert_eq!(
            ctx.path_info.as_deref(),
            if path == "/pinned.php/profile" { Some("/profile") } else { None }
        );
    }
}
