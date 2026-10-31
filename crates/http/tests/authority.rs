use claviron_http::authority::{AuthorityError, RequestAuthority};
use http::{Method, Request, Version, header};

fn request(
    version: Version,
    uri: &str,
    hosts: &[&str],
) -> Request<()> {
    let mut builder = Request::builder().version(version).uri(uri);
    for host in hosts {
        builder = builder.header(header::HOST, *host);
    }
    builder.body(()).unwrap()
}

#[test]
fn detached_head_uses_the_same_authority_rules_without_a_body() {
    for (version, uri, hosts) in [
        (Version::HTTP_10, "/", vec![]),
        (Version::HTTP_11, "/", vec![]),
        (Version::HTTP_11, "http://app.test/", vec!["other.test"]),
        (Version::HTTP_2, "https://app.test/", vec!["other.test"]),
        (Version::HTTP_3, "https://app.test/", vec!["app.test:443"]),
        (Version::HTTP_11, "/", vec!["app.test", "app.test"]),
    ] {
        let request = request(version, uri, &hosts);
        let expected = RequestAuthority::from_request(&request);
        let (head, ()) = request.into_parts();
        let actual = RequestAuthority::from_parts(&head);
        assert_eq!(actual.as_ref().err(), expected.as_ref().err());
        assert_eq!(
            actual.ok().flatten().map(|authority| (
                authority.authority().clone(),
                authority.host().clone()
            )),
            expected.ok().flatten().map(|authority| (
                authority.authority().clone(),
                authority.host().clone()
            )),
        );
    }
}

#[test]
fn errors_implement_the_standard_error_contract_without_losing_the_variant() {
    for (error, message) in [
        (AuthorityError::Missing, "request authority is missing"),
        (AuthorityError::Invalid, "request authority is invalid"),
        (AuthorityError::Conflicting, "request authority fields conflict"),
        (
            AuthorityError::WrongScheme,
            "request authority uses an unsupported scheme",
        ),
    ] {
        let source: &dyn std::error::Error = &error;
        assert_eq!(source.to_string(), message);
        assert!(source.source().is_none());
        assert_eq!(source.downcast_ref::<AuthorityError>(), Some(&error));
    }
}

#[test]
fn http11_requires_host_even_with_an_absolute_target() {
    for uri in ["/", "http://app.test/resource", "app.test:443"] {
        assert_eq!(
            RequestAuthority::from_request(&request(
                Version::HTTP_11,
                uri,
                &[]
            ))
            .err(),
            Some(AuthorityError::Missing)
        );
    }
}

#[test]
fn http10_can_leave_default_authority_selection_to_the_connection() {
    let no_host = request(Version::HTTP_10, "/", &[]);
    assert!(RequestAuthority::from_request(&no_host).unwrap().is_none());
    assert_eq!(
        RequestAuthority::from_cleartext(&no_host).err(),
        Some(AuthorityError::Missing)
    );
    for (uri, hosts) in
        [("http://app.test/", &[][..]), ("/", &["app.test"][..])]
    {
        let parsed = RequestAuthority::from_request(&request(
            Version::HTTP_10,
            uri,
            hosts,
        ))
        .unwrap()
        .unwrap();
        assert_eq!(parsed.authority().as_str(), "app.test");
    }
}

#[test]
fn every_version_rejects_duplicate_and_malformed_host_fields() {
    for version in
        [Version::HTTP_10, Version::HTTP_11, Version::HTTP_2, Version::HTTP_3]
    {
        for hosts in [["app.test", "app.test"], ["app.test", "other.test"]] {
            assert_eq!(
                RequestAuthority::from_request(&request(version, "/", &hosts))
                    .err(),
                Some(AuthorityError::Conflicting)
            );
        }
        for host in [
            "",
            "user@app.test",
            "app.test:",
            "app.test:xyz",
            "app.test:65536",
            " app.test",
            "app.test ",
            "app.test,other.test",
            "[::1]:xyz",
        ] {
            assert_eq!(
                RequestAuthority::from_request(&request(version, "/", &[host]))
                    .err(),
                Some(AuthorityError::Invalid),
                "{version:?}: {host:?}"
            );
        }
    }
}

#[test]
fn http1_target_authority_wins_without_rewriting_the_request() {
    for version in [Version::HTTP_10, Version::HTTP_11] {
        let request = request(
            version,
            "http://APP.test:8080/original?q=%2F",
            &["other.test"],
        )
        .map(|()| 42);
        let original_uri = request.uri().clone();
        let original_headers = request.headers().clone();
        let parsed = RequestAuthority::from_cleartext(&request).unwrap();
        assert_eq!(parsed.authority().as_str(), "APP.test:8080");
        assert_eq!(parsed.host(), &"app.test".parse().unwrap());
        assert_eq!(request.uri(), &original_uri);
        assert_eq!(request.headers(), &original_headers);
        assert_eq!(*request.body(), 42);
    }
}

#[test]
fn http1_absolute_target_does_not_exempt_invalid_host() {
    for hosts in [&["invalid:port"][..], &["app.test", "other.test"][..]] {
        let request = request(Version::HTTP_11, "http://app.test/", hosts);
        assert!(RequestAuthority::from_request(&request).is_err());
    }
}

#[test]
fn h2_h3_allow_authority_or_host_but_require_agreement_when_both_are_present() {
    for version in [Version::HTTP_2, Version::HTTP_3] {
        for (uri, hosts, expected) in [
            ("https://app.test/", &[][..], "app.test"),
            ("/", &["app.test:8443"][..], "app.test:8443"),
            ("https://app.test/", &["APP.test.:443"][..], "app.test"),
            ("http://app.test/", &["app.test:80"][..], "app.test"),
            ("https://[::1]/", &["[0:0:0:0:0:0:0:1]:443"][..], "[::1]"),
        ] {
            let parsed =
                RequestAuthority::from_request(&request(version, uri, hosts))
                    .unwrap()
                    .unwrap();
            assert_eq!(parsed.authority().as_str(), expected);
        }
        for host in ["other.test", "app.test:80", "app.test:8443"] {
            assert_eq!(
                RequestAuthority::from_request(&request(
                    version,
                    "https://app.test/",
                    &[host]
                ))
                .err(),
                Some(AuthorityError::Conflicting)
            );
        }
        assert_eq!(
            RequestAuthority::from_request(&request(version, "/", &[])).err(),
            Some(AuthorityError::Missing)
        );
    }
}

#[test]
fn connect_and_options_authorities_do_not_depend_on_a_path_or_hyper_extension()
{
    let mut classic = request(Version::HTTP_11, "app.test:80", &["app.test"]);
    *classic.method_mut() = Method::CONNECT;
    assert_eq!(
        RequestAuthority::from_request(&classic)
            .unwrap()
            .unwrap()
            .authority()
            .as_str(),
        "app.test:80"
    );
    for version in [Version::HTTP_2, Version::HTTP_3] {
        for uri in ["app.test:443", "https://app.test/socket"] {
            let mut request = request(version, uri, &[]);
            *request.method_mut() = Method::CONNECT;
            assert!(
                RequestAuthority::from_request(&request).unwrap().is_some()
            );
        }
    }
    let mut options = request(Version::HTTP_11, "*", &["app.test"]);
    *options.method_mut() = Method::OPTIONS;
    assert_eq!(
        RequestAuthority::from_request(&options)
            .unwrap()
            .unwrap()
            .authority()
            .as_str(),
        "app.test"
    );
}

#[test]
fn invalid_target_ports_and_userinfo_are_rejected_too() {
    for uri in [
        "http://app.test:xyz/",
        "http://app.test:65536/",
        "http://user@app.test/",
    ] {
        let request = request(Version::HTTP_11, uri, &["app.test"]);
        assert_eq!(
            RequestAuthority::from_request(&request).err(),
            Some(AuthorityError::Invalid)
        );
    }
}

#[test]
fn cleartext_scheme_check_does_not_change_shared_authority_semantics() {
    let request = request(Version::HTTP_11, "https://app.test/", &["app.test"]);
    assert!(RequestAuthority::from_request(&request).unwrap().is_some());
    assert_eq!(
        RequestAuthority::from_cleartext(&request).err(),
        Some(AuthorityError::WrongScheme)
    );
}

#[test]
fn wire_authority_rejects_config_only_host_forms_and_non_digit_ports() {
    for host in [
        "app.test:+443",
        "[::1]:+443",
        "*.app.test",
        "app.test..",
        "app.test...:443",
        "[127.0.0.1]",
        "[127.0.0.1]:443",
        "2001:db8::1",
        "[fe80::1%25en0]",
        "app..test",
        "-app.test",
        "app-.test",
        "app_test",
        "app.test/path",
        "app.test?query",
    ] {
        assert_eq!(
            RequestAuthority::from_request(&request(
                Version::HTTP_11,
                "/",
                &[host]
            ))
            .err(),
            Some(AuthorityError::Invalid),
            "{host:?}"
        );
    }
    let oversized_label = format!("{}.test", "a".repeat(64));
    assert_eq!(
        RequestAuthority::from_request(&request(
            Version::HTTP_11,
            "/",
            &[&oversized_label]
        ))
        .err(),
        Some(AuthorityError::Invalid)
    );
}

#[test]
fn concrete_host_forms_preserve_wire_authority_and_normalize_only_identity() {
    for (raw, normalized) in [
        ("APP.test.:00443", "app.test"),
        ("LOCALHOST:0", "localhost"),
        ("127.0.0.1:65535", "127.0.0.1"),
        ("[2001:DB8::1]:443", "[2001:db8::1]"),
        ("[::ffff:192.0.2.1]", "[::ffff:192.0.2.1]"),
    ] {
        let request = request(Version::HTTP_11, "/raw?query=%2F", &[raw]);
        let parsed = RequestAuthority::from_request(&request).unwrap().unwrap();
        assert_eq!(parsed.authority().as_str(), raw);
        assert_eq!(parsed.host(), &normalized.parse().unwrap());
        assert_eq!(request.headers()[header::HOST], raw);
        assert_eq!(request.uri(), "/raw?query=%2F");
    }
}
