use super::*;

fn compile(yaml: &str) -> Arc<CompiledAcl> {
    CompiledAcl::compile(&serde_yaml::from_str(yaml).unwrap()).unwrap()
}

#[test]
fn longest_segment_prefix_wins_without_falling_back_after_denial() {
    let acl = compile(
        r#"
'/': {policy: allow}
'/admin': {policy: deny, allowed: ['10.0.0.0/8', '2001:db8::/32']}
'/admin/public': {policy: allow}
'/admin/private': {policy: deny}
"#,
    );
    for ip in ["10.2.3.4", "203.0.113.1", "2001:db8::1", "2001:db9::1"] {
        let internal = matches!(ip, "10.2.3.4" | "2001:db8::1");
        let ip = ip.parse().unwrap();
        for path in ["/admin", "/admin/", "/admin/users"] {
            assert_eq!(acl.check(path, ip).is_ok(), internal, "{path}, {ip}");
        }
        for path in
            ["/", "/administrator", "/admin/public", "/admin/public/info"]
        {
            assert!(acl.check(path, ip).is_ok(), "{path}, {ip}");
        }
        assert_eq!(
            acl.check("/admin/private", ip),
            Err(RequestRejection::AccessDenied)
        );
    }
    let blacklist =
        compile("'/blocked': {policy: allow, denied: ['203.0.113.0/24']}");
    assert_eq!(
        blacklist.check("/blocked", "203.0.113.1".parse().unwrap()),
        Err(RequestRejection::AccessDenied)
    );
    assert!(
        blacklist.check("/unmatched", "203.0.113.1".parse().unwrap()).is_ok()
    );
    assert!(
        CompiledAcl::compile(&serde_yaml::from_str("{}").unwrap()).is_none()
    );
}

#[test]
fn common_paths_are_borrowed_and_escaped_text_cannot_evade_acl() {
    assert!(matches!(
        comparison_paths("/admin/users").map(|paths| paths.separated),
        Ok(Cow::Borrowed(_))
    ));
    let acl = compile("'/admin': {policy: deny}\n'/café': {policy: deny}");
    let ip = "203.0.113.1".parse().unwrap();
    for path in ["/adm%69n", "/%61dmin/users", "/caf%C3%A9"] {
        assert_eq!(acl.check(path, ip), Err(RequestRejection::AccessDenied));
    }
    assert!(acl.check("/users/john%20doe", ip).is_ok());
    for path in [
        "/admin%2fpublic",
        "/admin%5Cpublic",
        "/%2561dmin",
        "/admin/../public",
        "/admin/./public",
        "/admin/%2e%2e/public",
        "//admin",
        "/admin\\public",
        "/bad%",
        "/bad%xx",
        "/bad%00",
        "/bad%ff",
        "/admin%3fpublic",
        "/admin%23public",
    ] {
        assert_eq!(
            acl.check(path, ip),
            Err(RequestRejection::AmbiguousPath),
            "{path}"
        );
    }
}

/// npm names a scoped package `@scope%2fname`. Under a rule both readings of
/// the escaped slash fall in, it is a character, not an ambiguity; it still is
/// one when the readings fall under different rules.
#[test]
fn escaped_slash_is_refused_only_when_it_changes_the_rule() {
    let ip = "10.8.0.2".parse().unwrap();
    let site = compile("'/': {policy: deny, allowed: ['10.8.0.0/24']}");
    assert!(
        site.check("/api/packages/revgaming/npm/@revgaming%2furl", ip).is_ok()
    );
    assert_eq!(
        site.check(
            "/api/packages/revgaming/npm/@revgaming%2furl",
            "203.0.113.1".parse().unwrap()
        ),
        Err(RequestRejection::AccessDenied)
    );
    let admin = compile("'/': {policy: allow}\n'/admin': {policy: deny}");
    assert_eq!(
        admin.check("/admin%2fx", ip),
        Err(RequestRejection::AmbiguousPath)
    );
    assert!(admin.check("/public/a%2fb", ip).is_ok());
}

#[test]
fn compiled_order_is_independent_of_yaml_map_order() {
    let acl = compile(
        "'/z': {policy: allow}\n'/': {policy: allow}\n'/aa': {policy: deny}\n'/a': {policy: deny}",
    );
    let paths: Vec<_> =
        acl.rules.iter().map(|(path, _)| path.as_str()).collect();
    assert_eq!(paths, ["/aa", "/a", "/z", "/"]);
}
