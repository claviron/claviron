use super::*;

fn policy(yaml: &str) -> Arc<RequestPolicy> {
    RequestPolicy::compile(
        &serde_yaml::from_str(&format!("root: public\n{yaml}")).unwrap(),
    )
    .unwrap()
}

#[test]
fn acl_precedes_ordered_segment_redirects() {
    assert!(
        RequestPolicy::compile(
            &serde_yaml::from_str("root: public\nacl: {}\nredirect: []")
                .unwrap()
        )
        .is_none()
    );
    let policy =
        policy("acl: {'/private': {policy: deny}}\nredirect: {to: /new}");
    let ip = "203.0.113.1".parse().unwrap();
    assert_eq!(
        policy.check(&"/private".parse().unwrap(), ip),
        Err(HostResponse::Rejected(RequestRejection::AccessDenied))
    );
    assert_eq!(
        policy.check(&"/public".parse().unwrap(), ip),
        Err(HostResponse::Redirect {
            status: StatusCode::TEMPORARY_REDIRECT,
            location: HeaderValue::from_static("/new")
        })
    );
    let redirects = self::policy(
        "redirect: [{path: /old, to: /first}, {path: /old/deep, to: /second}]",
    );
    for path in ["/old", "/old/", "/old/deep"] {
        let reply = redirects
            .check(&path.parse().unwrap(), ip)
            .unwrap_err()
            .into_response();
        assert_eq!(reply.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(reply.headers()[http::header::LOCATION], "/first");
    }
    assert!(redirects.check(&"/older".parse().unwrap(), ip).is_ok());
}

#[test]
fn redirect_tokens_do_not_reinterpret_request_data() {
    let policy = policy(
        "redirect: {to: 'https://new.test$uri?$args&copy=$query&literal=$unknown'}",
    );
    let uri: Uri = "/old/$args?next=$uri&value=%2F".parse().unwrap();
    let before = uri.clone();
    let reply = policy
        .check(&uri, "203.0.113.1".parse().unwrap())
        .unwrap_err()
        .into_response();
    assert_eq!(
        reply.headers()[http::header::LOCATION],
        "https://new.test/old/$args?next=$uri&value=%2F&copy=next=$uri&value=%2F&literal=$unknown"
    );
    assert_eq!(uri, before);
    let target = Target::compile("/new");
    assert!(matches!(target, Target::Static(_)));
    assert_eq!(
        Target::compile("/new?$args$query")
            .render(&"/old".parse().unwrap())
            .unwrap(),
        "/new?"
    );
}
