use http::{Version, header};

use super::*;
use crate::deny::DenyService;

#[test]
fn cleartext_authority_handoff_rejects_invalid_heads() {
    let deny = DenyService::empty();
    for (version, uri, hosts, expected) in [
        (Version::HTTP_11, "/", &[][..], AuthorityError::Missing),
        (Version::HTTP_10, "/", &[][..], AuthorityError::Missing),
        (
            Version::HTTP_11,
            "http://app.test/",
            &["app.test", "other.test"][..],
            AuthorityError::Conflicting,
        ),
        (
            Version::HTTP_11,
            "http://app.test/",
            &["user@app.test"][..],
            AuthorityError::Invalid,
        ),
        (
            Version::HTTP_11,
            "https://app.test/",
            &["app.test"][..],
            AuthorityError::WrongScheme,
        ),
    ] {
        let mut request = Request::builder().version(version).uri(uri);
        for host in hosts {
            request = request.header(header::HOST, *host);
        }
        let checked =
            deny.check_request(request.body(()).unwrap()).ok().unwrap();
        assert_eq!(CleartextRequest::new(checked).err(), Some(expected));
    }
}

#[test]
fn cleartext_authority_handoff_keeps_the_request_and_its_selected_host() {
    struct UnreadBody;
    let deny = DenyService::empty();
    let table = serde_yaml::from_str(
        "app.test: {root: public, aliases: [alias.test]}\n'[::1]': {root: public}",
    )
    .unwrap();
    for (uri, host, canonical) in [
        ("/first?q=%2F", "ALIAS.test:8080", "app.test"),
        ("/second", "[::1]:8080", "[::1]"),
        ("http://ALIAS.test:8080/original?q=%2F", "other.test", "app.test"),
    ] {
        let request = Request::builder()
            .uri(uri)
            .header(header::HOST, host)
            .body(UnreadBody)
            .unwrap();
        let checked = deny.check_request(request).ok().unwrap();
        let validated = CleartextRequest::new(checked).unwrap();
        let resolved =
            resolve_request_authority(validated.authority(), &table).unwrap();
        assert_eq!(resolved.vhost_key.0.as_ref(), &canonical.parse().unwrap());
        assert_eq!(resolved.hostname.0.as_ref(), validated.authority().host());
        assert_eq!(validated.request().uri(), uri);
        assert_eq!(validated.request().headers()[header::HOST], host);
        let (checked, authority) = validated.into_parts();
        let request = deny.accept_checked(checked).ok().unwrap();
        assert_eq!(request.uri(), uri);
        assert_eq!(request.headers()[header::HOST], host);
        assert_eq!(&*resolved.hostname.0, authority.host());
    }
}
