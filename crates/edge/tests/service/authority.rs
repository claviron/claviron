use http::{Version, header};

use super::*;
use crate::deny::DenyService;

fn authority(host: &str) -> RequestAuthority {
    let request =
        Request::builder().header(header::HOST, host).body(()).unwrap();
    RequestAuthority::from_cleartext(&request).unwrap()
}

#[test]
fn host_identity_reuses_two_connection_local_allocations() {
    let table: VhostTable =
        serde_yaml::from_str("default: {root: public}").unwrap();
    let mut cache = HostIdentityCache::default();
    let first = cache.resolve(&authority("127.0.0.1:8080"), &table).unwrap();
    let next = cache.resolve(&authority("127.0.0.1:9090"), &table).unwrap();
    assert!(Arc::ptr_eq(&first.hostname.0, &next.hostname.0));
    assert!(Arc::ptr_eq(&first.vhost_key.0, &next.vhost_key.0));
    assert_eq!(first.hostname.0.as_ref(), &"127.0.0.1".parse().unwrap());
    assert_eq!(first.vhost_key.0.as_ref(), &"default".parse().unwrap());

    let other_connection = HostIdentityCache::default()
        .resolve(&authority("127.0.0.1:8080"), &table)
        .unwrap();
    // Reuse does not introduce a refcount shared by all connections to a host.
    assert!(!Arc::ptr_eq(&first.hostname.0, &other_connection.hostname.0));
    assert!(!Arc::ptr_eq(&first.vhost_key.0, &other_connection.vhost_key.0));
}

#[test]
fn host_identity_reuse_never_pins_alias_routing_or_config() {
    let first_table: VhostTable =
        serde_yaml::from_str("app.test: {root: first, aliases: [alias.test]}")
            .unwrap();
    let updated_table: VhostTable = serde_yaml::from_str(
        "next.test: {root: second, aliases: [alias.test]}",
    )
    .unwrap();
    let mut cache = HostIdentityCache::default();
    let first =
        cache.resolve(&authority("ALIAS.test:8080"), &first_table).unwrap();
    let updated =
        cache.resolve(&authority("alias.test:8080"), &updated_table).unwrap();
    assert!(Arc::ptr_eq(&first.hostname.0, &updated.hostname.0));
    assert_eq!(first.vhost_key.0.as_ref(), &"app.test".parse().unwrap());
    assert_eq!(updated.vhost_key.0.as_ref(), &"next.test".parse().unwrap());
    assert!(!Arc::ptr_eq(&first.vhost_key.0, &updated.vhost_key.0));
    assert!(!Arc::ptr_eq(&first.vhost_config, &updated.vhost_config));
    assert!(
        cache
            .resolve(&authority("alias.test"), &VhostTable::default())
            .is_none()
    );
    let restored =
        cache.resolve(&authority("alias.test"), &first_table).unwrap();
    assert!(Arc::ptr_eq(&first.vhost_config, &restored.vhost_config));
    assert_eq!(restored.vhost_key, first.vhost_key);
}

#[test]
fn host_identity_cache_releases_replaced_names_and_keeps_accepted_requests() {
    let table: VhostTable =
        serde_yaml::from_str("default: {root: public}").unwrap();
    let mut cache = HostIdentityCache::default();
    let first = cache.resolve(&authority("first.test"), &table).unwrap();
    let old_name = Arc::downgrade(&first.hostname.0);
    let next = cache.resolve(&authority("second.test"), &table).unwrap();
    assert_eq!(first.hostname.0.as_ref(), &"first.test".parse().unwrap());
    assert_eq!(next.hostname.0.as_ref(), &"second.test".parse().unwrap());
    assert!(Arc::ptr_eq(&first.vhost_key.0, &next.vhost_key.0));
    assert!(old_name.upgrade().is_some());
    drop(first);
    assert!(old_name.upgrade().is_none());

    assert!(
        cache
            .resolve(&authority("unhosted.test"), &VhostTable::default())
            .is_none()
    );
    let repeated = cache.resolve(&authority("second.test"), &table).unwrap();
    assert!(Arc::ptr_eq(&next.hostname.0, &repeated.hostname.0));
}

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
