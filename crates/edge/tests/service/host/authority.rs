use super::*;
use crate::service::AcceptedAuthority;
use claviron_http::authority::AuthorityError;
use http::{Version, header};

#[test]
fn tls_authority_is_checked_for_every_request_without_reading_the_body() {
    // Deliberately not an HTTP Body: admission only borrows request parts.
    struct UnreadBody;
    let service = service("", RequestPolicy::unlimited(), None);
    // One connection: what it accepted earlier must not admit what follows.
    let mut accepted = AcceptedAuthority::default();
    for version in [Version::HTTP_11, Version::HTTP_2, Version::HTTP_3] {
        for (hosts, expected) in [
            (&[][..], AuthorityError::Missing),
            (&["app.test", "app.test"][..], AuthorityError::Conflicting),
            (&["bad@host"][..], AuthorityError::Invalid),
            (&["app.test:+443"][..], AuthorityError::Invalid),
        ] {
            let mut request = Request::builder().version(version);
            for host in hosts {
                request = request.header(header::HOST, *host);
            }
            let request = request.body(UnreadBody).unwrap();
            assert_eq!(
                service.validate_authority(&request, &mut accepted),
                Err(RequestRejection::InvalidAuthority(expected))
            );
        }
        for (host, expected) in [
            ("APP.test:443", Ok(())),
            ("other.test", Err(RequestRejection::Misdirected)),
            ("app.test", Ok(())),
        ] {
            let request = Request::builder()
                .version(version)
                .header(header::HOST, host)
                .body(UnreadBody)
                .unwrap();
            assert_eq!(
                service.validate_authority(&request, &mut accepted),
                expected
            );
            assert!(request.extensions().is_empty());
        }
    }
}

#[test]
fn tls_authority_admission_preserves_wire_metadata_and_body_ownership() {
    let service = service("", RequestPolicy::unlimited(), None);
    let mut accepted = AcceptedAuthority::default();
    let drops = Arc::new(AtomicUsize::new(0));
    for (uri, host, expected) in [
        ("https://APP.test:443/original?raw=%2f", "APP.test:443", Ok(())),
        (
            "https://app.test/",
            "other.test",
            Err(RequestRejection::InvalidAuthority(
                AuthorityError::Conflicting,
            )),
        ),
        (
            "https://other.test/",
            "other.test",
            Err(RequestRejection::Misdirected),
        ),
    ] {
        let body = OwnedBody {
            bytes: Bytes::from_static(b"untouched"),
            drops: drops.clone(),
        };
        let pointer = body.bytes.as_ptr();
        let request = Request::builder()
            .version(Version::HTTP_2)
            .uri(uri)
            .header(header::HOST, host)
            .body(body)
            .unwrap();
        let before = drops.load(Ordering::Relaxed);
        assert_eq!(
            service.validate_authority(&request, &mut accepted),
            expected
        );
        assert_eq!(request.uri(), uri);
        assert_eq!(request.headers()[header::HOST], host);
        assert_eq!(request.body().bytes.as_ptr(), pointer);
        assert_eq!(drops.load(Ordering::Relaxed), before);
        drop(request);
        assert_eq!(drops.load(Ordering::Relaxed), before + 1);
    }
    let legacy = Request::builder().version(Version::HTTP_10).body(()).unwrap();
    assert_eq!(service.validate_authority(&legacy, &mut accepted), Ok(()));
}

#[test]
fn tls_repeat_is_admitted_only_for_the_fields_last_accepted() {
    let service = service("", RequestPolicy::unlimited(), None);
    let mut accepted = AcceptedAuthority::default();
    let request = |version, hosts: &[&str]| {
        let mut request = Request::builder().version(version);
        for host in hosts {
            request = request.header(header::HOST, *host);
        }
        request.body(()).unwrap()
    };
    let admitted = request(Version::HTTP_11, &["app.test"]);
    assert!(!accepted.repeats(&admitted));
    assert_eq!(service.validate_authority(&admitted, &mut accepted), Ok(()));
    assert!(accepted.repeats(&admitted));

    // Anything that differs is validated, and a refusal records nothing.
    for (version, hosts, expected) in [
        (Version::HTTP_11, &["other.test"][..], RequestRejection::Misdirected),
        (
            Version::HTTP_11,
            &["app.test", "app.test"][..],
            RequestRejection::InvalidAuthority(AuthorityError::Conflicting),
        ),
        (
            Version::HTTP_11,
            &[][..],
            RequestRejection::InvalidAuthority(AuthorityError::Missing),
        ),
        (
            Version::HTTP_2,
            &[][..],
            RequestRejection::InvalidAuthority(AuthorityError::Missing),
        ),
    ] {
        let other = request(version, hosts);
        assert!(!accepted.repeats(&other));
        assert_eq!(
            service.validate_authority(&other, &mut accepted),
            Err(expected)
        );
        assert!(accepted.repeats(&admitted));
    }

    // An accepted Host field does not cover a target naming another host.
    let conflicting = Request::builder()
        .version(Version::HTTP_2)
        .uri("https://other.test/")
        .header(header::HOST, "app.test")
        .body(())
        .unwrap();
    assert!(!accepted.repeats(&conflicting));
    assert_eq!(
        service.validate_authority(&conflicting, &mut accepted),
        Err(RequestRejection::InvalidAuthority(AuthorityError::Conflicting))
    );
}
