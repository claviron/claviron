use super::*;
use http::{Version, header};

#[test]
fn tls_authority_is_checked_for_every_request_without_reading_the_body() {
    // Deliberately not an HTTP Body: admission only borrows request parts.
    struct UnreadBody;
    let service = service("", RequestPolicy::unlimited(), None);
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
                service.validate_authority(&request),
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
            assert_eq!(service.validate_authority(&request), expected);
            assert!(request.extensions().is_empty());
        }
    }
}

#[test]
fn tls_authority_admission_preserves_wire_metadata_and_body_ownership() {
    let service = service("", RequestPolicy::unlimited(), None);
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
        assert_eq!(service.validate_authority(&request), expected);
        assert_eq!(request.uri(), uri);
        assert_eq!(request.headers()[header::HOST], host);
        assert_eq!(request.body().bytes.as_ptr(), pointer);
        assert_eq!(drops.load(Ordering::Relaxed), before);
        drop(request);
        assert_eq!(drops.load(Ordering::Relaxed), before + 1);
    }
    let legacy = Request::builder().version(Version::HTTP_10).body(()).unwrap();
    assert_eq!(service.validate_authority(&legacy), Ok(()));
}
