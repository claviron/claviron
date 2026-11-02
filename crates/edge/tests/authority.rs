use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use claviron_edge::pipeline::host::{
    HostLayerBuilder, HostPipeline, HostPipelineBuilder,
};
use claviron_edge::pipeline::layers::{AuthorityLayer, HostLayer};
use claviron_edge::transport::{
    RequestAuthority, RequestRejection, VhostTable, resolve_request_authority,
};
use claviron_http::ServiceResult;
use claviron_http::authority::AuthorityError;
use claviron_http::ext::HostName;
use http::{Request, StatusCode, Version, header};
use http_body::Body;

struct AfterAuthority(Arc<AtomicUsize>);

impl HostLayer<(), RequestRejection> for AfterAuthority {
    fn prepare(
        self,
        layer: &mut HostLayerBuilder<(), RequestRejection>,
    ) -> claviron_edge::Result<()> {
        layer.before(move |_, _| {
            self.0.fetch_add(1, Ordering::Relaxed);
            ControlFlow::Continue(())
        });
        Ok(())
    }
}

fn authority_pipeline(
    entries: Arc<AtomicUsize>
) -> HostPipeline<(), RequestRejection> {
    let mut builder = HostPipelineBuilder::new();
    builder
        .layer(AuthorityLayer(HostName(Arc::new("app.test".parse().unwrap()))))
        .unwrap();
    builder.layer(AfterAuthority(entries)).unwrap();
    builder.build()
}

#[test]
fn configured_authority_resolution_keeps_aliases_and_ipv6() {
    let table: VhostTable = serde_yaml::from_str(
        "app.test: {root: public, aliases: [alias.test]}\n'[::1]': {root: public}"
    )
    .unwrap();
    for (raw, canonical) in
        [("ALIAS.test:8080", "app.test"), ("[::1]:8080", "[::1]")]
    {
        let request = Request::builder()
            .uri("/original?q=1")
            .header(header::HOST, raw)
            .body(())
            .unwrap();
        let authority = RequestAuthority::from_cleartext(&request).unwrap();
        let resolved = resolve_request_authority(&authority, &table).unwrap();
        assert_eq!(resolved.vhost_key.0.as_ref(), &canonical.parse().unwrap());
        assert_eq!(authority.authority().as_str(), raw);
        assert_eq!(request.uri(), "/original?q=1");
    }
}

#[test]
fn authority_layer_rejects_before_later_actions_or_body_access() {
    // No Body implementation: authority checks must not gain body access.
    struct UnreadBody;
    let entries = Arc::new(AtomicUsize::new(0));
    let pipeline = authority_pipeline(entries.clone());
    for version in [Version::HTTP_11, Version::HTTP_2, Version::HTTP_3] {
        for (hosts, expected) in [
            (&[][..], AuthorityError::Missing),
            (&["app.test", "app.test"][..], AuthorityError::Conflicting),
            (&["bad@host"][..], AuthorityError::Invalid),
            (&["app.test:+443"][..], AuthorityError::Invalid),
        ] {
            entries.store(0, Ordering::Relaxed);
            let mut request = Request::builder().version(version);
            for host in hosts {
                request = request.header(header::HOST, *host);
            }
            let ServiceResult::Handled(reply) =
                pipeline.prepare(request.body(UnreadBody).unwrap())
            else {
                panic!("invalid authority reached later actions");
            };
            let response = reply.finish_with(|reason| {
                assert_eq!(
                    reason,
                    RequestRejection::InvalidAuthority(expected)
                );
                reason.into()
            });
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            assert!(response.body().is_end_stream());
            assert_eq!(entries.load(Ordering::Relaxed), 0);
        }
        let wrong = Request::builder()
            .version(version)
            .header(header::HOST, "other.test")
            .body(UnreadBody)
            .unwrap();
        let ServiceResult::Handled(reply) = pipeline.prepare(wrong) else {
            panic!("mismatched authority reached later actions");
        };
        let response = reply.finish_with(|reason| {
            assert_eq!(reason, RequestRejection::Misdirected);
            reason.into()
        });
        assert_eq!(response.status(), StatusCode::MISDIRECTED_REQUEST);
        assert_eq!(entries.load(Ordering::Relaxed), 0);

        let good = Request::builder()
            .version(version)
            .header(header::HOST, "APP.test:443")
            .body(UnreadBody)
            .unwrap();
        assert!(matches!(pipeline.prepare(good), ServiceResult::Unhandled(_)));
        assert_eq!(entries.load(Ordering::Relaxed), 1);
    }
}

#[test]
fn authority_layer_preserves_wire_metadata_and_http10_default() {
    let entries = Arc::new(AtomicUsize::new(0));
    let pipeline = authority_pipeline(entries.clone());
    let request = Request::builder()
        .version(Version::HTTP_2)
        .uri("https://APP.test:443/original?raw=%2f")
        .body(())
        .unwrap();
    let ServiceResult::Unhandled(prepared) = pipeline.prepare(request) else {
        panic!("valid authority was rejected");
    };
    assert_eq!(prepared.head().uri, "https://APP.test:443/original?raw=%2f");
    assert!(!prepared.head().headers.contains_key(header::HOST));
    assert_eq!(entries.load(Ordering::Relaxed), 1);

    let legacy = Request::builder().version(Version::HTTP_10).body(()).unwrap();
    assert!(matches!(pipeline.prepare(legacy), ServiceResult::Unhandled(_)));
    assert_eq!(entries.load(Ordering::Relaxed), 2);
}
