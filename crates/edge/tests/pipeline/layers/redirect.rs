use super::*;
use claviron_http::ServiceResult;
use crate::pipeline::host::HostPipelineBuilder;
use http::Request;

#[test]
fn declared_redirect_order_and_segment_boundaries_are_preserved() {
    let mut builder = HostPipelineBuilder::<(), HostResponse>::new();
    for yaml in ["{path: /old, to: /first}", "{path: /old/deep, to: /second}"] {
        builder.layer(RedirectLayer::new(&serde_yaml::from_str(yaml).unwrap())).unwrap();
    }
    let pipeline = builder.build();
    for path in ["/old", "/old/", "/old/deep"] {
        let ServiceResult::Handled(reply) = pipeline.prepare(Request::builder().uri(path).body(()).unwrap()) else { panic!("matching redirect expected"); };
        let response = reply.finish_with(Into::into);
        assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(response.headers()[http::header::LOCATION], "/first");
    }
    assert!(matches!(pipeline.prepare(Request::builder().uri("/older").body(()).unwrap()), ServiceResult::Unhandled(_)));
}

#[test]
fn redirect_tokens_do_not_reinterpret_request_data() {
    let uri: Uri = "/old/$args?next=$uri&value=%2F".parse().unwrap();
    assert_eq!(Target::compile("https://new.test$uri?$args&copy=$query&literal=$unknown").render(&uri).unwrap(),
        "https://new.test/old/$args?next=$uri&value=%2F&copy=next=$uri&value=%2F&literal=$unknown");
    assert_eq!(uri, "/old/$args?next=$uri&value=%2F");
    assert!(matches!(Target::compile("/new"), Target::Static(_)));
    assert_eq!(Target::compile("/new?$args$query").render(&"/old".parse().unwrap()).unwrap(), "/new?");
}
