use super::*;
use crate::plan::HostPipelineBuilder;
use claviron_http::ServiceResult;
use http::Request;

#[test]
fn selected_scope_runs_redirects_in_declaration_order() {
    let mut builder = HostPipelineBuilder::<(), HostResponse>::new();
    for yaml in ["{to: /first}", "{to: /second}"] {
        builder
            .layer(RedirectLayer::new(&serde_yaml::from_str(yaml).unwrap()))
            .unwrap();
    }
    let pipeline = builder.build();
    for path in ["/old", "/old/", "/old/deep", "/other"] {
        let ServiceResult::Handled(reply) =
            pipeline.prepare(Request::builder().uri(path).body(()).unwrap())
        else {
            panic!("matching redirect expected");
        };
        let response = reply.finish_with(Into::into);
        assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(response.headers()[http::header::LOCATION], "/first");
    }
}

#[test]
fn redirect_tokens_do_not_reinterpret_request_data() {
    let uri: Uri = "/old/$args?next=$uri&value=%2F".parse().unwrap();
    assert_eq!(
        Target::compile(
            "https://new.test$uri?$args&copy=$query&literal=$unknown"
        )
        .render(&uri)
        .unwrap(),
        "https://new.test/old/$args?next=$uri&value=%2F&copy=next=$uri&value=%2F&literal=$unknown"
    );
    assert_eq!(uri, "/old/$args?next=$uri&value=%2F");
    assert!(matches!(Target::compile("/new"), Target::Static(_)));
    assert_eq!(
        Target::compile("/new?$args$query")
            .render(&"/old".parse().unwrap())
            .unwrap(),
        "/new?"
    );
}
