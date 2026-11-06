use super::*;
use claviron_http::{RouteResponse, ServiceResult, empty_body};
use futures_util::FutureExt;
use http::{Request, Response};

fn apply(
    defaults: &HostHeadersConfig,
    scope: &HostHeadersConfig,
) -> RouteResponse {
    let mut builder = HostPipelineBuilder::<()>::new();
    prepare_headers(&mut builder, [defaults, scope]).unwrap();
    let pipeline = builder.build();
    let ServiceResult::Unhandled(prepared) =
        pipeline.prepare(Request::new(empty_body()))
    else {
        panic!("response layers cannot claim a request");
    };
    prepared
        .dispatch(|_| {
            std::future::ready(
                Response::builder()
                    .header("x-remove", "application")
                    .body(empty_body())
                    .unwrap(),
            )
        })
        .now_or_never()
        .expect("ready fixture")
}

#[test]
fn explicit_scope_header_edits_override_defaults_without_an_implicit_server() {
    let defaults: HostHeadersConfig = serde_yaml::from_str("server: edge\nset: {x-policy: global, x-remove: global}\nunset: [x-revive, server]").unwrap();
    let scope = serde_yaml::from_str("server: host\nset: {x-policy: host, x-revive: host}\nunset: [x-remove]").unwrap();
    let response = apply(&defaults, &scope);
    assert_eq!(response.headers()[http::header::SERVER], "host");
    assert_eq!(response.headers()["x-policy"], "host");
    assert_eq!(response.headers()["x-revive"], "host");
    assert!(!response.headers().contains_key("x-remove"));
    let mut builder = HostPipelineBuilder::<()>::new();
    prepare_headers(&mut builder, [&HostHeadersConfig::default()]).unwrap();
    assert!(builder.is_empty());
}

#[test]
fn invalid_programmatic_server_values_fail_during_layer_preparation() {
    let mut builder = HostPipelineBuilder::<()>::new();
    let invalid = HostHeadersConfig {
        server: Some("invalid\nvalue".into()),
        ..HostHeadersConfig::default()
    };
    assert!(prepare_headers(&mut builder, [&invalid]).is_err());
    assert!(builder.is_empty());
}
