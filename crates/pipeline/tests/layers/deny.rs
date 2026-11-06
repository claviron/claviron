use super::*;
use claviron_http::{ServiceResult, empty_body};
use http::{Request, StatusCode};

#[test]
fn empty_deny_set_installs_no_action_and_active_rules_check_only_the_path() {
    let mut builder = HostPipelineBuilder::<()>::new();
    if let Some(layer) = DenyLayer::new(RegexSet::empty()) {
        builder.layer(layer).unwrap();
    }
    assert!(builder.is_empty());

    let layer = DenyLayer::new(RegexSet::new(["^/private"]).unwrap()).unwrap();
    builder.layer(layer).unwrap();
    let pipeline = builder.build();
    let blocked =
        Request::builder().uri("/private/data").body(empty_body()).unwrap();
    let ServiceResult::Handled(response) = pipeline.prepare(blocked) else {
        panic!("the configured deny rule must block this path");
    };
    assert_eq!(response.finish().status(), StatusCode::FORBIDDEN);
    let allowed = Request::builder()
        .uri("/public?next=/private")
        .body(empty_body())
        .unwrap();
    assert!(matches!(pipeline.prepare(allowed), ServiceResult::Unhandled(_)));
}
