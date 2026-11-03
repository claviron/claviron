use super::*;

use claviron_http::{OtherHandler, OtherService, RouteBody, RouteFuture};
use futures_util::FutureExt;
use http::{Response, StatusCode};
use http_body_util::BodyExt;

static PAYLOAD: &[u8] = b"payload";

struct Reply;

impl OtherHandler for Reply {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> RouteFuture {
        drop(request);
        Box::pin(std::future::ready(Ok(Response::builder()
            .header("server", "application")
            .header("x-application", "remove")
            .header("x-original", "keep")
            .body(claviron_http::bytes_body(Bytes::from_static(PAYLOAD)))
            .unwrap())))
    }
}

#[test]
fn compiled_headers_fold_defaults_and_ancestors_and_apply_to_early_replies() {
    let state = AppState::standalone("scope-headers", ".");
    let http: HttpSnapshot = serde_yaml::from_str(
        "defaults:\n  headers:\n    server: global\n    set: {x-global: inherited, x-mode: global}\n    unset: [x-application]\n  deny: ['^/api/blocked$']\n",
    ).unwrap();
    let config: HostConfig = serde_yaml::from_str(
        r#"
headers:
  server: host
  set: {x-host: root, x-mode: host}
  unset: [x-global]
routes:
  /api:
    headers:
      set: {x-mode: api}
      unset: [x-host]
  /api/deep:
    headers:
      set: {x-extra: deep}
  /api/blocked: {}
"#,
    )
    .unwrap();
    let owner =
        HostCompiler { state: &state, http: Some(&http), protocol: None }
            .prepare(
                &"example.test".parse().unwrap(),
                Arc::new(config),
                Some(RustRouter::Other(OtherService::new(Reply))),
                &HostContributions::default(),
                #[cfg(feature = "rate-limit")]
                None,
                #[cfg(feature = "websocket")]
                None,
            )
            .unwrap();
    for (path, mode, has_root, extra, status) in [
        ("/", "host", true, false, StatusCode::OK),
        ("/api/item", "api", false, false, StatusCode::OK),
        ("/api/deep/item", "api", false, true, StatusCode::OK),
        ("/api/blocked", "api", false, false, StatusCode::FORBIDDEN),
    ] {
        let response = owner
            .handle(
                Request::builder()
                    .uri(path)
                    .body(claviron_http::empty_body())
                    .unwrap(),
            )
            .now_or_never()
            .unwrap()
            .unwrap();
        assert_eq!(response.status(), status);
        assert_eq!(response.headers()["server"], "host");
        assert_eq!(response.headers()["x-mode"], mode);
        assert_eq!(response.headers().contains_key("x-host"), has_root);
        assert_eq!(response.headers().contains_key("x-extra"), extra);
        assert!(!response.headers().contains_key("x-global"));
        assert!(!response.headers().contains_key("x-application"));
        if status == StatusCode::OK {
            assert_eq!(response.headers()["x-original"], "keep");
            let bytes = response
                .into_body()
                .collect()
                .now_or_never()
                .unwrap()
                .unwrap()
                .to_bytes();
            assert_eq!(bytes.as_ptr(), PAYLOAD.as_ptr());
        }
    }
}

#[test]
fn compiled_empty_headers_keep_the_direct_router_and_its_response_identity() {
    let state = AppState::standalone("empty-headers", ".");
    let owner = HostCompiler { state: &state, http: None, protocol: None }
        .prepare(
            &"example.test".parse().unwrap(),
            Arc::new(HostConfig::new("public")),
            Some(RustRouter::Other(OtherService::new(Reply))),
            &HostContributions::default(),
            #[cfg(feature = "rate-limit")]
            None,
            #[cfg(feature = "websocket")]
            None,
        )
        .unwrap();
    assert_eq!(owner.kind(), "other");
    let response = owner
        .handle(Request::new(claviron_http::empty_body()))
        .now_or_never()
        .unwrap()
        .unwrap();
    assert_eq!(response.headers()["server"], "application");
    assert_eq!(response.headers()["x-application"], "remove");
}
