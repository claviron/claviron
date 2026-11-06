use super::*;
use crate::config::HostConfig;
use crate::pipeline::{HostHandlerFuture, HostRequest};
use claviron_http::{
    OtherHandler, OtherService, RouteBody, RouteFuture, ServiceResult,
};
use futures_util::FutureExt;
use http::{Response, StatusCode};

struct Contribution;
impl HostHandler for Contribution {
    fn call(
        &self,
        request: HostRequest,
    ) -> HostHandlerFuture {
        let result = if request.head().uri.path() == "/module" {
            drop(request);
            ServiceResult::Handled(Response::new(claviron_http::empty_body()))
        } else {
            ServiceResult::Unhandled(request)
        };
        Box::pin(std::future::ready(result))
    }
}

struct Router;
impl OtherHandler for Router {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> RouteFuture {
        assert_eq!(request.uri().path(), "/open");
        drop(request);
        Box::pin(std::future::ready(Ok(Response::new(
            claviron_http::empty_body(),
        ))))
    }
}

#[test]
fn custom_redirect_follows_contributions_and_precedes_routers_and_backends() {
    let state = AppState::standalone("custom-redirect", ".");
    let config: HostConfig = serde_yaml::from_str(
        r#"
headers: {set: {x-policy: reached}}
redirect: {to: '/new$uri?$args', code: 307}
index: [index.html]
routes:
  /custom:
    redirect: {to: /fixed, code: 308}
  /open:
    redirect: []
"#,
    )
    .unwrap();
    let host = "example.test".parse().unwrap();
    let root = claviron_http::ext::PathPrefix::root();
    let plan = BackendPlan::prepare(&RouteContext {
        state: &state,
        host: &host,
        config: &config,
        route: &config,
        prefix: &root,
    });
    assert!(plan.steps.is_empty());
    assert!(plan.fallback.is_none());
    let mut contributions = HostContributions::default();
    contributions.applications.push(Arc::new(Contribution));
    let owner = HostCompiler { state: &state, http: None, protocol: None }
        .prepare(
            &host,
            Arc::new(config),
            Some(RustRouter::Other(OtherService::new(Router))),
            &contributions,
            #[cfg(feature = "rate-limit")]
            None,
            #[cfg(feature = "websocket")]
            None,
        )
        .unwrap();
    for (path, status, location) in [
        ("/module", StatusCode::OK, None),
        ("/open", StatusCode::OK, None),
        ("/old?q=%2F", StatusCode::TEMPORARY_REDIRECT, Some("/new/old?q=%2F")),
        ("/custom/item", StatusCode::PERMANENT_REDIRECT, Some("/fixed")),
    ] {
        let drops = Arc::new(AtomicUsize::new(0));
        let request = Request::builder()
            .uri(path)
            .body(claviron_http::boxed_body(Unread(drops.clone())))
            .unwrap();
        let response = owner.handle(request).now_or_never().unwrap().unwrap();
        assert_eq!(response.status(), status);
        assert_eq!(response.headers()["x-policy"], "reached");
        assert_eq!(
            response
                .headers()
                .get(http::header::LOCATION)
                .map(|value| value.to_str().unwrap()),
            location
        );
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }
}
