use super::*;
use crate::pipeline::host::{DispatchResult, HostContext, HostPipelineBuilder};
use crate::service::RequestArrival;
use claviron_http::ext::{ConnCtx, HostKey, HostName, RealIpExt};
use claviron_http::{ServiceResult, empty_body};
use futures_util::FutureExt;
use http::{Response, StatusCode};

fn connection() -> Arc<ConnCtx> {
    Arc::new(ConnCtx {
        state: Default::default(),
        listener: "public".parse().unwrap(),
        proto: Some(claviron_http::listener::HttpProtocol::H1),
        remote: "192.0.2.1:1234".parse().unwrap(),
        local_ip: "127.0.0.1".parse().unwrap(),
        local_port: 8080,
        real_ip: None,
        country_code: Some("GB".into()),
        tls: None,
    })
}

struct FileStep;
impl DispatchStep for FileStep {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> DispatchCall {
        assert!(request.extensions().is_empty());
        if request.uri().path() == "/asset" {
            DispatchCall::ready(ServiceResult::Handled(Response::new(
                request.into_body(),
            )))
        } else {
            DispatchCall::ready(ServiceResult::Unhandled(request))
        }
    }
}

struct First;
impl BackendHandler for First {
    fn reads_identity(&self) -> bool {
        true
    }
}
impl DispatchStep for First {
    fn call(
        &self,
        mut request: Request<RouteBody>,
    ) -> DispatchCall {
        assert_eq!(request.extensions().len(), 3);
        assert_eq!(
            request.extensions().get::<HttpPolicy>(),
            Some(&HttpPolicy::unlimited())
        );
        assert_eq!(
            request.extensions().get::<HostName>().unwrap().0.to_string(),
            "alias.test"
        );
        // An earlier backend may enrich the same unread request before yielding.
        request
            .extensions_mut()
            .insert(HostName(Arc::new("selected.test".parse().unwrap())));
        request
            .extensions_mut()
            .insert(RealIpExt("203.0.113.1".parse().unwrap()));
        DispatchCall::ready(ServiceResult::Unhandled(request))
    }
}

struct Last(Arc<ConnCtx>);
impl BackendHandler for Last {
    fn reads_identity(&self) -> bool {
        true
    }
}
impl DispatchStep for Last {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> DispatchCall {
        assert!(Arc::ptr_eq(
            request.extensions().get::<Arc<ConnCtx>>().unwrap(),
            &self.0
        ));
        assert_eq!(
            request.extensions().get::<HostName>().unwrap().0.to_string(),
            "selected.test"
        );
        assert_eq!(
            claviron_http::ext::request_real_ip(request.extensions()),
            Some("203.0.113.1".parse().unwrap())
        );
        assert!(self.0.real_ip.is_none());
        assert_eq!(self.0.country_code.as_deref(), Some("GB"));
        DispatchCall::ready(ServiceResult::Handled(Response::new(
            request.into_body(),
        )))
    }
}

#[test]
fn backend_identity_starts_after_native_file_steps_and_is_exported_only_once() {
    let connection = connection();
    let hostname = HostName(Arc::new("alias.test".parse().unwrap()));
    let key = HostKey(Arc::new("app.test".parse().unwrap()));
    let context = Some(
        HostContext::new(&connection, RequestArrival::Established)
            .with_host(&hostname, &key),
    );
    let mut builder = HostPipelineBuilder::<()>::new();
    builder.layer(Arc::new(FileStep) as Arc<dyn DispatchStep>).unwrap();
    builder.layer(Backend::new(First, HttpPolicy::unlimited())).unwrap();
    builder
        .layer(Backend::new(Last(connection.clone()), HttpPolicy::unlimited()))
        .unwrap();
    let owner = builder.build().into_execution();
    for path in ["/asset", "/application"] {
        let request = Request::builder().uri(path).body(empty_body()).unwrap();
        let response = owner
            .handle_in(request, context)
            .now_or_never()
            .expect("synchronous owners are ready")
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}

struct Untouched;
impl BackendHandler for Untouched {}
impl DispatchStep for Untouched {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> DispatchCall {
        // Nothing is written for an owner that declares no identity.
        assert!(request.extensions().is_empty());
        DispatchCall::ready(ServiceResult::Handled(Response::new(
            request.into_body(),
        )))
    }
}

#[test]
fn an_owner_that_reads_no_identity_receives_the_request_untouched() {
    let connection = connection();
    let hostname = HostName(Arc::new("alias.test".parse().unwrap()));
    let key = HostKey(Arc::new("app.test".parse().unwrap()));
    let context = Some(
        HostContext::new(&connection, RequestArrival::Established)
            .with_host(&hostname, &key),
    );
    let mut builder = HostPipelineBuilder::<()>::new();
    builder.layer(Backend::new(Untouched, HttpPolicy::unlimited())).unwrap();
    let owner = builder.build().into_execution();
    let request =
        Request::builder().uri("/application").body(empty_body()).unwrap();
    let response = owner
        .handle_in(request, context)
        .now_or_never()
        .expect("synchronous owners are ready")
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

struct Pending;
impl BackendHandler for Pending {
    fn reads_identity(&self) -> bool {
        true
    }
}
impl DispatchStep for Pending {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> DispatchCall {
        assert!(request.extensions().get::<Arc<ConnCtx>>().is_some());
        DispatchCall::new(async move {
            std::future::pending::<()>().await;
            DispatchResult::Unhandled(request)
        })
    }
}

#[test]
fn backend_identity_releases_its_connection_when_a_pending_call_is_cancelled() {
    use std::task::{Context, Poll, Waker};
    let connection = connection();
    let baseline = Arc::strong_count(&connection);
    let mut builder = HostPipelineBuilder::<()>::new();
    builder.layer(Backend::new(Pending, HttpPolicy::unlimited())).unwrap();
    let owner = builder.build().into_execution();
    let mut answering = owner.handle_in(
        Request::new(empty_body()),
        Some(HostContext::new(&connection, RequestArrival::Established)),
    );
    assert!(matches!(
        answering.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    ));
    assert_eq!(Arc::strong_count(&connection), baseline + 1);
    drop(answering);
    assert_eq!(Arc::strong_count(&connection), baseline);
}
