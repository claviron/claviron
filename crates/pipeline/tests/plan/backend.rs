use super::*;
use crate::plan::DispatchCall;
use crate::plan::{DispatchResult, HostContext, HostPipelineBuilder};
use crate::{
    BackendContract, BackendHandler, BackendIdentity, BackendReply,
    DispatchStep,
};
use claviron_http::RouteBody;
use claviron_http::ext::RequestArrival;
use claviron_http::ext::{ConnCtx, HostKey, HostName, RealIpExt};
use claviron_http::policy::HttpPolicy;
use claviron_http::{ServiceResult, empty_body};
use futures_util::FutureExt;
use http::Request;
use http::{Response, StatusCode};
use std::sync::Arc;

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
        _context: Option<crate::plan::HostContext<'_>>,
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
    fn contract(&self) -> crate::BackendContract {
        crate::BackendContract {
            identity: crate::BackendIdentity::Request,
            reply: crate::BackendReply::Owner,
        }
    }
}
impl DispatchStep for First {
    fn call(
        &self,
        mut request: Request<RouteBody>,
        _context: Option<crate::plan::HostContext<'_>>,
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
    fn contract(&self) -> crate::BackendContract {
        crate::BackendContract {
            identity: crate::BackendIdentity::Request,
            reply: crate::BackendReply::Owner,
        }
    }
}
impl DispatchStep for Last {
    fn call(
        &self,
        request: Request<RouteBody>,
        _context: Option<crate::plan::HostContext<'_>>,
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
impl BackendHandler for Untouched {
    fn contract(&self) -> BackendContract {
        BackendContract {
            identity: BackendIdentity::None,
            reply: BackendReply::Owner,
        }
    }
}
impl DispatchStep for Untouched {
    fn call(
        &self,
        request: Request<RouteBody>,
        _context: Option<crate::plan::HostContext<'_>>,
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
    fn contract(&self) -> crate::BackendContract {
        crate::BackendContract {
            identity: crate::BackendIdentity::Request,
            reply: crate::BackendReply::Owner,
        }
    }
}
impl DispatchStep for Pending {
    fn call(
        &self,
        request: Request<RouteBody>,
        _context: Option<crate::plan::HostContext<'_>>,
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

struct External(tokio_util::sync::CancellationToken);

impl BackendHandler for External {
    fn contract(&self) -> BackendContract {
        BackendContract {
            identity: BackendIdentity::None,
            reply: BackendReply::ExternalEventStream(self.0.clone()),
        }
    }
}

impl DispatchStep for External {
    fn call(
        &self,
        request: Request<RouteBody>,
        _context: Option<crate::plan::HostContext<'_>>,
    ) -> DispatchCall {
        match request.uri().path() {
            "/miss" => DispatchCall::ready(ServiceResult::Unhandled(request)),
            "/finite" => DispatchCall::ready(ServiceResult::Handled(
                Response::new(request.into_body()),
            )),
            _ => {
                let response = Response::builder()
                    .header(http::header::CONTENT_TYPE, "text/event-stream")
                    .body(claviron_http::boxed_body(
                        http_body_util::StreamBody::new(
                            futures_util::stream::pending::<
                                Result<
                                    http_body::Frame<bytes::Bytes>,
                                    std::convert::Infallible,
                                >,
                            >(),
                        ),
                    ))
                    .unwrap();
                DispatchCall::new(std::future::ready(ServiceResult::Handled(
                    response,
                )))
            }
        }
    }
}

#[test]
fn backend_handoff_ends_only_the_selected_external_event_stream() {
    use http_body_util::BodyExt;
    let shutdown = tokio_util::sync::CancellationToken::new();
    let mut builder = HostPipelineBuilder::<()>::new();
    builder
        .layer(Backend::new(
            External(shutdown.clone()),
            HttpPolicy::unlimited(),
        ))
        .unwrap();
    builder.layer(Backend::new(Untouched, HttpPolicy::unlimited())).unwrap();
    let owner = builder.build().into_execution();
    let mut response = owner
        .handle(Request::new(empty_body()))
        .now_or_never()
        .unwrap()
        .unwrap();
    assert!(response.body_mut().frame().now_or_never().is_none());
    shutdown.cancel();
    assert!(response.body_mut().frame().now_or_never().unwrap().is_none());

    // Cancellation cannot turn a finite reply or an unread miss into an SSE end.
    for path in ["/finite", "/miss"] {
        let bytes = bytes::Bytes::from_static(b"unchanged");
        let pointer = bytes.as_ptr();
        let request = Request::builder()
            .uri(path)
            .body(claviron_http::bytes_body(bytes))
            .unwrap();
        let mut response =
            owner.handle(request).now_or_never().unwrap().unwrap();
        let frame = response
            .body_mut()
            .frame()
            .now_or_never()
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(frame.data_ref().unwrap().as_ptr(), pointer);
    }
}

/// An owner that reads its scope from the context and declares no export.
struct Borrower {
    policy: HttpPolicy,
    connection: Arc<ConnCtx>,
    last: bool,
}
impl BackendHandler for Borrower {
    fn contract(&self) -> crate::BackendContract {
        crate::BackendContract {
            identity: crate::BackendIdentity::None,
            reply: crate::BackendReply::Owner,
        }
    }
}
impl DispatchStep for Borrower {
    fn call(
        &self,
        request: Request<RouteBody>,
        context: Option<crate::plan::HostContext<'_>>,
    ) -> DispatchCall {
        let context = context.expect("an accepted request has a context");
        assert_eq!(context.policy(), Some(&self.policy));
        assert!(Arc::ptr_eq(context.connection, &self.connection));
        assert_eq!(context.hostname().unwrap().0.to_string(), "alias.test");
        assert!(request.extensions().is_empty());
        DispatchCall::ready(if self.last {
            ServiceResult::Handled(Response::new(request.into_body()))
        } else {
            ServiceResult::Unhandled(request)
        })
    }
}

#[test]
fn each_backend_is_lent_its_own_scope_policy_and_the_request_is_not_written() {
    let connection = connection();
    let hostname = HostName(Arc::new("alias.test".parse().unwrap()));
    let key = HostKey(Arc::new("app.test".parse().unwrap()));
    let context = Some(
        HostContext::new(&connection, RequestArrival::Established)
            .with_host(&hostname, &key),
    );
    let mut narrowed = HttpPolicy::unlimited();
    narrowed.requests.max_body_size = Some(1024);
    let mut builder = HostPipelineBuilder::<()>::new();
    for (policy, last) in [(narrowed, false), (HttpPolicy::unlimited(), true)] {
        builder
            .layer(Backend::new(
                Borrower { policy, connection: connection.clone(), last },
                policy,
            ))
            .unwrap();
    }
    let response = builder
        .build()
        .into_execution()
        .handle_in(Request::new(empty_body()), context)
        .now_or_never()
        .expect("synchronous owners are ready")
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}
