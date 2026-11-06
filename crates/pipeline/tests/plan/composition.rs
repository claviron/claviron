use super::*;

#[test]
fn appended_pipeline_retains_early_and_complete_reply_scopes() {
    let tagged = |name| {
        layer(move |layer: &mut HostLayerBuilder| {
            layer.after(move |head, _| {
                head.headers
                    .append("x-example-order", HeaderValue::from_static(name));
            });
            Ok(())
        })
    };
    let mut inner = HostPipelineBuilder::new();
    inner
        .layer(layer(|layer: &mut HostLayerBuilder| {
            layer.before(|head, _| {
                if head.uri.path() == "/early" {
                    ControlFlow::Break(Response::new(empty_body()))
                } else {
                    ControlFlow::Continue(())
                }
            });
            layer.after(|head, _| {
                head.headers.append(
                    "x-example-order",
                    HeaderValue::from_static("inner"),
                );
            });
            Ok(())
        }))
        .unwrap();
    inner.layer(tagged("inner-tail")).unwrap();
    let mut outer = HostPipelineBuilder::new();
    outer.layer(tagged("outer")).unwrap();
    outer.append(inner.build());
    outer
        .layer(layer(|layer: &mut HostLayerBuilder| {
            layer.before(|_, _| ControlFlow::Continue(()));
            layer.after(|head, _| {
                head.headers
                    .append("x-example-order", HeaderValue::from_static("end"));
            });
            Ok(())
        }))
        .unwrap();
    let pipeline = outer.build();
    for (path, expected) in [
        ("/early", vec!["inner", "outer"]),
        ("/complete", vec!["end", "inner-tail", "inner", "outer"]),
    ] {
        let request = Request::builder().uri(path).body(empty_body()).unwrap();
        let response = match pipeline.prepare(request) {
            ServiceResult::Handled(reply) => reply.finish(),
            ServiceResult::Unhandled(request) => ready(
                request.dispatch(|_| async { Response::new(empty_body()) }),
            ),
        };
        assert_eq!(
            response
                .headers()
                .get_all("x-example-order")
                .iter()
                .collect::<Vec<_>>(),
            expected
        );
    }
}

#[test]
fn appended_pipeline_keeps_original_frames_trailers_and_typed_state() {
    let mut inner = HostPipelineBuilder::<Vec<&'static str>>::new();
    inner
        .layer(layer(|layer: &mut HostLayerBuilder<Vec<&'static str>>| {
            layer.before(|_, state| {
                state.push("inner");
                ControlFlow::Continue(())
            });
            Ok(())
        }))
        .unwrap();
    let mut outer = HostPipelineBuilder::new();
    outer
        .layer(layer(|layer: &mut HostLayerBuilder<Vec<&'static str>>| {
            layer.before(|_, state| {
                state.push("outer");
                ControlFlow::Continue(())
            });
            layer.after(|head, state| {
                assert_eq!(*state, ["outer", "inner"]);
                head.headers.insert(
                    "x-example-state",
                    HeaderValue::from_static("same"),
                );
            });
            Ok(())
        }))
        .unwrap();
    outer.append(inner.build());
    let pipeline = outer.build();
    let (request, counts, addresses) = request();
    let request = unhandled(pipeline.prepare_with_state(request, Vec::new()));
    assert_eq!(counts.polls.load(Ordering::Relaxed), 0);
    let response = ready(request.map_body(boxed_body).dispatch(
        move |request| async move {
            let mut body = request.into_body();
            for address in addresses {
                let data =
                    body.frame().await.unwrap().unwrap().into_data().unwrap();
                assert_eq!(data.as_ptr() as usize, address);
            }
            let trailers =
                body.frame().await.unwrap().unwrap().into_trailers().unwrap();
            assert_eq!(trailers["x-example-end"], "complete");
            assert!(body.frame().await.is_none());
            Response::new(empty_body())
        },
    ));
    assert_eq!(response.headers()["x-example-state"], "same");
    assert_eq!(counts.drops.load(Ordering::Relaxed), 1);
}

#[test]
fn appended_identity_is_exported_once_with_the_first_policy() {
    let mut first_policy = claviron_http::policy::HttpPolicy::unlimited();
    first_policy.requests.max_body_size = Some(1024);
    let mut later_policy = first_policy;
    later_policy.requests.max_body_size = Some(512);
    let mut first = HostPipelineBuilder::new();
    first.layer(crate::layers::IdentityPolicy::new(first_policy)).unwrap();
    let mut later = HostPipelineBuilder::new();
    later.layer(crate::layers::IdentityPolicy::new(later_policy)).unwrap();
    first.append(later.build());
    let pipeline = first.build();
    assert_eq!(pipeline.before.len(), 1);
    let request = unhandled(pipeline.prepare(Request::new(empty_body())));
    assert_eq!(
        request
            .head()
            .extensions
            .get::<claviron_http::policy::HttpPolicy>()
            .unwrap()
            .requests
            .max_body_size,
        Some(1024)
    );
}

#[test]
fn cancelling_appended_pending_owner_drops_body_state_and_deferred_reply() {
    struct Capture(Arc<AtomicUsize>);
    impl ExchangeAction for Capture {
        fn call<'a>(
            &'a self,
            _: &'a mut RequestParts,
            reply: &'a mut ReplyHooks,
        ) -> Pin<Box<dyn Future<Output = ControlFlow<RouteResponse>> + Send + 'a>>
        {
            let guard = DropGuard(self.0.clone());
            reply.headers(move |_| {
                drop(guard);
                panic!("cancelled reply must not run");
            });
            Box::pin(std::future::ready(ControlFlow::Continue(())))
        }
    }
    let deferred = Arc::new(AtomicUsize::new(0));
    let state_drops = Arc::new(AtomicUsize::new(0));
    let mut inner = HostPipelineBuilder::<DropGuard>::new();
    let captured = deferred.clone();
    inner
        .layer(layer(move |layer: &mut HostLayerBuilder<DropGuard>| {
            layer.exchange(Capture(captured));
            layer.request(|request| {
                DispatchCall::new(async move {
                    std::future::pending::<()>().await;
                    ServiceResult::Unhandled(request)
                })
            });
            Ok(())
        }))
        .unwrap();
    let mut outer = HostPipelineBuilder::new();
    outer.append(inner.build());
    let pipeline = outer.build();
    let (request, counts, _) = request();
    let request = unhandled(
        pipeline.prepare_with_state(request, DropGuard(state_drops.clone())),
    );
    let mut work = Box::pin(request.map_body(boxed_body).dispatch(|_| async {
        panic!("pending owner must retain the request")
    }));
    assert!(
        work.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert_eq!(counts.polls.load(Ordering::Relaxed), 0);
    drop(work);
    assert_eq!(counts.drops.load(Ordering::Relaxed), 1);
    assert_eq!(state_drops.load(Ordering::Relaxed), 1);
    assert_eq!(deferred.load(Ordering::Relaxed), 1);
}
