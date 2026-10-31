use std::collections::VecDeque;
use std::convert::Infallible;
use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use bytes::Bytes;
use claviron_http::{ServiceResult, boxed_body, empty_body};
use futures_util::task::AtomicWaker;
use http::{HeaderMap, HeaderValue, StatusCode};
use http_body::{Body, Frame};
use http_body_util::BodyExt;

use super::*;

struct Layer<F>(F);

impl<S, F> HostLayer<S> for Layer<F>
where
    F: FnOnce(&mut HostLayerBuilder<S>) -> crate::Result<()>,
{
    fn prepare(
        self,
        layer: &mut HostLayerBuilder<S>,
    ) -> crate::Result<()> {
        self.0(layer)
    }
}

fn layer<S, F>(prepare: F) -> Layer<F>
where
    F: FnOnce(&mut HostLayerBuilder<S>) -> crate::Result<()>,
{
    Layer(prepare)
}

fn ready<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    match future.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("fixture must be ready without a runtime"),
    }
}

fn unhandled<'p, B, S>(
    result: ServiceResult<PreparedReply<'p, S>, PreparedRequest<'p, B, S>>
) -> PreparedRequest<'p, B, S> {
    match result {
        ServiceResult::Unhandled(request) => request,
        ServiceResult::Handled(_) => panic!("request must remain unclaimed"),
    }
}

#[derive(Default)]
struct BodyCounts {
    polls: AtomicUsize,
    drops: AtomicUsize,
}

#[derive(Default)]
struct Gate {
    open: AtomicBool,
    waker: AtomicWaker,
}

#[derive(Default)]
struct WakeCounter(AtomicUsize);

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

impl Gate {
    fn release(&self) {
        self.open.store(true, Ordering::Release);
        self.waker.wake();
    }
}

// Deliberately no Clone implementation. Frames own their original Bytes.
struct RequestBody {
    frames: VecDeque<Frame<Bytes>>,
    counts: Arc<BodyCounts>,
    gate: Option<Arc<Gate>>,
}

impl Body for RequestBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        self.counts.polls.fetch_add(1, Ordering::Relaxed);
        if self.frames.len() == 2
            && let Some(gate) = &self.gate
            && !gate.open.load(Ordering::Acquire)
        {
            gate.waker.register(cx.waker());
            if !gate.open.load(Ordering::Acquire) {
                return Poll::Pending;
            }
        }
        Poll::Ready(self.frames.pop_front().map(Ok))
    }
}

impl Drop for RequestBody {
    fn drop(&mut self) {
        self.counts.drops.fetch_add(1, Ordering::Relaxed);
    }
}

fn request() -> (Request<RequestBody>, Arc<BodyCounts>, Vec<usize>) {
    let first = Bytes::from(vec![0, 255, 13, 10]);
    let second = Bytes::from(vec![128, 42, 0]);
    let addresses = vec![first.as_ptr() as usize, second.as_ptr() as usize];
    let mut trailers = HeaderMap::new();
    trailers.insert("x-example-end", HeaderValue::from_static("complete"));
    let counts = Arc::new(BodyCounts::default());
    let body = RequestBody {
        frames: VecDeque::from([
            Frame::data(first),
            Frame::data(second),
            Frame::trailers(trailers),
        ]),
        counts: counts.clone(),
        gate: None,
    };
    let request = Request::builder()
        .method("POST")
        .uri("/example?source=body")
        .header("x-example", "original")
        .body(body)
        .unwrap();
    (request, counts, addresses)
}

struct DropGuard(Arc<AtomicUsize>);

impl Drop for DropGuard {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn only_active_actions_are_bound_once() {
    let prepared = Arc::new(AtomicUsize::new(0));
    let mut builder = HostPipelineBuilder::<Vec<&str>>::new();
    for (enabled, name) in [(true, "first"), (false, "absent"), (true, "last")]
    {
        let prepared = prepared.clone();
        builder
            .layer(layer(move |actions: &mut HostLayerBuilder<Vec<&str>>| {
                prepared.fetch_add(1, Ordering::Relaxed);
                if enabled {
                    actions.before(move |_, state| {
                        state.push(name);
                        ControlFlow::Continue(())
                    });
                    actions.after(move |head, state| {
                        state.push(name);
                        head.headers.append(
                            "x-example-order",
                            HeaderValue::from_str(&state.join(",")).unwrap(),
                        );
                    });
                }
                Ok(())
            }))
            .unwrap();
    }
    let pipeline = builder.build();
    assert_eq!(prepared.load(Ordering::Relaxed), 3);
    assert_eq!(pipeline.before.len(), 2);
    assert_eq!(pipeline.after.len(), 2);
    for _ in 0..2 {
        let request = unhandled(
            pipeline.prepare_with_state(Request::new(()), Vec::new()),
        );
        let response =
            ready(request.dispatch(|_| async { Response::new(empty_body()) }));
        let order: Vec<_> =
            response.headers().get_all("x-example-order").iter().collect();
        assert_eq!(order, ["first,last,last", "first,last,last,first"]);
    }
    assert_eq!(prepared.load(Ordering::Relaxed), 3);
}

#[test]
fn failed_layer_leaves_no_partial_actions_or_captures() {
    let discarded = Arc::new(AtomicUsize::new(0));
    let captured = DropGuard(discarded.clone());
    let mut builder = HostPipelineBuilder::new();
    let failed = builder.layer(layer(move |actions| {
        actions.before(move |_, _: &mut ()| {
            let _capture = &captured;
            panic!("discarded action must never be called")
        });
        actions.after(|_, _| panic!("discarded reply action"));
        Err(crate::Error::Invalid("example invalid layer".into()))
    }));
    assert!(failed.is_err());
    assert_eq!(discarded.load(Ordering::Relaxed), 1);
    let pipeline = builder.build();
    assert!(pipeline.before.is_empty());
    assert!(pipeline.after.is_empty());
    let response = ready(
        unhandled(pipeline.prepare(Request::new(())))
            .dispatch(|_| async { Response::new(empty_body()) }),
    );
    assert_eq!(response.status(), StatusCode::OK);
}

#[test]
fn early_reply_runs_only_entered_response_scope() {
    let mut builder = HostPipelineBuilder::new();
    builder
        .layer(layer(|actions| {
            actions.after(|head, _: &mut ()| {
                head.headers.append(
                    "x-example-order",
                    HeaderValue::from_static("outer"),
                );
            });
            Ok(())
        }))
        .unwrap();
    builder
        .layer(layer(|actions| {
            actions.before(|_, _| {
                ControlFlow::Break(
                    Response::builder()
                        .status(StatusCode::FORBIDDEN)
                        .body(empty_body())
                        .unwrap(),
                )
            });
            actions.after(|head, _| {
                head.headers.append(
                    "x-example-order",
                    HeaderValue::from_static("reject"),
                );
            });
            actions.body(|body, _| body);
            Ok(())
        }))
        .unwrap();
    builder
        .layer(layer(|actions| {
            actions.before(|_, _| panic!("later head must not run"));
            actions.after(|_, _| panic!("later reply must not run"));
            actions.body(|_, _| panic!("later body wrapper must not run"));
            Ok(())
        }))
        .unwrap();
    let pipeline = builder.build();
    let (request, counts, _) = request();
    let ServiceResult::Handled(reply) = pipeline.prepare(request) else {
        panic!("layer must reply early")
    };
    assert_eq!(counts.polls.load(Ordering::Relaxed), 0);
    assert_eq!(counts.drops.load(Ordering::Relaxed), 1);
    let response = reply.finish();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let order: Vec<_> =
        response.headers().get_all("x-example-order").iter().collect();
    assert_eq!(order, ["reject", "outer"]);
}

#[test]
fn later_response_only_layer_does_not_decorate_earlier_rejection() {
    let mut builder = HostPipelineBuilder::new();
    builder
        .layer(layer(|actions| {
            actions.before(|_, _: &mut ()| {
                ControlFlow::Break(Response::new(empty_body()))
            });
            Ok(())
        }))
        .unwrap();
    builder
        .layer(layer(|actions| {
            actions.after(|_, _| {
                panic!("later response-only layer is not entered")
            });
            actions.body(|_, _| panic!("later body-only layer is not entered"));
            Ok(())
        }))
        .unwrap();
    let pipeline = builder.build();
    let ServiceResult::Handled(reply) = pipeline.prepare(Request::new(()))
    else {
        panic!("expected early reply")
    };
    assert_eq!(reply.finish().status(), StatusCode::OK);
}

#[test]
fn unhandled_body_reaches_terminal_once_with_original_buffers_and_trailers() {
    let mut builder = HostPipelineBuilder::new();
    builder
        .layer(layer(|actions| {
            actions.before(|head, _: &mut ()| {
                assert_eq!(head.uri.path(), "/example");
                head.extensions.insert(17_u32);
                ControlFlow::Continue(())
            });
            Ok(())
        }))
        .unwrap();
    let pipeline = builder.build();
    let (request, counts, addresses) = request();
    let prepared = unhandled(pipeline.prepare(request));
    assert_eq!(counts.polls.load(Ordering::Relaxed), 0);
    assert_eq!(prepared.head().extensions.get::<u32>(), Some(&17));
    let prepared = prepared.map_body(|body| body);
    let checked = counts.clone();
    let response = ready(prepared.dispatch(move |request| async move {
        assert_eq!(request.headers()["x-example"], "original");
        assert_eq!(request.uri().query(), Some("source=body"));
        let mut body = request.into_body();
        for address in addresses {
            let bytes =
                body.frame().await.unwrap().unwrap().into_data().unwrap();
            assert_eq!(bytes.as_ptr() as usize, address);
        }
        let trailers =
            body.frame().await.unwrap().unwrap().into_trailers().unwrap();
        assert_eq!(trailers["x-example-end"], "complete");
        assert!(body.frame().await.is_none());
        assert_eq!(checked.polls.load(Ordering::Relaxed), 4);
        Response::new(empty_body())
    }));
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(counts.drops.load(Ordering::Relaxed), 1);
}

#[test]
fn dispatch_forwards_erased_stream_without_polling_its_frames() {
    let pipeline = HostPipelineBuilder::new().build();
    let (request, counts, addresses) = request();
    let request = request.map(boxed_body);
    let mut response = ready(
        unhandled(pipeline.prepare(request))
            .dispatch(|request| async { Response::new(request.into_body()) }),
    );
    assert_eq!(counts.polls.load(Ordering::Relaxed), 0);
    assert_eq!(counts.drops.load(Ordering::Relaxed), 0);
    for address in addresses {
        let frame = ready(response.body_mut().frame()).unwrap().unwrap();
        assert_eq!(frame.data_ref().unwrap().as_ptr() as usize, address);
    }
    assert!(ready(response.body_mut().frame()).unwrap().unwrap().is_trailers());
    assert!(ready(response.body_mut().frame()).is_none());
    drop(response);
    assert_eq!(counts.drops.load(Ordering::Relaxed), 1);
}

#[test]
fn dropping_unhandled_request_releases_body_and_typed_state_once() {
    let pipeline = HostPipelineBuilder::<DropGuard>::new().build();
    let (request, counts, _) = request();
    let dropped = Arc::new(AtomicUsize::new(0));
    let prepared = unhandled(
        pipeline.prepare_with_state(request, DropGuard(dropped.clone())),
    );
    assert_eq!(dropped.load(Ordering::Relaxed), 0);
    drop(prepared);
    assert_eq!(counts.polls.load(Ordering::Relaxed), 0);
    assert_eq!(counts.drops.load(Ordering::Relaxed), 1);
    assert_eq!(dropped.load(Ordering::Relaxed), 1);
}

#[test]
fn cancelling_before_first_poll_never_starts_handler_or_response_actions() {
    let mut builder = HostPipelineBuilder::<DropGuard>::new();
    builder
        .layer(layer(|actions| {
            actions.after(|_, _| panic!("cancelled request has no reply"));
            Ok(())
        }))
        .unwrap();
    let pipeline = builder.build();
    let (request, counts, _) = request();
    let dropped = Arc::new(AtomicUsize::new(0));
    let prepared = unhandled(
        pipeline.prepare_with_state(request, DropGuard(dropped.clone())),
    );
    let future = prepared.dispatch(|_| -> std::future::Ready<RouteResponse> {
        panic!("handler must not start before first poll")
    });
    drop(future);
    assert_eq!(counts.polls.load(Ordering::Relaxed), 0);
    assert_eq!(counts.drops.load(Ordering::Relaxed), 1);
    assert_eq!(dropped.load(Ordering::Relaxed), 1);
}

#[test]
fn cancelling_pending_handler_drops_request_and_state_once() {
    let pipeline = HostPipelineBuilder::<DropGuard>::new().build();
    let (request, counts, _) = request();
    let dropped = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(AtomicUsize::new(0));
    let observed = entered.clone();
    let prepared = unhandled(
        pipeline.prepare_with_state(request, DropGuard(dropped.clone())),
    );
    let mut future = Box::pin(prepared.dispatch(move |request| {
        entered.fetch_add(1, Ordering::Relaxed);
        async move {
            futures_util::future::pending::<()>().await;
            drop(request);
            Response::new(empty_body())
        }
    }));
    assert!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert_eq!(observed.load(Ordering::Relaxed), 1);
    assert_eq!(dropped.load(Ordering::Relaxed), 0);
    drop(future);
    assert_eq!(counts.polls.load(Ordering::Relaxed), 0);
    assert_eq!(counts.drops.load(Ordering::Relaxed), 1);
    assert_eq!(dropped.load(Ordering::Relaxed), 1);
}

#[test]
fn cancellation_while_reading_body_releases_its_single_owner() {
    let pipeline = HostPipelineBuilder::<DropGuard>::new().build();
    let (mut request, counts, _) = request();
    request.body_mut().gate = Some(Arc::new(Gate::default()));
    let dropped = Arc::new(AtomicUsize::new(0));
    let prepared = unhandled(
        pipeline.prepare_with_state(request, DropGuard(dropped.clone())),
    );
    let mut future = Box::pin(prepared.dispatch(|request| async move {
        let mut body = request.into_body();
        assert!(body.frame().await.unwrap().unwrap().is_data());
        let _ = body.frame().await;
        Response::new(empty_body())
    }));
    assert!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert_eq!(counts.polls.load(Ordering::Relaxed), 2);
    assert_eq!(counts.drops.load(Ordering::Relaxed), 0);
    drop(future);
    assert_eq!(counts.drops.load(Ordering::Relaxed), 1);
    assert_eq!(dropped.load(Ordering::Relaxed), 1);
}

#[test]
fn body_wait_resumes_only_when_original_source_wakes_it() {
    let pipeline = HostPipelineBuilder::new().build();
    let (mut request, counts, _) = request();
    let gate = Arc::new(Gate::default());
    request.body_mut().gate = Some(gate.clone());
    let mut future = Box::pin(unhandled(pipeline.prepare(request)).dispatch(
        |request| async move {
            let mut body = request.into_body();
            while let Some(frame) = body.frame().await {
                frame.unwrap();
            }
            Response::new(empty_body())
        },
    ));
    let awakened = Arc::new(WakeCounter::default());
    let waker = Waker::from(awakened.clone());
    let mut context = Context::from_waker(&waker);
    assert!(future.as_mut().poll(&mut context).is_pending());
    assert_eq!(counts.polls.load(Ordering::Relaxed), 2);
    assert_eq!(awakened.0.load(Ordering::Relaxed), 0);
    gate.release();
    assert_eq!(awakened.0.load(Ordering::Relaxed), 1);
    assert!(future.as_mut().poll(&mut context).is_ready());
    assert_eq!(counts.polls.load(Ordering::Relaxed), 5);
    assert_eq!(counts.drops.load(Ordering::Relaxed), 1);
}

struct ResponseBody {
    inner: RouteBody,
    guard: Option<DropGuard>,
}

impl Body for ResponseBody {
    type Data = Bytes;
    type Error = claviron_http::RouteBodyError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let this = self.get_mut();
        let frame = Pin::new(&mut this.inner).poll_frame(cx);
        if matches!(frame, Poll::Ready(None | Some(Err(_)))) {
            this.guard.take();
        }
        frame
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

#[test]
fn response_body_keeps_moved_guard_until_completion_or_cancellation() {
    let mut builder = HostPipelineBuilder::<Option<DropGuard>>::new();
    builder
        .layer(layer(|actions: &mut HostLayerBuilder<Option<DropGuard>>| {
            actions.body(|body, state| {
                ResponseBody { inner: body, guard: state.take() }.boxed_unsync()
            });
            Ok(())
        }))
        .unwrap();
    let pipeline = builder.build();
    for consume in [false, true] {
        let (request, counts, addresses) = request();
        let dropped = Arc::new(AtomicUsize::new(0));
        let prepared = unhandled(
            pipeline
                .prepare_with_state(request, Some(DropGuard(dropped.clone()))),
        );
        let mut response = ready(prepared.dispatch(|request| async {
            Response::new(boxed_body(request.into_body()))
        }));
        assert_eq!(dropped.load(Ordering::Relaxed), 0);
        let first = ready(response.body_mut().frame()).unwrap().unwrap();
        assert_eq!(first.data_ref().unwrap().as_ptr() as usize, addresses[0]);
        if consume {
            while let Some(frame) = ready(response.body_mut().frame()) {
                frame.unwrap();
            }
        }
        assert_eq!(dropped.load(Ordering::Relaxed), usize::from(consume));
        drop(response);
        assert_eq!(counts.drops.load(Ordering::Relaxed), 1);
        assert_eq!(dropped.load(Ordering::Relaxed), 1);
    }
}

#[test]
fn streaming_body_errors_reach_the_response_consumer() {
    let pipeline = HostPipelineBuilder::new().build();
    let body =
        http_body_util::StreamBody::new(futures_util::stream::iter([Err::<
            Frame<Bytes>,
            _,
        >(
            std::io::Error::other("example stream failure"),
        )]));
    let mut response =
        ready(unhandled(pipeline.prepare(Request::new(body))).dispatch(
            |request| async { Response::new(boxed_body(request.into_body())) },
        ));
    let error = ready(response.body_mut().frame()).unwrap().unwrap_err();
    assert_eq!(error.to_string(), "example stream failure");
}

#[test]
fn interleaved_requests_share_plan_but_not_typed_state() {
    let mut builder = HostPipelineBuilder::<usize>::new();
    builder
        .layer(layer(|actions: &mut HostLayerBuilder<usize>| {
            actions.before(|head, state| {
                *state += 1;
                head.extensions.insert(*state);
                ControlFlow::Continue(())
            });
            actions.after(|head, state| {
                head.headers.insert(
                    "x-example-state",
                    HeaderValue::from_str(&state.to_string()).unwrap(),
                );
            });
            Ok(())
        }))
        .unwrap();
    let pipeline = builder.build();
    let first = unhandled(pipeline.prepare_with_state(Request::new(()), 10));
    let second = unhandled(pipeline.prepare_with_state(Request::new(()), 20));
    assert_eq!(first.head().extensions.get::<usize>(), Some(&11));
    assert_eq!(second.head().extensions.get::<usize>(), Some(&21));
    let reply = |_: Request<()>| async { Response::new(empty_body()) };
    let second = ready(second.dispatch(reply));
    let first = ready(first.dispatch(reply));
    assert_eq!(first.headers()["x-example-state"], "11");
    assert_eq!(second.headers()["x-example-state"], "21");
}

#[test]
fn dropping_early_reply_releases_state_without_running_response_actions() {
    let mut builder = HostPipelineBuilder::<DropGuard>::new();
    builder
        .layer(layer(|actions| {
            actions
                .before(|_, _| ControlFlow::Break(Response::new(empty_body())));
            actions.after(|_, _| panic!("reply was abandoned, not finished"));
            Ok(())
        }))
        .unwrap();
    let pipeline = builder.build();
    let (request, counts, _) = request();
    let dropped = Arc::new(AtomicUsize::new(0));
    let ServiceResult::Handled(reply) =
        pipeline.prepare_with_state(request, DropGuard(dropped.clone()))
    else {
        panic!("expected early reply")
    };
    assert_eq!(counts.drops.load(Ordering::Relaxed), 1);
    assert_eq!(dropped.load(Ordering::Relaxed), 0);
    drop(reply);
    assert_eq!(dropped.load(Ordering::Relaxed), 1);
}
