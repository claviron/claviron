use super::super::tests::{request, service_with_limits};
use super::*;
use crate::pipeline::bypass::BypassHandler;
use claviron_http::RouteFuture;
#[cfg(feature = "axum")]
use claviron_http::boxed_body;
#[cfg(any(feature = "tcp", feature = "axum"))]
use futures_util::FutureExt;
use http_body_util::BodyExt;
use std::convert::Infallible;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

#[derive(Default)]
struct State {
    ready: AtomicBool,
    entered: AtomicUsize,
    dropped: AtomicUsize,
}

struct Handler(Arc<State>);

struct Reply {
    request: Option<Request<RouteBody>>,
    state: Arc<State>,
}

impl Future for Reply {
    type Output = Result<RouteResponse, Infallible>;

    fn poll(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Self::Output> {
        if !self.state.ready.load(Ordering::Relaxed) {
            return Poll::Pending;
        }
        let request = self.request.take().unwrap();
        Poll::Ready(Ok(http::Response::builder()
            .status(StatusCode::CREATED)
            .body(request.into_body())
            .unwrap()))
    }
}

impl Drop for Reply {
    fn drop(&mut self) {
        self.state.dropped.fetch_add(1, Ordering::Relaxed);
    }
}

impl BypassHandler for Handler {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> RouteFuture {
        self.0.entered.fetch_add(1, Ordering::Relaxed);
        Box::pin(Reply { request: Some(request), state: self.0.clone() })
    }
}

fn apps(state: &Arc<State>) -> Vec<AppKind> {
    let apps =
        vec![AppKind::Bypass(BypassService::new(Handler(state.clone())))];
    #[cfg(feature = "axum")]
    let apps = {
        let mut apps = apps;
        let state = state.clone();
        apps.push(AppKind::Axum(axum::Router::new().fallback(
            move |request: Request<axum::body::Body>| {
                state.entered.fetch_add(1, Ordering::Relaxed);
                Reply {
                    request: Some(request.map(boxed_body)),
                    state: state.clone(),
                }
                .map(Result::unwrap)
            },
        )));
        apps
    };
    apps
}

fn prepared(
    app: AppKind,
    policy: RequestPolicy,
) -> PreparedRequest<RouteBody> {
    let service = service_with_limits(
        HttpProtocol::H1,
        "",
        HostApp { admission: Admission::limited(Some(1)), ..app.into() },
        policy,
    );
    prepare(&service)
}

fn dispatch(
    prepared: PreparedRequest<RouteBody>
) -> impl Future<Output = RouteResponse> {
    #[cfg(feature = "tcp")]
    {
        prepared.into_route_future().map(Result::unwrap)
    }
    #[cfg(not(feature = "tcp"))]
    {
        prepared.dispatch()
    }
}

fn prepare(service: &HostService) -> PreparedRequest<RouteBody> {
    service
        .prepare(request("/"), RequestArrival::Established)
        .unwrap()
        .map_body(|()| claviron_http::empty_body())
}

#[test]
fn host_dispatch_cancellation_before_poll_never_calls_the_owner() {
    let state = Arc::new(State::default());
    for app in apps(&state) {
        let prepared = prepared(app, RequestPolicy::unlimited());
        let generation = Arc::downgrade(&prepared.service.policy);
        let answering = prepared.dispatch();
        assert!(generation.upgrade().is_some());
        drop(answering);
        assert!(generation.upgrade().is_none());
    }
    assert_eq!(state.entered.load(Ordering::Relaxed), 0);
    assert_eq!(state.dropped.load(Ordering::Relaxed), 0);
}

#[test]
fn host_dispatch_cancellation_releases_admission_and_owned_work() {
    let mut cx = Context::from_waker(Waker::noop());
    let state = Arc::new(State::default());
    for app in apps(&state) {
        let service = service_with_limits(
            HttpProtocol::H1,
            "",
            HostApp { admission: Admission::limited(Some(1)), ..app.into() },
            RequestPolicy::unlimited(),
        );
        let entered = state.entered.load(Ordering::Relaxed);
        let dropped = state.dropped.load(Ordering::Relaxed);
        let mut first = Box::pin(dispatch(prepare(&service)));
        let mut queued = Box::pin(dispatch(prepare(&service)));
        assert!(first.as_mut().poll(&mut cx).is_pending());
        assert!(queued.as_mut().poll(&mut cx).is_pending());
        assert_eq!(state.entered.load(Ordering::Relaxed), entered + 1);
        assert_eq!(state.dropped.load(Ordering::Relaxed), dropped);
        // An accepted request stays owned while its reply is pending. Only
        // explicit cancellation releases it and lets the next caller enter.
        drop(first);
        assert_eq!(state.dropped.load(Ordering::Relaxed), dropped + 1);
        assert!(queued.as_mut().poll(&mut cx).is_pending());
        assert_eq!(state.entered.load(Ordering::Relaxed), entered + 2);
        drop(queued);
        assert_eq!(state.dropped.load(Ordering::Relaxed), dropped + 2);
    }
}

#[tokio::test(start_paused = true)]
async fn host_dispatch_declared_head_timeout_cancels_only_its_reply() {
    let state = Arc::new(State::default());
    let limit = std::time::Duration::from_secs(1);
    let mut cx = Context::from_waker(Waker::noop());
    for app in apps(&state) {
        let prepared = prepared(
            app,
            RequestPolicy {
                head_timeout: Some(limit),
                ..RequestPolicy::unlimited()
            },
        );
        let dropped = state.dropped.load(Ordering::Relaxed);
        let mut answering = Box::pin(dispatch(prepared));
        assert!(answering.as_mut().poll(&mut cx).is_pending());
        tokio::time::advance(limit).await;
        let Poll::Ready(response) = answering.as_mut().poll(&mut cx) else {
            panic!("the declared head deadline must end the reply");
        };
        assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(state.dropped.load(Ordering::Relaxed), dropped + 1);
        drop(answering);
        assert_eq!(state.dropped.load(Ordering::Relaxed), dropped + 1);
    }
}

#[tokio::test]
async fn host_dispatch_transfers_response_payload_and_retains_generation() {
    let state = Arc::new(State::default());
    static PAYLOAD: &[u8] = b"payload";
    for app in apps(&state) {
        state.ready.store(false, Ordering::Relaxed);
        let prepared = prepared(app, RequestPolicy::unlimited());
        let generation = Arc::downgrade(&prepared.service.policy);
        let prepared = prepared.map_body(|_| {
            claviron_http::bytes_body(bytes::Bytes::from_static(PAYLOAD))
        });
        let mut answering = Box::pin(dispatch(prepared));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(answering.as_mut().poll(&mut cx).is_pending());
        assert!(generation.upgrade().is_some());
        state.ready.store(true, Ordering::Relaxed);
        let Poll::Ready(response) = answering.as_mut().poll(&mut cx) else {
            panic!("the ready response must be forwarded");
        };
        assert_eq!(response.status(), StatusCode::CREATED);
        drop(answering);
        assert!(generation.upgrade().is_none());
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(bytes.as_ptr(), PAYLOAD.as_ptr());
        assert_eq!(bytes, PAYLOAD);
    }
}

#[cfg(feature = "tcp")]
#[test]
fn host_dispatch_tcp_boxes_only_the_selected_concrete_future() {
    let state = Arc::new(State::default());
    for app in apps(&state) {
        let pending = prepared(app.clone(), RequestPolicy::unlimited());
        let (owner, request) = pending.into_dispatch();
        let expected = match owner {
            #[cfg(feature = "axum")]
            AppKind::Axum(owner) => std::mem::size_of_val(
                &request.dispatch(owner).map(Ok::<_, Infallible>),
            ),
            AppKind::Bypass(owner) => std::mem::size_of_val(
                &request.dispatch(owner).map(Ok::<_, Infallible>),
            ),
        };
        let prepared = prepared(app, RequestPolicy::unlimited());
        let generation = Arc::downgrade(&prepared.service.policy);
        let future = prepared.into_route_future();
        assert_eq!(std::mem::size_of_val(future.as_ref().get_ref()), expected);
        drop(future);
        assert!(generation.upgrade().is_none());
    }
    assert_eq!(state.entered.load(Ordering::Relaxed), 0);
}
