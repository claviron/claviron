//! What answers a request: the application's Rust routes in front, the owner
//! behind them, and the runner's own layers around both in one fixed order.
//!
//! **Whoever is the fallback is the owner.** [`register_router`] makes the
//! owner the router's fallback and its method-not-allowed fallback, so Rust
//! routes only intercept and the application stays the authority on what
//! exists. [`register_router_with`] hands the owner to the application to
//! place itself; one that places it nowhere has made its Rust service the
//! owner.
//!
//! Every ingress calls the one [`Application`] this composes, so an
//! application never learns which door a request came through.

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use claviron_http::admission::{Admission, head_within};
use claviron_http::body::{GuardedBody, declared_length_within, guard_reply};
use claviron_http::policy::HttpPolicy;
use claviron_http::{RouteBody, RouteBodyError, empty_body};
use claviron_runtime::{AppState, registry};
use http::{Method, Request, Response, StatusCode};
use http_body_util::BodyExt;
use tower::Service;
use tower::util::{BoxCloneSyncService, ServiceExt};

/// The request every ingress hands over and the owner receives.
pub type AppRequest = Request<RequestBody>;

/// A request body as an ingress hands it over, read as it arrives.
///
/// Its own type rather than [`RouteBody`] itself: with the boxed error's
/// lifetime elided inside a request type, a service future that captures one
/// fails the compiler's `Send` and `Service` checks for every lifetime at
/// once. A name says what it is besides.
pub struct RequestBody(RouteBody);

impl RequestBody {
    /// Box any body.
    pub fn new<B>(body: B) -> Self
    where
        B: http_body::Body<Data = Bytes> + Send + 'static,
        B::Error: Into<RouteBodyError>,
    {
        Self(route_body(body))
    }

    /// A request with no body.
    pub fn empty() -> Self {
        Self::new(http_body_util::Empty::<Bytes>::new())
    }
}

impl http_body::Body for RequestBody {
    type Data = Bytes;
    type Error = RouteBodyError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, RouteBodyError>>> {
        Pin::new(&mut self.0).poll_frame(context)
    }

    fn is_end_stream(&self) -> bool {
        self.0.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.0.size_hint()
    }
}

/// What the composed service answers with.
pub type AppResponse = Response<RouteBody>;

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// Any body, as the shell carries it; its error as text when it is not
/// already one the shell can carry.
fn route_body<B>(body: B) -> RouteBody
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<RouteBodyError>,
{
    body.map_err(Into::into).boxed_unsync()
}

/// The owner: the language's service — embedded PHP, the ASGI engine — as the
/// shell holds it. A runner registers it before the application's `configure`
/// runs; its engine boots before any ingress does, so a service whose inside
/// is filled at boot is what a runner hands over.
#[derive(Clone)]
pub struct Owner {
    service: BoxCloneSyncService<AppRequest, AppResponse, Infallible>,
    upgrades: Option<BoxCloneSyncService<AppRequest, AppResponse, Infallible>>,
    orbit_sessions: Option<crate::orbit::OrbitSession>,
}

impl Owner {
    pub fn new<S, B>(service: S) -> Self
    where
        S: Service<AppRequest, Response = Response<B>, Error = Infallible>
            + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
        B: http_body::Body<Data = Bytes> + Send + 'static,
        B::Error: Into<RouteBodyError>,
    {
        Self { service: boxed(service), upgrades: None, orbit_sessions: None }
    }

    /// Requests asking to take the connection over (`Upgrade:`), which the
    /// owner answers itself — an ASGI WebSocket. The request keeps the
    /// socket's upgrade handle in its extensions. Without this, an upgrade
    /// nothing else claims is refused.
    pub fn upgrades<S, B>(
        mut self,
        service: S,
    ) -> Self
    where
        S: Service<AppRequest, Response = Response<B>, Error = Infallible>
            + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
        B: http_body::Body<Data = Bytes> + Send + 'static,
        B::Error: Into<RouteBodyError>,
    {
        self.upgrades = Some(boxed(service));
        self
    }

    /// WebSockets over Orbit, which the edge speaks: the owner answers the
    /// handshake and exchanges whole messages over the slot. Without this, the
    /// edge is answered 501.
    pub fn orbit_sessions<F, Fut>(
        mut self,
        session: F,
    ) -> Self
    where
        F: Fn(
                AppRequest,
                crate::orbit::Endpoint,
                crate::orbit::Handshake,
            ) -> Fut
            + Send
            + Sync
            + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.orbit_sessions =
            Some(Arc::new(move |request, endpoint, handshake| {
                Box::pin(session(request, endpoint, handshake))
            }));
        self
    }

    pub(crate) fn orbit_session(&self) -> Option<crate::orbit::OrbitSession> {
        self.orbit_sessions.clone()
    }

    /// Hand a request to the owner and wait for its answer: the `Service`
    /// implementation as a plain method, so a closure reaches it without
    /// importing a trait.
    pub async fn handle<B>(
        &self,
        request: Request<B>,
    ) -> AppResponse
    where
        B: http_body::Body<Data = Bytes> + Send + 'static,
        B::Error: Into<RouteBodyError>,
    {
        let Ok(response) =
            self.service.clone().oneshot(request.map(RequestBody::new)).await;
        response
    }
}

impl<B> Service<Request<B>> for Owner
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<RouteBodyError>,
{
    type Response = AppResponse;
    type Error = Infallible;
    type Future = BoxFuture<Result<AppResponse, Infallible>>;

    fn poll_ready(
        &mut self,
        _: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(
        &mut self,
        request: Request<B>,
    ) -> Self::Future {
        let owner = self.clone();
        Box::pin(async move { Ok(owner.handle(request).await) })
    }
}

fn boxed<S, B>(
    service: S
) -> BoxCloneSyncService<AppRequest, AppResponse, Infallible>
where
    S: Service<AppRequest, Response = Response<B>, Error = Infallible>
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send + 'static,
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<RouteBodyError>,
{
    BoxCloneSyncService::new(
        service.map_response(|response| response.map(route_body)),
    )
}

/// Register the owner. A runner does this before the application's
/// `configure`, which composes in front of it.
pub fn register_owner(
    state: &AppState,
    owner: Owner,
) -> registry::Result<()> {
    state.registry_ref().insert(Arc::new(owner)).map(|_| ())
}

fn owner(state: &AppState) -> registry::Result<Owner> {
    state
        .registry_ref()
        .resolve::<Owner>()
        .map(|owner| (*owner).clone())
        .ok_or_else(|| {
            registry::Error::msg(
                "the runner registers its owner before configure",
            )
        })
}

/// The runner's `routing:` section and the guards before it: what stands in
/// front of the application's routes. Registered by the runner before
/// `configure`.
pub struct Routing {
    policy: HttpPolicy,
    admission: Admission,
    #[cfg(feature = "files")]
    files: Option<Arc<crate::files::Files>>,
}

impl Routing {
    /// `policy` bounds every request and reply; `HttpPolicy::unlimited()` is
    /// the explicit choice of none. `max_concurrent_requests` bounds requests
    /// until their response head; `None` is no bound.
    pub fn new(
        policy: HttpPolicy,
        max_concurrent_requests: Option<usize>,
    ) -> Self {
        Self {
            policy,
            admission: Admission::limited(max_concurrent_requests),
            #[cfg(feature = "files")]
            files: None,
        }
    }

    /// Static mounts, answered before any route.
    #[cfg(feature = "files")]
    pub fn files(
        mut self,
        files: Option<Arc<crate::files::Files>>,
    ) -> Self {
        self.files = files;
        self
    }
}

/// The service this application answers with, as every ingress calls it:
/// the guards, then what the application composed.
pub struct Application {
    service: Arc<dyn ErasedService>,
    #[cfg(feature = "broadcast")]
    broadcast: Option<Arc<crate::broadcast::Broadcast>>,
    upgrades: Option<BoxCloneSyncService<AppRequest, AppResponse, Infallible>>,
    policy: HttpPolicy,
    admission: Admission,
}

impl Application {
    /// The broadcast service's place in front of the owner, when present.
    #[cfg(feature = "broadcast")]
    pub(crate) fn broadcast(&self) -> Option<&crate::broadcast::Broadcast> {
        self.broadcast.as_deref()
    }

    /// Hand a request to whatever the application composed.
    ///
    /// Every ingress arrives here, which is why the exchange is counted and
    /// guarded here rather than in each of them. Claviron's edge counts at its own single
    /// fork, so the number means the same on both sides of the transport:
    /// work this process has taken and not yet answered.
    pub fn call(
        &self,
        request: AppRequest,
    ) -> BoxFuture<AppResponse> {
        let exchange = claviron_runtime::activity::ACTIVITY.begin();
        let policy = self.policy;
        let admission = self.admission.clone();
        let service = Arc::clone(&self.service);
        // An upgrade the broadcast service claims goes to it, in front of the
        // owner; any other is the owner's to claim.
        #[cfg(feature = "broadcast")]
        let broadcasting = self
            .broadcast
            .as_ref()
            .is_some_and(|broadcast| broadcast.accepts(&request));
        #[cfg(not(feature = "broadcast"))]
        let broadcasting = false;
        let upgrades = self.upgrades.clone();
        Box::pin(async move {
            let response = if broadcasting {
                service.call_erased(request).await
            } else {
                guarded(policy, admission, service, upgrades, request).await
            };
            drop(exchange);
            response
        })
    }
}

/// Refuse what no stage may read differently or take over, bound what is
/// admitted, and hold the request and its reply to the policy.
async fn guarded(
    policy: HttpPolicy,
    admission: Admission,
    service: Arc<dyn ErasedService>,
    upgrades: Option<BoxCloneSyncService<AppRequest, AppResponse, Infallible>>,
    mut request: AppRequest,
) -> AppResponse {
    // Framing two parsers could read differently, refused before anything
    // reads it as a request.
    if claviron_http::forwarding::reject_request_smuggling(
        request.version(),
        request.headers(),
    )
    .is_err()
    {
        return status(StatusCode::BAD_REQUEST);
    }
    // A connection taken over is the owner's to claim; its body is the
    // connection itself, so no body guard applies. Unclaimed, it is refused,
    // and so is a tunnel.
    if request.method() == Method::CONNECT {
        return status(StatusCode::NOT_IMPLEMENTED);
    }
    if request.headers().contains_key(http::header::UPGRADE) {
        return match upgrades {
            Some(upgrades) => {
                let Ok(response) = upgrades.oneshot(request).await;
                response
            }
            None => status(StatusCode::NOT_IMPLEMENTED),
        };
    }
    if declared_length_within(request.headers(), policy.requests.max_body_size)
        .is_err()
    {
        return status(StatusCode::PAYLOAD_TOO_LARGE);
    }
    request.extensions_mut().insert(policy);
    let request = request.map(|body| {
        RequestBody::new(GuardedBody::new(
            body,
            policy.requests.max_body_size,
            policy.requests.body_read_timeout,
        ))
    });
    let Some(_place) = admission.enter(policy.requests.queue_timeout).await
    else {
        return status(StatusCode::SERVICE_UNAVAILABLE);
    };
    match head_within(
        policy.requests.head_timeout,
        service.call_erased(request),
    )
    .await
    {
        Some(response) => response.map(|body| {
            guard_reply(
                body,
                policy.responses.max_body_size,
                policy.responses.body_idle_timeout,
            )
        }),
        None => status(StatusCode::GATEWAY_TIMEOUT),
    }
}

fn status(code: StatusCode) -> AppResponse {
    let mut response = Response::new(empty_body());
    *response.status_mut() = code;
    response
}

trait ErasedService: Send + Sync + 'static {
    fn call_erased(
        &self,
        request: AppRequest,
    ) -> BoxFuture<AppResponse>;
}

struct Erased<S>(S);

impl<S, B> ErasedService for Erased<S>
where
    S: Service<AppRequest, Response = Response<B>, Error = Infallible>
        + Clone
        + Send
        + Sync,
    S: 'static,
    S::Future: Send + 'static,
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<RouteBodyError>,
{
    fn call_erased(
        &self,
        request: AppRequest,
    ) -> BoxFuture<AppResponse> {
        let service = self.0.clone();
        Box::pin(async move {
            let Ok(response) = service.oneshot(request).await;
            response.map(route_body)
        })
    }
}

/// The routing order around what the application composed: its front
/// layers, then the service itself.
fn compose<S, B>(
    state: &AppState,
    service: S,
) -> registry::Result<()>
where
    S: Service<AppRequest, Response = Response<B>, Error = Infallible>
        + Clone
        + Send
        + Sync,
    S: 'static,
    S::Future: Send + 'static,
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<RouteBodyError>,
{
    let routing =
        state.registry_ref().resolve::<Routing>().ok_or_else(|| {
            registry::Error::msg(
                "the runner registers Routing before configure",
            )
        })?;
    let service: Arc<dyn ErasedService> = Arc::new(Erased(service));
    #[cfg(feature = "files")]
    let service = match routing.files.clone() {
        Some(files) => {
            let inner = Arc::clone(&service);
            let router = crate::files::in_front(
                files,
                axum::Router::new().fallback(
                    move |request: axum::extract::Request| {
                        let inner = Arc::clone(&inner);
                        async move {
                            inner
                                .call_erased(request.map(RequestBody::new))
                                .await
                        }
                    },
                ),
            );
            Arc::new(Erased(router)) as Arc<dyn ErasedService>
        }
        None => service,
    };
    #[cfg(feature = "broadcast")]
    let (broadcast, service) = {
        let inner = Arc::clone(&service);
        let router =
            axum::Router::new().fallback(
                move |request: axum::extract::Request| {
                    let inner = Arc::clone(&inner);
                    async move {
                        inner.call_erased(request.map(RequestBody::new)).await
                    }
                },
            );
        match crate::broadcast::Broadcast::wrap(state, router)? {
            (Some(broadcast), router) => (
                Some(broadcast),
                Arc::new(Erased(router)) as Arc<dyn ErasedService>,
            ),
            (None, _) => (None, service),
        }
    };
    let upgrades = state
        .registry_ref()
        .resolve::<Owner>()
        .and_then(|owner| owner.upgrades.clone());
    state.registry_ref().insert(Arc::new(Application {
        service,
        #[cfg(feature = "broadcast")]
        broadcast,
        upgrades,
        policy: routing.policy,
        admission: routing.admission.clone(),
    }))?;
    Ok(())
}

/// The routables registered through `claviron::http::routing`, in front of the
/// owner: what a request the application's own routes leave unmatched meets
/// next. A path no routable declares, and a declared path reached with an
/// undeclared method, both go on to the owner. The owner's upgrades and Orbit
/// sessions are kept.
#[cfg(feature = "axum")]
fn routables_over(
    state: &AppState,
    owner: Owner,
) -> registry::Result<Owner> {
    let routables = state
        .registry_ref()
        .resolve::<claviron_http::routing::RoutableRegistry>()
        .ok_or_else(|| {
            registry::Error::msg(
                "the runner registers RoutableRegistry before configure",
            )
        })?;
    let unmatched = owner.clone();
    let router = routables
        .build(state)?
        .with_state(state.clone())
        .fallback_service(owner.clone())
        .method_not_allowed_fallback(move |request: axum::extract::Request| {
            let owner = unmatched.clone();
            async move { owner.handle(request).await }
        });
    Ok(Owner {
        service: boxed(router),
        upgrades: owner.upgrades,
        orbit_sessions: owner.orbit_sessions,
    })
}

/// Make sure something answers. An application that registered nothing is
/// served by its owner alone, with any host-free routables in front — the
/// whole of it until its first Rust route exists. Called by the runner after
/// `configure`.
pub fn ensure_application(state: &AppState) -> registry::Result<()> {
    if state.registry_ref().resolve::<Application>().is_some() {
        return Ok(());
    }
    #[cfg(feature = "axum")]
    return register_router(state, |_| axum::Router::new());
    #[cfg(not(feature = "axum"))]
    compose(state, owner(state)?)
}

/// This application's Rust routes, in front of its owner.
///
/// ```ignore
/// pub fn configure(state: &AppState) -> registry::Result<()> {
///     runner::register_router(state, routes::router)
/// }
/// ```
///
/// Handed the function that builds the router, called with the application's
/// state. The routables registered through `claviron::http::routing` stand
/// behind it, so a routable written once serves under any runner, and the
/// application's own route wins where both declare one. A path the router
/// does not declare, and a declared path reached with an undeclared method,
/// go on to the routables and then to the owner.
#[cfg(feature = "axum")]
pub fn register_router<F>(
    state: &AppState,
    build: F,
) -> registry::Result<()>
where
    F: FnOnce(AppState) -> axum::Router,
{
    let router = build(state.clone());
    let owner = routables_over(state, owner(state)?)?;
    let unmatched = owner.clone();
    let router = router.fallback_service(owner).method_not_allowed_fallback(
        move |request: axum::extract::Request| {
            let owner = unmatched.clone();
            async move { owner.handle(request).await }
        },
    );
    compose(state, router)
}

/// This application's service, placing the owner yourself: its own 404 for a
/// subtree, the owner somewhere other than the end, or nowhere. The owner
/// handed over has the host-free routables in front of it.
///
/// ```ignore
/// runner::register_router_with(state, |owner| my_router().fallback_service(owner))
/// ```
pub fn register_router_with<F, S, B>(
    state: &AppState,
    build: F,
) -> registry::Result<()>
where
    F: FnOnce(Owner) -> S,
    S: Service<AppRequest, Response = Response<B>, Error = Infallible>
        + Clone
        + Send
        + Sync,
    S: 'static,
    S::Future: Send + 'static,
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<RouteBodyError>,
{
    // The owner handed over already has the routables in front of it, so the
    // application's service still answers first.
    #[cfg(feature = "axum")]
    let owner = routables_over(state, owner(state)?)?;
    #[cfg(not(feature = "axum"))]
    let owner = owner(state)?;
    let service = build(owner);
    compose(state, service)
}

#[cfg(test)]
#[path = "../tests/routing.rs"]
mod tests;
