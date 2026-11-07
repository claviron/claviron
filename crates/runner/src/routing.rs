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
use claviron_http::deadline::head_within;
use claviron_http::ext::{ConnCtx, RequestArrival};
use claviron_http::policy::HttpPolicy;
use claviron_http::{RouteBody, RouteBodyError, RouteFuture, empty_body};
use claviron_runtime::{AppState, registry};
use http::{Method, Request, Response, StatusCode};
use tower::Service;
use tower::util::{BoxCloneSyncService, ServiceExt};

pub use claviron_transport::owner::{AppRequest, RequestBody};

/// What answers a message session the edge carries, in a build with that door;
/// a build without it can name none.
#[cfg(feature = "orbit")]
pub type Sessions = crate::orbit::OrbitSession;
#[cfg(not(feature = "orbit"))]
pub type Sessions = Infallible;

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
    RequestBody::new(body).into_inner()
}

/// An owner's service for requests asking to take the connection over.
pub type Upgrades = BoxCloneSyncService<AppRequest, AppResponse, Infallible>;

/// The owner: the language's service — embedded PHP, the ASGI engine — as the
/// shell holds it. A runner registers it before the application's `configure`
/// runs; its engine boots before any ingress does, so a service whose inside
/// is filled at boot is what a runner hands over.
#[derive(Clone)]
pub struct Owner {
    service: BoxCloneSyncService<AppRequest, AppResponse, Infallible>,
    upgrades: Option<Upgrades>,
    orbit_sessions: Option<Sessions>,
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
    #[cfg(feature = "orbit")]
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

    pub(crate) fn orbit_session(&self) -> Option<Sessions> {
        self.orbit_sessions.clone()
    }

    /// Take the owner's own upgrades, for something placed in front of it to
    /// answer what it does not claim.
    pub fn take_upgrades(&mut self) -> Option<Upgrades> {
        self.upgrades.take()
    }

    /// Take the owner's own message sessions, the same way.
    #[cfg(feature = "orbit")]
    pub fn take_orbit_sessions(&mut self) -> Option<Sessions> {
        self.orbit_sessions.take()
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
    #[cfg(feature = "pipeline")]
    site: Option<Arc<claviron_pipeline::Site>>,
    #[cfg(feature = "files")]
    files: Option<Arc<crate::files::Files>>,
}

#[cfg(feature = "pipeline")]
impl Routing {
    /// The whole site in front of the application, as an edge host declares
    /// it. It replaces any file mounts.
    pub fn site(
        mut self,
        site: Option<Arc<claviron_pipeline::Site>>,
    ) -> Self {
        self.site = site;
        self
    }

    /// What the plan serves from, when this launch declares anything.
    fn declared_site(&self) -> Option<Arc<claviron_pipeline::Site>> {
        if self.site.is_some() {
            return self.site.clone();
        }
        #[cfg(feature = "files")]
        return self.files.as_ref().map(|files| Arc::new(files.site()));
        #[cfg(not(feature = "files"))]
        None
    }
}

impl Routing {
    /// `policy` bounds every request and reply; `HttpPolicy::unlimited()` is
    /// the explicit choice of none.
    pub fn new(policy: HttpPolicy) -> Self {
        Self {
            policy,
            #[cfg(feature = "pipeline")]
            site: None,
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
    #[cfg(feature = "orbit")]
    sessions: Option<Sessions>,
}

impl Application {
    #[cfg(feature = "orbit")]
    pub(crate) fn orbit_session(&self) -> Option<Sessions> {
        self.sessions.clone()
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
    ) -> impl Future<Output = AppResponse> + Send + use<> {
        let exchange = claviron_runtime::activity::ACTIVITY.begin();
        let service = Arc::clone(&self.service);
        async move {
            let response = admitted(service, request).await;
            drop(exchange);
            response
        }
    }
}

/// A door with no connection facts of its own to attach: over Orbit the
/// exchange already carries what the edge knew.
impl claviron_transport::owner::Owner for Application {
    fn call(
        &self,
        request: Request<RouteBody>,
        _connection: Option<&Arc<ConnCtx>>,
        _arrival: RequestArrival,
    ) -> impl Future<Output = AppResponse> + Send + use<> {
        Application::call(self, request.map(RequestBody::new))
    }
}

/// Whether a door offered this request as a session. An `Upgrade` to anything
/// else is an ordinary request, which HTTP lets a server answer as one.
fn attempting(request: &AppRequest) -> bool {
    #[cfg(any(feature = "socket", feature = "orbit"))]
    return request
        .extensions()
        .get::<claviron_http::socket::Offer>()
        .is_some();
    #[cfg(not(any(feature = "socket", feature = "orbit")))]
    {
        let _ = request;
        false
    }
}

/// Refuse what no stage may read differently or take over, then hand on what
/// is left.
async fn admitted(
    service: Arc<dyn ErasedService>,
    request: AppRequest,
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
    // A tunnel is refused. A session attempt goes on with the rest, through
    // whatever stands in front of the application.
    if request.method() == Method::CONNECT && !attempting(&request) {
        return status(StatusCode::NOT_IMPLEMENTED);
    }
    let Ok(response) = service.call_erased(request).await;
    response
}

/// The composed service as the plan's last step: it is told the policy that
/// applies and its reply head is held to the deadline, which is an owner's to
/// enforce.
struct Answering {
    service: Arc<dyn ErasedService>,
    /// Who takes a connection over. It answers when it decides, so the reply
    /// head's deadline is not its; unclaimed, the attempt is refused.
    upgrades: Option<Arc<dyn ErasedService>>,
    policy: HttpPolicy,
}

impl ErasedService for Answering {
    fn call_erased(
        &self,
        mut request: AppRequest,
    ) -> RouteFuture {
        let (service, policy) = (Arc::clone(&self.service), self.policy);
        let upgrades = self.upgrades.clone();
        Box::pin(async move {
            request.extensions_mut().insert(policy);
            if attempting(&request) {
                return Ok(match upgrades {
                    Some(upgrades) => match upgrades.call_erased(request).await
                    {
                        Ok(response) => response,
                        Err(never) => match never {},
                    },
                    None => status(StatusCode::NOT_IMPLEMENTED),
                });
            }
            Ok(
                match head_within(
                    policy.requests.head_timeout,
                    service.call_erased(request),
                )
                .await
                {
                    Some(Ok(response)) => response,
                    Some(Err(never)) => match never {},
                    None => status(StatusCode::GATEWAY_TIMEOUT),
                },
            )
        })
    }
}

#[cfg(feature = "pipeline")]
mod plan {
    use claviron_http::{RouteBody, ServiceResult};
    use claviron_pipeline::plan::HostExecution;
    use claviron_pipeline::{
        BackendContract, BackendHandler, BackendIdentity, BackendReply,
        DispatchCall, DispatchStep, HostContext, Site,
    };

    use super::*;

    impl DispatchStep for Answering {
        fn call(
            &self,
            request: Request<RouteBody>,
            _context: Option<HostContext<'_>>,
        ) -> DispatchCall {
            let answering = self.call_erased(request.map(RequestBody::new));
            DispatchCall::new(async move {
                let Ok(response) = answering.await;
                ServiceResult::Handled(response)
            })
        }
    }

    impl BackendHandler for Answering {
        fn contract(&self) -> BackendContract {
            BackendContract {
                identity: BackendIdentity::None,
                reply: BackendReply::Owner,
            }
        }
    }

    struct Planned(HostExecution);

    impl ErasedService for Planned {
        fn call_erased(
            &self,
            request: AppRequest,
        ) -> RouteFuture {
            self.0.handle(request.map(RequestBody::into_inner))
        }
    }

    /// Bodies, replies and files in front of what answers. A launch that
    /// declared none of them leaves no plan to walk through.
    pub(super) fn over(
        state: &AppState,
        routing: &Routing,
        answering: Answering,
    ) -> registry::Result<Arc<dyn ErasedService>> {
        let policy = answering.policy;
        let site = routing.declared_site();
        if !bounded(&policy) && site.is_none() {
            return Ok(Arc::new(answering));
        }
        let site = site.unwrap_or_else(|| Arc::new(Site::new(".")));
        let execution = claviron_pipeline::compile::compile_with_owner(
            state,
            site,
            policy,
            Arc::new(answering),
        )
        .map_err(|error| registry::Error::msg(error.to_string()))?;
        Ok(Arc::new(Planned(execution)))
    }
}

/// Whether the policy bounds a body in either direction. The reply head's
/// deadline is not among them: an owner holds it with or without a plan.
fn bounded(policy: &HttpPolicy) -> bool {
    policy.requests.max_body_size.is_some()
        || policy.requests.body_read_timeout.is_some()
        || policy.responses.max_body_size.is_some()
        || policy.responses.body_idle_timeout.is_some()
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
    ) -> RouteFuture;
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
    ) -> RouteFuture {
        let service = self.0.clone();
        Box::pin(async move {
            let Ok(response) = service.oneshot(request).await;
            Ok(response.map(route_body))
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
    compose_with(state, service, false, None)
}

fn compose_with<S, B>(
    state: &AppState,
    service: S,
    rust: bool,
    sessions: Option<Sessions>,
) -> registry::Result<()>
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
    compose_service(state, Arc::new(Erased(service)), rust, sessions)
}

fn compose_service(
    state: &AppState,
    service: Arc<dyn ErasedService>,
    rust: bool,
    sessions: Option<Sessions>,
) -> registry::Result<()> {
    let routing =
        state.registry_ref().resolve::<Routing>().ok_or_else(|| {
            registry::Error::msg(
                "the runner registers Routing before configure",
            )
        })?;
    let owner = state.registry_ref().resolve::<Owner>();
    let upgrades = if rust && cfg!(feature = "websocket") {
        Some(Arc::clone(&service))
    } else {
        owner
            .as_ref()
            .and_then(|owner| owner.upgrades.clone())
            .map(|service| Arc::new(Erased(service)) as Arc<dyn ErasedService>)
    };
    let sessions = sessions
        .or_else(|| owner.as_ref().and_then(|owner| owner.orbit_session()));
    let answering = Answering { service, upgrades, policy: routing.policy };
    #[cfg(feature = "pipeline")]
    let service = plan::over(state, &routing, answering)?;
    #[cfg(not(feature = "pipeline"))]
    let service: Arc<dyn ErasedService> = {
        if bounded(&answering.policy) {
            return Err(registry::Error::msg(
                "requests: and responses: body bounds need claviron-runner's \
                 `pipeline` feature (`runner-pipeline` on the facade); \
                 without it they would not be applied",
            ));
        }
        Arc::new(answering)
    };
    #[cfg(not(feature = "orbit"))]
    let _ = sessions;
    state.registry_ref().insert(Arc::new(Application {
        service,
        #[cfg(feature = "orbit")]
        sessions,
    }))?;
    Ok(())
}

#[cfg(feature = "rust")]
struct PreparedRouter(claviron_http::RustRouter);

#[cfg(feature = "rust")]
impl ErasedService for PreparedRouter {
    fn call_erased(
        &self,
        request: AppRequest,
    ) -> RouteFuture {
        self.0.handle(request.map(RequestBody::into_inner))
    }
}

#[cfg(feature = "rust")]
pub(crate) fn register_prepared_router(
    state: &AppState,
    router: claviron_http::RustRouter,
    sessions: Option<Sessions>,
) -> registry::Result<()> {
    let service: Arc<dyn ErasedService> = Arc::new(PreparedRouter(router));
    #[cfg(feature = "axum")]
    if let Some(router) = contributed_routes(state)? {
        let router = router.fallback(move |request: axum::extract::Request| {
            let service = Arc::clone(&service);
            async move {
                let Ok(response) =
                    service.call_erased(request.map(RequestBody::new)).await;
                response
            }
        });
        return compose_with(state, router, true, sessions);
    }
    compose_service(state, service, true, sessions)
}

/// The routables registered through `claviron::pipeline::routing`, in front of the
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
        .resolve::<claviron_pipeline::routing::RoutableRegistry>()
        .ok_or_else(|| {
            registry::Error::msg(
                "the runner registers RoutableRegistry before configure",
            )
        })?;
    let Some(router) = routables.build(state)? else {
        return Ok(owner);
    };
    let unmatched = owner.clone();
    let router = router
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
    return compose(state, routables_over(state, owner(state)?)?);
    #[cfg(not(feature = "axum"))]
    compose(state, owner(state)?)
}

#[cfg(all(feature = "rust", feature = "axum"))]
fn contributed_routes(
    state: &AppState
) -> registry::Result<Option<axum::Router>> {
    let registry = state
        .registry_ref()
        .resolve::<claviron_pipeline::routing::RoutableRegistry>()
        .ok_or_else(|| {
            registry::Error::msg("RoutableRegistry is not registered")
        })?;
    Ok(registry.build(state)?.map(|router| router.with_state(state.clone())))
}

/// Register a terminal service without first erasing it as a language owner.
#[cfg(feature = "rust")]
pub(crate) fn register_other<S, B>(
    state: &AppState,
    service: S,
    sessions: Option<Sessions>,
) -> registry::Result<()>
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
    #[cfg(feature = "axum")]
    if let Some(router) = contributed_routes(state)? {
        let router = router.fallback(move |request: axum::extract::Request| {
            let service = service.clone();
            async move {
                let Ok(response) =
                    service.oneshot(request.map(RequestBody::new)).await;
                response
            }
        });
        return compose_with(state, router, true, sessions);
    }
    compose_with(state, service, true, sessions)
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
/// state. The routables registered through `claviron::pipeline::routing` stand
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

/// Place selected Rust paths before an existing application owner.
///
/// Select by the original path before transferring or polling the body. A
/// selected router's response is terminal, including 404/405. Paths outside
/// the table reach the owner's normal service unchanged. Register one complete
/// table during the application's configuration callback.
pub fn register_routes(
    state: &AppState,
    routes: impl IntoIterator<
        Item = (claviron_http::ext::PathPrefix, claviron_http::RustRouter),
    >,
) -> registry::Result<()> {
    let routes = claviron_http::config::routes::RouteTable::new(routes);
    if routes.is_empty() {
        return Ok(());
    }
    for (index, (prefix, _)) in routes.iter().enumerate() {
        if routes.iter().take(index).any(|(other, _)| other == prefix) {
            return Err(registry::Error::msg(format!(
                "duplicate Rust route prefix '{prefix}'"
            )));
        }
    }
    #[cfg(feature = "axum")]
    let owner = routables_over(state, owner(state)?)?;
    #[cfg(not(feature = "axum"))]
    let owner = owner(state)?;
    compose_service(
        state,
        Arc::new(SelectedRoutes { routes, owner }),
        false,
        None,
    )
}

struct SelectedRoutes {
    routes:
        claviron_http::config::routes::RouteTable<claviron_http::RustRouter>,
    owner: Owner,
}

impl ErasedService for SelectedRoutes {
    fn call_erased(
        &self,
        request: AppRequest,
    ) -> RouteFuture {
        match self.routes.find(request.uri().path()) {
            Some(router) => router.handle(request.map(RequestBody::into_inner)),
            None => Box::pin(self.owner.service.clone().oneshot(request)),
        }
    }
}

#[cfg(test)]
#[path = "../tests/routing.rs"]
mod tests;
