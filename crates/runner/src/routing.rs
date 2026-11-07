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
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use claviron_http::config::transport::{H1Policy, H2Policy, TransportPolicy};
use claviron_http::ext::{ConnCtx, RequestArrival};
use claviron_http::policy::HttpPolicy;
use claviron_http::{
    OtherHandler, OtherService, RouteBody, RouteBodyError, RouteFuture,
    RustRouter, empty_body,
};
use claviron_pipeline::compile::{SiteTerms, compile_site};
use claviron_pipeline::factory::{BackendPlacement, BackendScope, backend};
use claviron_pipeline::plan::HostExecution;
use claviron_pipeline::{BackendHandler, Site, SiteRegistry};
use claviron_runtime::{AppState, registry};
use http::{Method, Request, Response, StatusCode};
use tower::Service;
use tower::util::{BoxCloneSyncService, ServiceExt};

pub use claviron_transport::owner::{AppRequest, RequestBody};

/// What the composed service answers with.
pub type AppResponse = Response<RouteBody>;

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
        Self { service: boxed(service), upgrades: None }
    }

    /// Requests attempting to open a session, on any door: what answers one
    /// claims the offer in its extensions — an ASGI WebSocket. Without this, an
    /// attempt that reaches the owner is refused.
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

    /// Take the owner's own upgrades, for something placed in front of it to
    /// answer what it does not claim.
    pub fn take_upgrades(&mut self) -> Option<Upgrades> {
        self.upgrades.take()
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
        let Ok(response) = self.answer(request.map(RequestBody::new)).await;
        response
    }

    /// One owner answers HTTP and sessions alike.
    fn answer(
        &self,
        request: AppRequest,
    ) -> RouteFuture {
        let service = if attempting(&request) {
            match &self.upgrades {
                Some(upgrades) => upgrades,
                None => {
                    return Box::pin(std::future::ready(Ok(status(
                        StatusCode::NOT_IMPLEMENTED,
                    ))));
                }
            }
        } else {
            &self.service
        };
        Box::pin(service.clone().oneshot(request))
    }
}

impl<B> Service<Request<B>> for Owner
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<RouteBodyError>,
{
    type Response = AppResponse;
    type Error = Infallible;
    type Future = RouteFuture;

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
        self.answer(request.map(RequestBody::new))
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
    /// An application takes over what it claims; what a door can carry is the
    /// door's to say, and nothing here narrows it.
    transport: TransportPolicy,
    site: Option<Arc<Site>>,
    #[cfg(feature = "files")]
    files: Option<Arc<crate::files::Files>>,
}

impl Routing {
    /// `policy` bounds every request and reply; `HttpPolicy::unlimited()` is
    /// the explicit choice of none.
    pub fn new(policy: HttpPolicy) -> Self {
        Self {
            policy,
            transport: TransportPolicy {
                h1: H1Policy { upgrade: true },
                h2: H2Policy { connect_protocol: true },
                ..TransportPolicy::default()
            },
            site: None,
            #[cfg(feature = "files")]
            files: None,
        }
    }

    /// Register this, with the registry the application and its providers
    /// register owners and directives with; the site is compiled against it once
    /// `configure` is done.
    pub fn register(
        self,
        state: &AppState,
    ) -> registry::Result<()> {
        state.registry_ref().insert(Arc::new(SiteRegistry::default()))?;
        state.registry_ref().insert(Arc::new(self))?;
        Ok(())
    }

    /// The whole site in front of the application, as an edge host declares
    /// it. It replaces any file mounts.
    pub fn site(
        mut self,
        site: Option<Arc<Site>>,
    ) -> Self {
        self.site = site;
        self
    }

    /// What the plan serves from. A launch that declares nothing is an empty
    /// site, which leaves its owner nothing to walk through.
    fn declared_site(&self) -> Arc<Site> {
        if let Some(site) = &self.site {
            return Arc::clone(site);
        }
        #[cfg(feature = "files")]
        if let Some(files) = &self.files {
            return Arc::new(files.site());
        }
        Arc::new(Site::new("."))
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

/// This application, as every door calls it: what no door may read differently,
/// then the compiled site.
pub struct Application {
    execution: HostExecution,
}

impl Application {
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
        let answering = match refused(&request) {
            Some(code) => Err(code),
            None => {
                Ok(self.execution.handle(request.map(RequestBody::into_inner)))
            }
        };
        async move {
            let response = match answering {
                Ok(answering) => {
                    let Ok(response) = answering.await;
                    response
                }
                Err(code) => status(code),
            };
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
fn attempting<B>(request: &Request<B>) -> bool {
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

/// What is refused before anything reads it as a request: framing two parsers
/// could read differently, and a tunnel, which is not a session attempt.
fn refused(request: &AppRequest) -> Option<StatusCode> {
    if claviron_http::forwarding::reject_request_smuggling(
        request.version(),
        request.headers(),
    )
    .is_err()
    {
        return Some(StatusCode::BAD_REQUEST);
    }
    (request.method() == Method::CONNECT && !attempting(request))
        .then_some(StatusCode::NOT_IMPLEMENTED)
}

fn status(code: StatusCode) -> AppResponse {
    let mut response = Response::new(empty_body());
    *response.status_mut() = code;
    response
}

/// A service an application composed, as the router a site is compiled with. It
/// is told the policy that applies.
struct Composed<S> {
    service: S,
    policy: HttpPolicy,
}

impl<S, B> OtherHandler for Composed<S>
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
    fn call(
        &self,
        mut request: Request<RouteBody>,
    ) -> RouteFuture {
        request.extensions_mut().insert(self.policy);
        let service = self.service.clone();
        Box::pin(async move {
            let Ok(response) =
                service.oneshot(request.map(RequestBody::new)).await;
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
        + Sync
        + 'static,
    S::Future: Send + 'static,
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<RouteBodyError>,
{
    let policy = routing(state)?.policy;
    compose_router(
        state,
        RustRouter::Other(OtherService::new(Composed { service, policy })),
    )
}

fn routing(state: &AppState) -> registry::Result<Arc<Routing>> {
    state.registry_ref().resolve::<Routing>().ok_or_else(|| {
        registry::Error::msg("the runner registers Routing before configure")
    })
}

/// Register what answers as the owner of the site, after its files, and compile
/// the site. Whatever else was registered with the site's registry stands with
/// it, and a scope's own owner replaces this one there.
fn compose_router(
    state: &AppState,
    router: RustRouter,
) -> registry::Result<()> {
    let routing = routing(state)?;
    let sites =
        state.registry_ref().resolve::<SiteRegistry>().ok_or_else(|| {
            registry::Error::msg(
                "the runner registers SiteRegistry before configure",
            )
        })?;
    let owner: Arc<dyn BackendHandler> = Arc::new(router);
    sites.register_backend(backend(
        "application",
        BackendScope::site(),
        BackendPlacement::AfterFiles,
        move |_| Ok(vec![Arc::clone(&owner)]),
    ))?;
    let execution = compile_site(
        state,
        routing.declared_site(),
        SiteTerms {
            listener: None,
            policy: Some(routing.policy),
            transport: &routing.transport,
        },
        Some(&sites),
    )?;
    state.registry_ref().insert(Arc::new(Application { execution }))?;
    Ok(())
}

#[cfg(feature = "rust")]
pub(crate) fn register_prepared_router(
    state: &AppState,
    router: RustRouter,
) -> registry::Result<()> {
    #[cfg(feature = "axum")]
    if let Some(routes) = contributed_routes(state)? {
        let routes = routes.fallback(move |request: axum::extract::Request| {
            let router = router.clone();
            async move {
                let Ok(response) = router
                    .handle(
                        request
                            .map(RequestBody::new)
                            .map(RequestBody::into_inner),
                    )
                    .await;
                response
            }
        });
        return compose_router(state, routes.into());
    }
    compose_router(state, router)
}

/// The routables registered through `claviron::pipeline::routing`, in front of the
/// owner: what a request the application's own routes leave unmatched meets
/// next. A path no routable declares, and a declared path reached with an
/// undeclared method, both go on to the owner. The owner's upgrades are kept.
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
    // Attempts meet the same routes; what those leave reaches the owner's own.
    Ok(Owner { service: boxed(router.clone()), upgrades: Some(boxed(router)) })
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
        return compose_router(state, router.into());
    }
    compose(state, service)
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
    compose_router(state, router.into())
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
    let policy = routing(state)?.policy;
    compose_router(
        state,
        RustRouter::Other(OtherService::new(SelectedRoutes {
            routes,
            owner,
            policy,
        })),
    )
}

struct SelectedRoutes {
    routes:
        claviron_http::config::routes::RouteTable<claviron_http::RustRouter>,
    owner: Owner,
    policy: HttpPolicy,
}

impl OtherHandler for SelectedRoutes {
    fn call(
        &self,
        mut request: Request<RouteBody>,
    ) -> RouteFuture {
        request.extensions_mut().insert(self.policy);
        match self.routes.find(request.uri().path()) {
            Some(router) => router.handle(request),
            None => self.owner.answer(request.map(RequestBody::new)),
        }
    }
}

#[cfg(test)]
#[path = "../tests/routing.rs"]
mod tests;
