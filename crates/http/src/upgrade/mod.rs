//! HTTP/1 upgrade primitives shared by host routing and application runtimes.
//!
//! This module owns the protocol boundary only: exact host/path dispatch, the
//! pre-upgrade acceptance phase, the `101 Switching Protocols` response, and
//! delivery of Hyper's upgraded stream. WebSocket applications and vhost
//! assembly both use it; it depends on neither of them.

use std::any::Any;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, RwLock};

use crate::ext::ConnCtx;
use crate::session::SessionSpawner;
use async_trait::async_trait;
#[cfg(feature = "axum")]
use axum::{body::Body, extract::State, middleware::Next};
use crate::{RouteBody, RouteResponse, ServiceResult, bytes_body, empty_body, err_status};
use claviron_runtime::AppState;
use claviron_runtime::registry::{Error, Result};
use claviron_types::Host;
use http::{HeaderMap, Request, StatusCode, Uri};
use hyper::upgrade::{self, Upgraded};
use tracing::{debug, error};

/// Request context captured before an HTTP/1 connection is upgraded.
#[derive(Clone)]
pub struct H1UpgradeContext {
    pub host: Host,
    pub path: Arc<str>,
    pub uri: Uri,
    pub headers: HeaderMap,
    /// Policies and identity prepared by the host pipeline.
    pub extensions: http::Extensions,
    pub conn: Arc<ConnCtx>,
    /// Listener-owned session capability and cooperative graceful-close signal.
    pub sessions: SessionSpawner,
}

/// Opaque handler-owned state carried from acceptance to the upgraded session.
///
/// The transport pipeline never inspects this value. The handler that inserted
/// it knows its concrete type and can retrieve it with [`Self::take`].
pub struct H1UpgradeState {
    value: Box<dyn Any + Send + Sync>,
}

impl H1UpgradeState {
    pub fn new<T>(value: T) -> Self
    where
        T: Send + Sync + 'static,
    {
        Self { value: Box::new(value) }
    }

    pub fn take<T>(self) -> Result<T>
    where
        T: Send + Sync + 'static,
    {
        self.value
            .downcast::<T>()
            .map(|value| *value)
            .map_err(|_| Error::msg("H1 upgrade state type mismatch"))
    }
}

/// Application-owned HTTP/1 upgrade handler.
///
/// This registry is intentionally H1-only. HTTP/2 CONNECT and HTTP/3
/// WebTransport use their own transport-session registries because their
/// accepted stream/session APIs are different.
/// `handle_upgraded` is awaited by the accepting listener's session owner.
/// Keep session work in that future, or submit child work through `ctx.sessions`;
/// detached tasks cannot participate in the listener's drain guarantee.
#[async_trait]
pub trait H1UpgradeHandler: Send + Sync + 'static {
    fn name(&self) -> &'static str {
        "h1_upgrade_handler"
    }

    fn is_upgrade(
        &self,
        req: &Request<RouteBody>,
    ) -> bool;

    async fn accept(
        &self,
        state: &AppState,
        ctx: &H1UpgradeContext,
    ) -> H1UpgradeResult;

    async fn handle_upgraded(
        &self,
        state: AppState,
        ctx: H1UpgradeContext,
        accepted: Option<H1UpgradeState>,
        upgraded: Upgraded,
    ) -> Result<()>;
}

pub struct H1UpgradeAccept {
    response_headers: HeaderMap,
    state: Option<H1UpgradeState>,
}

pub type H1UpgradeResult =
    std::result::Result<H1UpgradeAccept, H1UpgradeReject>;

impl H1UpgradeAccept {
    pub fn new(response_headers: HeaderMap) -> Self {
        Self { response_headers, state: None }
    }

    pub fn anonymous(response_headers: HeaderMap) -> Self {
        Self::new(response_headers)
    }

    pub fn with_state<T>(
        response_headers: HeaderMap,
        state: T,
    ) -> Self
    where
        T: Send + Sync + 'static,
    {
        Self { response_headers, state: Some(H1UpgradeState::new(state)) }
    }

    /// Compatibility name for authentication-oriented handlers.
    pub fn authenticated<T>(
        response_headers: HeaderMap,
        principal: T,
    ) -> Self
    where
        T: Send + Sync + 'static,
    {
        Self::with_state(response_headers, principal)
    }
}

/// Compatibility alias for handlers that use the accepted state as identity.
pub type H1UpgradeAuthenticated = H1UpgradeState;

pub struct H1UpgradeReject {
    status: StatusCode,
    body: Option<String>,
}

impl H1UpgradeReject {
    /// Preserve an admission refusal when the same policy serves CONNECT.
    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn new(
        status: StatusCode,
        body: impl Into<Option<String>>,
    ) -> Self {
        Self { status, body: body.into() }
    }

    pub fn bad_request(body: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, Some(body.into()))
    }

    pub fn unauthorized(body: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, Some(body.into()))
    }

    pub fn forbidden(body: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, Some(body.into()))
    }

    pub fn internal(body: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, Some(body.into()))
    }

    pub fn service_unavailable(body: impl Into<String>) -> Self {
        Self::new(StatusCode::SERVICE_UNAVAILABLE, Some(body.into()))
    }

    fn into_response(self) -> RouteResponse {
        match self.body {
            Some(body) => http::Response::builder()
                .status(self.status)
                .body(bytes_body(body.into()))
                .unwrap_or_else(|_| {
                    err_status(StatusCode::INTERNAL_SERVER_ERROR)
                }),
            None => err_status(self.status),
        }
    }
}

#[derive(Clone)]
pub enum H1UpgradeBinding {
    Handler(Arc<dyn H1UpgradeHandler>),
}

impl H1UpgradeBinding {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Handler(_) => "handler",
        }
    }
}

#[derive(Clone)]
pub struct H1UpgradeRoute {
    path: Arc<str>,
    binding: H1UpgradeBinding,
}

impl H1UpgradeRoute {
    pub fn handler(
        path: impl Into<Arc<str>>,
        handler: Arc<dyn H1UpgradeHandler>,
    ) -> Result<Self> {
        Ok(Self {
            path: normalize_upgrade_path(path.into())?,
            binding: H1UpgradeBinding::Handler(handler),
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct H1UpgradeRouteKey {
    host: Host,
    path: Arc<str>,
}

#[derive(Default)]
pub struct H1UpgradeRegistry {
    routes: RwLock<HashMap<H1UpgradeRouteKey, H1UpgradeBinding>>,
}

impl H1UpgradeRegistry {
    /// Capture one host's exact endpoints while preparing its connection plan.
    /// Native dispatch borrows this immutable map without a registry lock.
    pub fn snapshot_for_host(&self, host: &Host) -> HashMap<Arc<str>, H1UpgradeBinding> {
        self.routes.read().expect("h1 upgrade registry lock poisoned").iter()
            .filter(|(key, _)| &key.host == host)
            .map(|(key, binding)| (key.path.clone(), binding.clone()))
            .collect()
    }
    pub fn register_handler_for_host(
        &self,
        host: &str,
        path: impl Into<Arc<str>>,
        handler: Arc<dyn H1UpgradeHandler>,
    ) -> Result<()> {
        let host = Host::from_str(host).map_err(|err| {
            Error::msg(format!("invalid H1 upgrade host '{host}': {err}"))
        })?;
        self.register_handler(host, path, handler)
    }

    pub fn register_handler(
        &self,
        host: Host,
        path: impl Into<Arc<str>>,
        handler: Arc<dyn H1UpgradeHandler>,
    ) -> Result<()> {
        self.register_binding(host, path, H1UpgradeBinding::Handler(handler))
    }

    pub fn register_binding(
        &self,
        host: Host,
        path: impl Into<Arc<str>>,
        binding: H1UpgradeBinding,
    ) -> Result<()> {
        let path = normalize_upgrade_path(path.into())?;
        let key = H1UpgradeRouteKey { host, path };
        let mut routes =
            self.routes.write().expect("h1 upgrade registry lock poisoned");
        if routes.contains_key(&key) {
            return Err(Error::msg(format!(
                "duplicate H1 upgrade binding for host={} path={}",
                key.host, key.path
            )));
        }
        routes.insert(key, binding);
        Ok(())
    }

    pub fn register_route(
        &self,
        host: Host,
        route: H1UpgradeRoute,
    ) -> Result<()> {
        self.register_binding(host, route.path, route.binding)
    }

    pub fn resolve(
        &self,
        host: &Host,
        path: &str,
    ) -> Option<H1UpgradeBinding> {
        let path = normalize_upgrade_path(Arc::<str>::from(path)).ok()?;
        let key = H1UpgradeRouteKey { host: host.clone(), path };
        self.routes
            .read()
            .expect("h1 upgrade registry lock poisoned")
            .get(&key)
            .cloned()
    }

    pub fn has_host(
        &self,
        host: &Host,
    ) -> bool {
        self.routes
            .read()
            .expect("h1 upgrade registry lock poisoned")
            .keys()
            .any(|key| &key.host == host)
    }
}

#[cfg(feature = "axum")]
#[doc(hidden)]
#[derive(Clone)]
pub struct H1UpgradeMiddlewareState {
    pub app_state: AppState,
    pub registry: Arc<H1UpgradeRegistry>,
    pub host: Host,
}

#[cfg(feature = "axum")]
#[doc(hidden)]
pub async fn h1_upgrade_middleware(
    State(state): State<H1UpgradeMiddlewareState>,
    req: Request<Body>,
    next: Next,
) -> axum::response::Response {
    let Some(binding) = state.registry.resolve(&state.host, req.uri().path())
    else {
        return next.run(req).await;
    };

    match try_h1_upgrade(state.app_state, state.host, binding, req.map(crate::boxed_body)).await {
        ServiceResult::Handled(response) => crate::into_axum_response(response),
        ServiceResult::Unhandled(request) => next.run(request.map(Body::new)).await,
    }
}

/// Admit one already-selected HTTP/1 endpoint and transfer its session to the
/// listener owner. Unhandled retains the original body; no router is involved.
pub async fn try_h1_upgrade(
    app_state: AppState,
    host: Host,
    binding: H1UpgradeBinding,
    req: Request<RouteBody>,
) -> ServiceResult<RouteResponse, Request<RouteBody>> {
    let H1UpgradeBinding::Handler(handler) = binding;
    if !handler.is_upgrade(&req) {
        return ServiceResult::Unhandled(req);
    }

    let Some(conn) = req.extensions().get::<Arc<ConnCtx>>().cloned() else {
        error!(
            host = %host,
            path = %req.uri().path(),
            handler = handler.name(),
            "H1 upgrade request missing connection context"
        );
        return ServiceResult::Handled(err_status(StatusCode::INTERNAL_SERVER_ERROR));
    };

    let Some(sessions) = req.extensions().get::<SessionSpawner>().cloned()
    else {
        error!(
            handler = handler.name(),
            "H1 upgrade request missing session owner"
        );
        return ServiceResult::Handled(err_status(StatusCode::INTERNAL_SERVER_ERROR));
    };

    let ctx = H1UpgradeContext {
        host,
        path: Arc::from(req.uri().path()),
        uri: req.uri().clone(),
        headers: req.headers().clone(),
        extensions: req.extensions().clone(),
        conn,
        sessions: sessions.clone(),
    };

    let accepted = match handler.accept(&app_state, &ctx).await {
        Ok(accepted) => accepted,
        Err(reject) => {
            debug!(handler = handler.name(), "H1 upgrade rejected");
            return ServiceResult::Handled(reject.into_response());
        }
    };

    let handler_name = handler.name();
    let response = switching_protocols_response(accepted.response_headers);
    let accepted_state = accepted.state;
    let submitted = sessions.spawn(async move {
        debug!(handler = handler_name, "H1 upgrade waiting");
        match upgrade::on(req).await {
            Ok(upgraded) => {
                debug!(handler = handler_name, "H1 upgrade accepted");
                // Open for as long as the session is, and never an exchange:
                // nothing expects a WebSocket to end.
                let _session = claviron_runtime::activity::ACTIVITY.open_session();
                if let Err(err) =
                    handler.handle_upgraded(app_state, ctx, accepted_state, upgraded).await
                {
                    error!(handler = handler_name, error = %err, "H1 upgrade handler failed");
                }
            }
            Err(err) => {
                error!(handler = handler_name, error = %err, "H1 upgrade failed");
            }
        }
    });

    if let Err(error) = submitted {
        debug!(handler = handler_name, %error, "H1 session hand-off rejected");
        return ServiceResult::Handled(err_status(StatusCode::SERVICE_UNAVAILABLE));
    }

    ServiceResult::Handled(response)
}

pub fn is_h1_header_upgrade<B>(
    req: &Request<B>,
    upgrade: &str,
) -> bool {
    if req.version() != http::Version::HTTP_11 {
        return false;
    }

    let has_connection_upgrade = req
        .headers()
        .get_all(http::header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|part| part.trim().eq_ignore_ascii_case("upgrade"));

    if !has_connection_upgrade {
        return false;
    }

    req.headers()
        .get(http::header::UPGRADE)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.eq_ignore_ascii_case(upgrade))
        .unwrap_or(false)
}

pub fn websocket_response_headers(
    ctx: &H1UpgradeContext,
    protocol: Option<&str>,
) -> Result<HeaderMap> {
    let key = ctx
        .headers
        .get(http::header::SEC_WEBSOCKET_KEY)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| Error::msg("missing Sec-WebSocket-Key"))?;
    let version = ctx
        .headers
        .get(http::header::SEC_WEBSOCKET_VERSION)
        .and_then(|value| value.to_str().ok());
    if version != Some("13") {
        return Err(Error::msg("unsupported WebSocket version"));
    }

    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::SEC_WEBSOCKET_ACCEPT,
        websocket_accept_key(key).parse().map_err(|err| {
            Error::msg(format!("invalid WebSocket accept header: {err}"))
        })?,
    );
    if let Some(protocol) = protocol {
        headers.insert(
            http::header::SEC_WEBSOCKET_PROTOCOL,
            protocol.parse().map_err(|err| {
                Error::msg(format!("invalid WebSocket protocol header: {err}"))
            })?,
        );
    }
    Ok(headers)
}

pub fn websocket_accept_key(key: &str) -> String {
    use claviron_helpers::base64::base64_standard_encode;
    use sha1::{Digest, Sha1};

    const WS_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    let concat = format!("{key}{WS_GUID}");
    let mut hasher = Sha1::new();
    hasher.update(concat.as_bytes());
    let hash = hasher.finalize();
    base64_standard_encode(&hash)
}

fn normalize_upgrade_path(path: Arc<str>) -> Result<Arc<str>> {
    if path.starts_with('/') {
        Ok(path)
    } else {
        Err(Error::msg(format!("H1 upgrade path must start with '/': {path}")))
    }
}

fn switching_protocols_response(headers: HeaderMap) -> RouteResponse {
    let mut builder = http::Response::builder()
        .status(StatusCode::SWITCHING_PROTOCOLS)
        .header(http::header::UPGRADE, "websocket")
        .header(http::header::CONNECTION, "Upgrade");

    for (name, value) in headers {
        if let Some(name) = name {
            builder = builder.header(name, value);
        }
    }

    builder
        .body(empty_body())
        .unwrap_or_else(|_| err_status(StatusCode::INTERNAL_SERVER_ERROR))
}

#[cfg(test)]
#[path = "../../tests/upgrade/lib.rs"]
mod tests;
