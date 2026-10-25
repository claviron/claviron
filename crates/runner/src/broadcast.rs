//! Claviron's broadcasting in front of the owner, on every door: the Pusher
//! REST publication routes and each application's WebSocket at `/app/{key}`
//! on the sockets, and the same session over Orbit, where the edge speaks the
//! protocol and whole messages cross the slot.
//!
//! The protocol, the sessions, channel authorization and the fleet's fanout
//! are the broadcast service's; this only places them. It is present when
//! Claviron registered the service — a `broadcast.yaml` beside the launch —
//! and absent otherwise, with nothing in front of the owner.

use std::collections::HashSet;
use std::sync::Arc;

use axum::{Router, middleware};
use claviron_broadcast::{BroadcastService, BroadcastSession};
use claviron_http::session::SessionSpawner;
use claviron_http::upgrade::{H1UpgradeMiddlewareState, H1UpgradeRegistry};
use claviron_runtime::{AppState, registry};
use http::{Method, StatusCode, Version};

use crate::routing::AppRequest;

pub(crate) struct Broadcast {
    paths: HashSet<String>,
    service: Arc<BroadcastService>,
}

impl Broadcast {
    /// The service's endpoints in front of `inner`, or nothing when the
    /// service is not registered.
    pub(crate) fn wrap(
        state: &AppState,
        inner: Router,
    ) -> registry::Result<(Option<Arc<Self>>, Router)> {
        let Some(service) = state.registry_ref().resolve::<BroadcastService>()
        else {
            return Ok((None, inner));
        };
        // The listener's own identity in the upgrade registry, not a host: a
        // runner has none, and whatever stands in front owns public hosts.
        let registry = Arc::new(H1UpgradeRegistry::default());
        let upgrade = H1UpgradeMiddlewareState {
            app_state: state.clone(),
            registry: Arc::clone(&registry),
            host: "runner".parse().expect("a valid listener name"),
        };
        let mut paths = HashSet::new();
        for (name, application) in service.applications().applications() {
            let path = format!("/app/{}", application.key);
            registry.register_handler(
                upgrade.host.clone(),
                path.clone(),
                service.upgrade_handler(name.as_str()),
            )?;
            paths.insert(path);
        }
        let router = Router::new()
            .merge(claviron_broadcast::pusher_routes(Arc::clone(&service)))
            .fallback_service(inner)
            .layer(middleware::from_fn_with_state(
                upgrade,
                claviron_http::upgrade::h1_upgrade_middleware,
            ));
        Ok((Some(Arc::new(Self { paths, service })), router))
    }

    /// Whether a socket's upgrade is one of the applications' WebSockets: an
    /// HTTP/1.1 GET to a registered path, on a connection that can hold the
    /// session it starts.
    pub(crate) fn accepts(
        &self,
        request: &AppRequest,
    ) -> bool {
        request.method() == Method::GET
            && request.version() == Version::HTTP_11
            && request.extensions().get::<SessionSpawner>().is_some()
            && self.paths.contains(request.uri().path())
    }

    /// A message stream over Orbit for one of the applications: `None` when the
    /// path is not one of theirs — the owner's own WebSocket — and the
    /// refusal's status when it is theirs and cannot be served. The edge
    /// owns the client's handshake and waits for this admission before accepting.
    pub(crate) fn accept_orbit(
        &self,
        request: &AppRequest,
    ) -> Option<
        impl std::future::Future<Output = Result<BroadcastSession, StatusCode>>
        + Send
        + use<>,
    > {
        let key = request.uri().path().strip_prefix("/app/")?;
        let catalog = self.service.applications();
        let (application, _) = catalog.application_by_key(key)?;
        let session = self
            .service
            .accept_session(Arc::from(application))
            .map_err(|refusal| refusal.status());
        let webtransport = is_webtransport(request);
        let host = request
            .headers()
            .get(http::header::HOST)
            .and_then(|host| host.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        let headers = request.headers().clone();
        Some(async move {
            let mut session = session?;
            if !webtransport {
                session.authenticate_headers(&host, &headers).await?;
            }
            Ok(session)
        })
    }
}

pub(crate) fn is_webtransport(request: &AppRequest) -> bool {
    request.version() == Version::HTTP_3 && request.method() == Method::CONNECT
}

/// A WebTransport stream uses the same message records over Link as it does
/// on a native H3 listener, including its optional first authentication record.
pub(crate) async fn serve_orbit(
    mut session: BroadcastSession,
    host: String,
    webtransport: bool,
    endpoint: claviron_orbit::transport::Endpoint,
) -> claviron_runtime::registry::Result<()> {
    let mut stream =
        claviron_broadcast::stream::OrbitMessageStream::new(endpoint);
    if webtransport
        && !session
            .authenticate_stream(&host, &mut stream, std::future::pending())
            .await
            .map_err(|error| {
                claviron_runtime::registry::Error::recoverable(
                    error.to_string(),
                )
            })?
    {
        return Ok(());
    }
    session.run(stream).await.map_err(|error| {
        claviron_runtime::registry::Error::recoverable(error.to_string())
    })
}
