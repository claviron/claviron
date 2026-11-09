//! The host a QUIC connection was accepted for, as the transport sees it.
//!
//! The handshake, the H3 driver and session routing are `crate::quic`. This is
//! what Edge adds: the SNI host, its early-data and WebTransport policy, its
//! retirement, and the handoff to its prepared execution and session registry.

use std::future::Future;
use std::sync::Arc;

use crate::VhostResolver;
use crate::host::{
    AnyAuthenticated, AuthContext, WebTransportBinding, WebTransportRegistry,
};
use crate::host::{H3WebTransportSession, WebTransportHandler};
use crate::service::HostService;
use crate::transport::VhostCtx;
use claviron_http::authority::AcceptedAuthority;
use claviron_http::config::ListenerConfig;
use claviron_http::config::transport::EarlyDataPolicy;
use claviron_http::ext::{ConnCtx, RequestArrival};
use claviron_http::socket::{Admission, Carrier, OfferInfo, SocketKind};
use claviron_http::{RouteBody, RouteResponse};
use claviron_pipeline::plan::HostExecution;
use claviron_runtime::AppState;
use http::{Request, StatusCode};
use tokio::sync::mpsc;
use tracing::{debug, error, info};

use claviron_transport::quic::wt::{
    Authorized, BiStream, NativeSession, QuicSession,
};
use claviron_transport::quic::{QuicHost, QuicRequests, Refusal, SessionScope};

/// The SNI host chosen for one connection by the publication it was accepted
/// under. Its routing and ingress policy hold for the connection's life.
pub(crate) struct SelectedHost {
    pub(super) vctx: VhostCtx,
    pub(super) app: HostExecution,
    pub(super) listener: Arc<ListenerConfig>,
    pub(super) state: AppState,
    pub(super) vhost_resolver: Arc<VhostResolver>,
    pub(super) web_transport: Arc<WebTransportRegistry>,
}

impl QuicHost for SelectedHost {
    type Requests = HostRequests;

    fn early_data(&self) -> EarlyDataPolicy {
        self.vctx.vhost_config.transport.h3.early_data
    }

    fn web_transport(&self) -> bool {
        self.web_transport.has_host(&self.vctx.vhost_key.0)
    }

    fn retired(&self) -> impl Future<Output = ()> + Send + 'static {
        let mut binding = self.vhost_resolver.bind_config(
            Arc::clone(&self.vctx.hostname.0),
            Arc::clone(&self.vctx.vhost_key.0),
            self.vctx.vhost_config.clone(),
        );
        async move {
            binding.retired().await;
            info!(protocol = "HTTP/3", host = %binding.hostname(), canonical = %binding.canonical(), "🔀 host mapping or configuration changed: retiring this connection gracefully");
        }
    }

    fn bind(
        self,
        mut facts: ConnCtx,
    ) -> Result<HostRequests, Refusal> {
        let Self { vctx, app, listener, state, web_transport, .. } = self;
        // The facts are complete; what the host prepares from them is frozen
        // into the connection before its first request.
        facts.state = app.accept(&claviron_pipeline::accept::Accepted::new(
            &facts,
            &vctx.hostname,
            &vctx.vhost_key,
            &vctx.vhost_config,
        ));
        let host = (*vctx.vhost_key.0).clone();
        let service =
            HostService::new(app, Arc::new(facts), vctx, listener.policy());
        Ok(HostRequests {
            service,
            accepted: AcceptedAuthority::default(),
            host,
            state,
            web_transport,
        })
    }
}

/// One H3 connection's requests, bound to its host.
pub(crate) struct HostRequests {
    service: HostService,
    /// This connection's last accepted authority fields; requests are
    /// admitted one at a time on its driver.
    accepted: AcceptedAuthority,
    host: claviron_types::Host,
    state: AppState,
    web_transport: Arc<WebTransportRegistry>,
}

impl QuicRequests for HostRequests {
    type Session = HostSession;

    fn admit(
        &mut self,
        head: &Request<()>,
    ) -> Result<(), StatusCode> {
        self.service
            .validate_authority(head, &mut self.accepted)
            .map_err(|reason| reason.status())
    }

    fn call(
        &self,
        request: Request<RouteBody>,
        arrival: RequestArrival,
    ) -> impl Future<Output = RouteResponse> + Send + 'static {
        self.service.request(request, arrival).dispatch()
    }

    fn connect(
        &self,
        head: Request<()>,
        scope: SessionScope,
    ) -> Result<
        impl Future<Output = Result<Authorized<HostSession>, StatusCode>>
        + Send
        + 'static,
        StatusCode,
    > {
        let attempt = Self::attempt(&head);
        let plan = self.service.clone();
        let prepared = self.service.request(head, RequestArrival::Established);
        let Some(WebTransportBinding::Handler(handler)) = self
            .web_transport
            .resolve(&self.host, prepared.request().uri().path())
        else {
            return Err(StatusCode::NOT_FOUND);
        };
        let mut context = prepared.auth_context();
        context.admission = Some(scope.permit);
        context.graceful = Some(scope.retired);
        let request = prepared.into_request();
        let state = self.state.clone();
        Ok(async move {
            // What stands in front of the owners decides first, as it does for
            // any other request to this scope.
            let admission = Admission::new(OfferInfo {
                kind: SocketKind::WebTransport,
                carrier: Carrier::H3,
            });
            let mut attempt = attempt;
            attempt.extensions_mut().insert(admission.offer());
            let answered =
                plan.request(attempt, RequestArrival::Established).dispatch();
            if let Some(refused) = admission.refused(answered.await) {
                debug!(status = %refused.status(), "WebTransport refused by the plan");
                return Err(refused.status());
            }
            let authenticated = match handler.accept(&state, &context).await {
                Ok(accepted) => accepted.into_authenticated(),
                Err(reject) => {
                    debug!(handler = handler.name(), "WebTransport rejected");
                    return Err(reject.status());
                }
            };
            Ok(Authorized {
                request,
                session: HostSession { handler, authenticated, state, context },
            })
        })
    }
}

impl HostRequests {
    /// The attempt as an ordinary request to the plan, with no body: session
    /// streams are not one, and no HTTP body limit applies to them.
    fn attempt(head: &Request<()>) -> Request<RouteBody> {
        let mut attempt = Request::new(claviron_http::empty_body());
        *attempt.method_mut() = head.method().clone();
        *attempt.uri_mut() = head.uri().clone();
        *attempt.version_mut() = head.version();
        *attempt.headers_mut() = head.headers().clone();
        attempt
    }
}

/// A WebTransport session authorized by the handler Edge's registry bound.
pub(crate) struct HostSession {
    handler: Arc<dyn WebTransportHandler>,
    authenticated: Option<AnyAuthenticated>,
    state: AppState,
    context: AuthContext,
}

impl QuicSession for HostSession {
    fn name(&self) -> &'static str {
        self.handler.name()
    }

    fn run(
        self,
        session: NativeSession,
        incoming: mpsc::Receiver<BiStream>,
    ) -> impl Future<Output = ()> + Send + 'static {
        let Self { handler, authenticated, state, context } = self;
        async move {
            let session = H3WebTransportSession::new(session, incoming);
            debug!(handler = handler.name(), session_id = ?session.session_id(), "WebTransport session accepted");
            if let Err(err) =
                handler.handle(state, context, authenticated, session).await
            {
                error!(
                    handler = handler.name(),
                    "WebTransport handler failed: {err}"
                );
            }
        }
    }
}
