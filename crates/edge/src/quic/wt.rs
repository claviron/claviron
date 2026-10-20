use crate::pipeline::{
    H3WebTransportBiStream, H3WebTransportSession, WebTransportHandler,
};
use crate::transport::{AnyAuthenticated, AuthContext, WebTransportBinding};
use bytes::Bytes;
use claviron_runtime::AppState;
use h3::ext::Protocol;
use h3::quic;
use h3_webtransport::server::WebTransportConnection;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error};

use crate::quic::request::send_status_response;

mod streams;
pub(super) use streams::SessionStreams;

pub(super) type H3Connection =
    WebTransportConnection<h3_quinn::Connection, Bytes>;
type H3Bidi = <h3_quinn::Connection as quic::OpenStreams<Bytes>>::BidiStream;
type H3RequestStream = h3::server::RequestStream<H3Bidi, Bytes>;

pub(crate) fn is_web_transport_connect(h3_head: &http::Request<()>) -> bool {
    h3_head.method() == http::Method::CONNECT
        && h3_head.extensions().get::<Protocol>()
            == Some(&Protocol::WEB_TRANSPORT)
}

pub(super) struct AuthorizedSession {
    handler: Arc<dyn WebTransportHandler>,
    authenticated: Option<AnyAuthenticated>,
    state: AppState,
    connect_ctx: AuthContext,
    request: http::Request<()>,
    stream: H3RequestStream,
}

impl AuthorizedSession {
    pub(super) fn id(&self) -> h3_webtransport::SessionId {
        self.stream.send_id().into()
    }
}

/// Authorization and refusal I/O run alongside incoming HTTP/3 resolution,
/// not inside the connection's accept arm.
pub(super) async fn authorize_web_transport(
    binding: WebTransportBinding,
    state: AppState,
    connect_ctx: AuthContext,
    request: http::Request<()>,
    mut stream: H3RequestStream,
) -> Option<AuthorizedSession> {
    let WebTransportBinding::Handler(handler) = binding;
    let authenticated = match handler.accept(&state, &connect_ctx).await {
        Ok(accepted) => accepted.into_authenticated(),
        Err(reject) => {
            debug!(handler = handler.name(), "WebTransport rejected");
            send_status_response(&mut stream, reject.status()).await;
            return None;
        }
    };
    Some(AuthorizedSession {
        handler,
        authenticated,
        state,
        connect_ctx,
        request,
        stream,
    })
}

/// Prepare CONNECT response I/O without borrowing the connection driver.
/// The caller owns this whole future in the connection's child task set.
// TODO(http3-metrics): CONNECT bypasses ordinary request metrics. Keep native
// WebTransport session accounting distinct from HTTP request inflight metrics.
pub(super) fn serve_web_transport(
    authorized: AuthorizedSession,
    connection: &mut H3Connection,
    incoming: mpsc::Receiver<H3WebTransportBiStream>,
) -> impl std::future::Future<Output = ()> + Send + 'static {
    let AuthorizedSession {
        handler,
        authenticated,
        state,
        connect_ctx,
        request,
        stream,
    } = authorized;
    let accepting = connection.accept_session(request, stream);
    async move {
        let session = match accepting.await {
            Ok(session) => H3WebTransportSession::new(session, incoming),
            Err(err) => {
                error!(
                    handler = handler.name(),
                    "WebTransport accept failed: {err:?}"
                );
                return;
            }
        };
        debug!(handler = handler.name(), session_id = ?session.session_id(), "WebTransport session accepted");
        let _session = claviron_runtime::activity::ACTIVITY.open_session();
        if let Err(err) =
            handler.handle(state, connect_ctx, authenticated, session).await
        {
            error!(
                handler = handler.name(),
                "WebTransport handler failed: {err}"
            );
        }
    }
}
