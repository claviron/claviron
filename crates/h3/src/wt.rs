use bytes::Bytes;
use claviron_edge::transport::{AuthContext, WebTransportBinding};
use claviron_runtime::AppState;
use h3::ext::Protocol;
use h3::quic;
use h3_webtransport::server::WebTransportSession;
use tracing::{debug, error};

use crate::request::send_status_response;

type H3Connection = h3::server::Connection<h3_quinn::Connection, Bytes>;
type H3Bidi = <h3_quinn::Connection as quic::OpenStreams<Bytes>>::BidiStream;
type H3RequestStream = h3::server::RequestStream<H3Bidi, Bytes>;

pub(crate) fn is_web_transport_connect(h3_head: &http::Request<()>) -> bool {
    h3_head.method() == http::Method::CONNECT
        && h3_head.extensions().get::<Protocol>()
            == Some(&Protocol::WEB_TRANSPORT)
}

// TODO(http3-metrics): WebTransport CONNECT bypasses Axum/Tower request
// middleware, so normal HTTP request inflight metrics never see this
// long-lived session. Add a dedicated H3/WebTransport inflight metric here
// (for example `claviron_http3_webtransport_inflight`) and remove this note.
pub(crate) async fn serve_web_transport(
    binding: WebTransportBinding,
    state: AppState,
    connect_ctx: AuthContext,
    request: http::Request<()>,
    mut stream: H3RequestStream,
    conn: H3Connection,
) {
    let WebTransportBinding::Handler(handler) = binding;
    let handler_name = handler.name();

    let authenticated = match handler.accept(&state, &connect_ctx).await {
        Ok(accepted) => accepted.into_authenticated(),
        Err(reject) => {
            debug!("WebTransport rejected handler={handler_name}");
            send_status_response(&mut stream, reject.status()).await;
            return;
        }
    };

    let session = match WebTransportSession::accept(request, stream, conn).await
    {
        Ok(session) => session,
        Err(err) => {
            error!(
                "WebTransport accept failed handler={handler_name}: {err:?}"
            );
            return;
        }
    };

    debug!(
        "WebTransport session accepted handler={} session_id={:?}",
        handler_name,
        session.session_id()
    );

    let _session = claviron_runtime::activity::ACTIVITY.open_session();
    if let Err(err) =
        handler.handle(state, connect_ctx, authenticated, session).await
    {
        error!("WebTransport handler failed handler={handler_name}: {err}");
    }
}
