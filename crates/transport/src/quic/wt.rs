//! WebTransport over one H3 connection: recognizing a CONNECT, authorizing it
//! beside the connection's other work and handing the session to its owner.

use std::future::Future;

use bytes::Bytes;
use h3::ext::Protocol;
use h3::quic;
use h3_webtransport::server::{
    AcceptedBi, BiStreamResolver, IncomingStream, WebTransportConnection,
    WebTransportSession,
};
use http::{Request, StatusCode};
use tokio::sync::mpsc;
use tracing::{debug, error};

use super::request::send_status_response;

pub mod streams;

pub type H3Connection = WebTransportConnection<h3_quinn::Connection, Bytes>;
type HttpConnection = h3::server::Connection<h3_quinn::Connection, Bytes>;

/// Keep the normal H3 driver free of WebTransport's session registry when the
/// selected host did not advertise it. Both drivers resolve headers outside
/// the connection's accept loop.
pub(super) enum Connection {
    Http(HttpConnection),
    WebTransport(H3Connection),
}

#[allow(
    clippy::large_enum_variant,
    reason = "The connection loop consumes each accept result immediately. Keeping the resolver inline avoids a separate allocation for each accepted bidirectional stream."
)]
pub(super) enum Incoming {
    Bidi(BidiResolver),
    Ignored,
}

pub(super) enum BidiResolver {
    Http(h3::server::RequestResolver<h3_quinn::Connection, Bytes>),
    WebTransport(BiStreamResolver<h3_quinn::Connection, Bytes>),
}

impl Connection {
    pub(super) async fn accept(
        &mut self
    ) -> Result<Option<Incoming>, h3::error::ConnectionError> {
        match self {
            Self::Http(connection) => Ok(connection
                .accept()
                .await?
                .map(|resolver| Incoming::Bidi(BidiResolver::Http(resolver)))),
            Self::WebTransport(connection) => {
                Ok(connection.accept().await?.map(|stream| match stream {
                    IncomingStream::Bidirectional(resolver) => {
                        Incoming::Bidi(BidiResolver::WebTransport(resolver))
                    }
                    // Current handlers consume reliable bidirectional streams.
                    IncomingStream::Unidirectional(_, _) => Incoming::Ignored,
                }))
            }
        }
    }

    pub(super) async fn shutdown(
        &mut self,
        max_requests: usize,
    ) -> Result<(), h3::error::ConnectionError> {
        match self {
            Self::Http(connection) => connection.shutdown(max_requests).await,
            Self::WebTransport(connection) => {
                connection.shutdown(max_requests).await
            }
        }
    }
}

impl BidiResolver {
    pub(super) async fn resolve(
        self
    ) -> Result<AcceptedBi<h3_quinn::Connection, Bytes>, h3::error::StreamError>
    {
        match self {
            Self::Http(resolver) => {
                let (head, stream) = resolver.resolve_request().await?;
                Ok(AcceptedBi::Request(head, stream))
            }
            Self::WebTransport(resolver) => resolver.resolve().await,
        }
    }
}

/// An accepted session, before its owner wraps it.
pub type NativeSession = WebTransportSession<h3_quinn::Connection, Bytes>;
/// A reliable incoming WebTransport bidirectional stream.
pub type BiStream =
    h3_webtransport::stream::BidiStream<h3_quinn::BidiStream<Bytes>, Bytes>;
type H3Bidi = <h3_quinn::Connection as quic::OpenStreams<Bytes>>::BidiStream;
type H3RequestStream = h3::server::RequestStream<H3Bidi, Bytes>;

pub(super) fn is_web_transport_connect(head: &Request<()>) -> bool {
    head.method() == http::Method::CONNECT
        && head.extensions().get::<Protocol>() == Some(&Protocol::WEB_TRANSPORT)
}

/// The owner of one authorized session.
pub trait QuicSession: Send + 'static {
    fn name(&self) -> &'static str;

    /// Run the accepted session until it ends. `incoming` carries the bidi
    /// streams the connection's driver routes to it.
    fn run(
        self,
        session: NativeSession,
        incoming: mpsc::Receiver<BiStream>,
    ) -> impl Future<Output = ()> + Send + 'static;
}

/// A CONNECT its owner agreed to take.
pub struct Authorized<S> {
    pub request: Request<()>,
    pub session: S,
}

pub(super) struct AuthorizedSession<S> {
    authorized: Authorized<S>,
    stream: H3RequestStream,
}

impl<S> AuthorizedSession<S> {
    pub(super) fn id(&self) -> h3_webtransport::SessionId {
        self.stream.send_id().into()
    }
}

/// Authorization and refusal I/O run alongside incoming HTTP/3 resolution,
/// not inside the connection's accept arm.
pub(super) async fn authorize<S, F>(
    deciding: F,
    mut stream: H3RequestStream,
) -> Option<AuthorizedSession<S>>
where
    F: Future<Output = Result<Authorized<S>, StatusCode>>,
{
    match deciding.await {
        Ok(authorized) => Some(AuthorizedSession { authorized, stream }),
        Err(status) => {
            debug!(%status, "WebTransport rejected");
            send_status_response(&mut stream, status).await;
            None
        }
    }
}

/// Prepare CONNECT response I/O without borrowing the connection driver.
/// The caller owns this whole future in the connection's child task set.
// TODO(http3-metrics): CONNECT bypasses ordinary request metrics. Keep native
// WebTransport session accounting distinct from HTTP request inflight metrics.
pub(super) fn serve_session<S: QuicSession>(
    authorized: AuthorizedSession<S>,
    connection: &mut H3Connection,
    incoming: mpsc::Receiver<BiStream>,
) -> impl Future<Output = ()> + Send + 'static {
    let AuthorizedSession {
        authorized: Authorized { request, session },
        stream,
    } = authorized;
    let accepting = connection.accept_session(request, stream);
    async move {
        let native = match accepting.await {
            Ok(native) => native,
            Err(err) => {
                error!(
                    handler = session.name(),
                    "WebTransport accept failed: {err:?}"
                );
                return;
            }
        };
        let _session = claviron_runtime::activity::ACTIVITY.open_session();
        session.run(native, incoming).await;
    }
}
