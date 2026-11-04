//! Driving one cleartext H1 connection.
//!
//! The mirror of `secure/conn.rs`: set up the socket, build the connection
//! context, and hand each request to `handler.rs` until the peer or a shutdown
//! ends it.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use claviron_http::ext::ConnCtx;
use claviron_http::listener::HttpProtocol;
use claviron_http::session::ConnectionScope;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;
use tracing::{debug, warn};

use super::handler::H1Service;
use super::server::ServerSnapshot;

/// A prepared cleartext listener's native handler for accepted TCP streams.
/// The caller owns acceptance, connection capacity and session drain. Each
/// stream keeps the native host-selection cache, HTTP driver and shutdown path.
#[derive(Clone)]
pub struct ConnectionHandler {
    snapshot: Arc<ServerSnapshot>,
}

impl ConnectionHandler {
    pub(super) fn new(snapshot: Arc<ServerSnapshot>) -> Self {
        Self { snapshot }
    }

    /// Serve one accepted stream until peer completion or its owned drain.
    /// No provider boot, socket bind or new connection permit is performed.
    pub fn serve(
        self,
        stream: TcpStream,
        peer: SocketAddr,
        scope: ConnectionScope,
    ) -> impl Future<Output = ()> + Send {
        let Self { snapshot } = self;
        serve(stream, peer, snapshot, scope)
    }
}

async fn serve(
    stream: TcpStream,
    peer: SocketAddr,
    snapshot: Arc<ServerSnapshot>,
    scope: ConnectionScope,
) {
    if let Err(error) = crate::tcp::protocol::socket(&stream, &snapshot.tcp) {
        warn!(listener = %snapshot.listener.id, %error, "TCP socket setup failed");
        return;
    }
    let local = match stream.local_addr() {
        Ok(local) => local,
        Err(error) => {
            warn!(listener = %snapshot.listener.id, %error, "H1 local address unavailable");
            return;
        }
    };
    let ctx = Arc::new(ConnCtx {
        state: Default::default(),
        listener: snapshot.listener.id.clone(),
        proto: Some(HttpProtocol::H1),
        remote: peer,
        local_ip: local.ip(),
        local_port: local.port(),
        real_ip: None,
        country_code: crate::tcp::geo::country_code(peer.ip()),
        tls: None,
    });
    let service = H1Service::new(snapshot.clone(), ctx);
    let service = service_fn(move |req| service.call(req));
    let (stream, errors) = crate::tcp::io::TcpIo::split(stream);
    let connection = crate::tcp::protocol::h1(&snapshot.h1)
        .serve_connection(TokioIo::new(stream), service);
    #[cfg(feature = "websocket")]
    let connection = connection.with_upgrades();
    tokio::pin!(connection);
    let serving = async {
        tokio::select! {
            biased;
            _ = scope.permit().wait_graceful_shutdown() => {
                connection.as_mut().graceful_shutdown();
                tokio::select! {
                    result = connection.as_mut() => Some(result),
                    _ = scope.permit().wait_forced_shutdown() => None
                }
            }
            result = connection.as_mut() => Some(result)
        }
    };
    let result = tokio::select! {
        biased;
        error = errors.failed() => {
            debug!(listener = %snapshot.listener.id, %error, "H1 socket failed");
            None
        },
        result = serving => result,
    };
    if let Some(Err(error)) = result
        && !error.is_user()
    {
        debug!(listener = %snapshot.listener.id, %error, "H1 connection ended");
    }
}
