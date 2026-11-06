//! The cleartext listener's handler for streams accepted elsewhere.
//!
//! Driving the connection is `claviron_transport::tcp::plain`; what this
//! driver adds is in `handler.rs`: selecting a host from each request's
//! authority.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use claviron_http::session::ConnectionScope;
use tokio::net::TcpStream;

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
        claviron_transport::tcp::plain::serve(
            stream,
            peer,
            self.snapshot,
            scope,
        )
    }
}
