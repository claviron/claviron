//! Owns transport-only TCP and Unix reachability probing.

use tokio::net::{TcpStream, UnixStream};

use crate::health::probe::OriginReachabilityFailure;
use crate::origin::{ConnectTarget, Origin};
use crate::proxy::service::UpstreamService;

impl UpstreamService {
    /// TCP/Unix connect only: no HTTP handshake or application-health verdict.
    pub(crate) async fn probe_origin_reachability(
        &self,
        origin: &Origin,
    ) -> Result<(), OriginReachabilityFailure> {
        let connect_timeout = self.inner.profile.connection.connect_timeout;
        match &origin.connect {
            ConnectTarget::Tls(addr) => {
                let _stream = crate::proxy::body::timeout::within(
                    connect_timeout,
                    TcpStream::connect(addr),
                )
                .await
                .map_err(|_| OriginReachabilityFailure::connect_timeout())?
                .map_err(|err| OriginReachabilityFailure::connect_io(&err))?;
            }
            ConnectTarget::Tcp(addr) => {
                let _stream = crate::proxy::body::timeout::within(
                    connect_timeout,
                    TcpStream::connect(addr),
                )
                .await
                .map_err(|_| OriginReachabilityFailure::connect_timeout())?
                .map_err(|err| OriginReachabilityFailure::connect_io(&err))?;
            }
            ConnectTarget::Unix(path) => {
                let _stream = crate::proxy::body::timeout::within(
                    connect_timeout,
                    UnixStream::connect(path),
                )
                .await
                .map_err(|_| OriginReachabilityFailure::connect_timeout())?
                .map_err(|err| OriginReachabilityFailure::connect_io(&err))?;
            }
        }
        Ok(())
    }
}
