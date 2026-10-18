//! Owns timeout-bounded TCP dialing and transport options.

use tokio::net::TcpStream;

use crate::health::probe::OriginReachabilityFailure;

pub(crate) async fn dial(
    addr: &std::net::SocketAddr,
    timeout: Option<std::time::Duration>,
    nodelay: bool,
    source: &str,
) -> Result<TcpStream, OriginReachabilityFailure> {
    let stream = crate::proxy::body::timeout::within(timeout, TcpStream::connect(addr))
        .await
        .map_err(|_| OriginReachabilityFailure::connect_timeout())?
        .map_err(|err| {
            tracing::debug!(%source, error = %err, "upstream transport setup failed");
            OriginReachabilityFailure::connect_io(&err)
        })?;
    stream.set_nodelay(nodelay).map_err(|err| {
        tracing::debug!(%source, error = %err, "upstream transport setup failed");
        OriginReachabilityFailure::connect_io(&err)
    })?;

    Ok(stream)
}
