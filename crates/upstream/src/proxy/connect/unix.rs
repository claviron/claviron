//! Owns timeout-bounded UNIX dialing and transport options.

use tokio::net::UnixStream;

use crate::health::probe::OriginReachabilityFailure;

pub(crate) async fn dial(
    path: &std::path::Path,
    timeout: Option<std::time::Duration>,
    source: &str,
) -> Result<UnixStream, OriginReachabilityFailure> {
    let stream = crate::proxy::body::timeout::within(timeout, UnixStream::connect(path))
        .await
        .map_err(|_| OriginReachabilityFailure::connect_timeout())?
        .map_err(|err| {
            tracing::debug!(%source, error = %err, "upstream transport setup failed");
            OriginReachabilityFailure::connect_io(&err)
        })?;

    Ok(stream)
}
