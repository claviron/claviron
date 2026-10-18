//! Hands upgraded IO to the accepting listener; retains outbound capacity.

use std::time::Duration;

use claviron_http::session::{SessionSpawnError, SessionSpawner};
use hyper::upgrade::OnUpgrade;
use hyper_util::rt::TokioIo;
use tokio::io::AsyncWriteExt;

/// A tunnel is a byte copy with no request to finish, so a drain would wait
/// on it forever. When the listener enters graceful drain (worker shutdown)
/// or the profile enters Stopping, the tunnel closes both halves in order:
/// each `shutdown()` sends TLS `close_notify` and the TCP FIN, and the peer
/// reads a clean end of stream instead of an aborted socket. The close itself
/// is bounded by the profile's `lifecycle.drain_timeout`; no WebSocket close
/// frame is invented, since this crate never parses frames.
pub(crate) fn submit<C: Send + 'static>(
    sessions: &SessionSpawner,
    client: OnUpgrade,
    upstream: OnUpgrade,
    lease: crate::proxy::lease::RequestLease,
    capacity: C,
    drain_timeout: Duration,
) -> Result<(), SessionSpawnError> {
    // Upgraded IO is no longer polled by the native driver. Its origin token
    // must therefore cancel the copy explicitly during origin retirement.
    let origin_force =
        lease.origin.as_ref().expect("selected origin").transport.force.clone();
    let tasks = lease.epoch.io.clone();
    let origin_tasks =
        lease.origin.as_ref().expect("selected origin").transport.io.clone();
    let listener = sessions.clone();
    sessions.spawn(tasks.track_future(origin_tasks.track_future(async move {
        let _capacity = capacity;
        let copy = async {
            let (client, upstream) = match tokio::try_join!(client, upstream) {
                Ok(io) => io,
                Err(error) => {
                    tracing::debug!(%error, "http upstream upgrade wait failed");
                    return;
                }
            };
            let mut client = TokioIo::new(client);
            let mut upstream = TokioIo::new(upstream);
            let stopping = async {
                tokio::select! {
                    _ = listener.wait_graceful_shutdown() => {},
                    _ = lease.epoch.background.cancelled() => {}
                }
            };
            let outcome = tokio::select! { biased;
                result = tokio::io::copy_bidirectional(&mut client, &mut upstream) => Some(result),
                _ = stopping => None
            };
            match outcome {
                Some(Ok(_)) => {}
                // A TLS peer that closes the socket without close_notify
                // surfaces as UnexpectedEof from rustls. For a byte tunnel
                // that is the peer hanging up, not a transport fault.
                Some(Err(error)) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                    tracing::trace!(%error, "http upstream tunnel ended by peer close")
                }
                Some(Err(error)) => {
                    tracing::debug!(%error, "http upstream tunnel ended with error")
                }
                None => {
                    let closed = tokio::time::timeout(drain_timeout, async {
                        tokio::join!(client.shutdown(), upstream.shutdown())
                    })
                    .await;
                    match closed {
                        Ok((Ok(()), Ok(()))) => {
                            tracing::debug!("http upstream tunnel closed for drain")
                        }
                        Ok((client, upstream)) => tracing::debug!(
                            client = ?client.err(),
                            upstream = ?upstream.err(),
                            "http upstream tunnel closed for drain with a peer error"
                        ),
                        Err(_) => tracing::debug!(
                            timeout = ?drain_timeout,
                            "http upstream tunnel close did not complete within drain_timeout"
                        )
                    }
                }
            }
        };
        tokio::select! { biased; _ = lease.epoch.force.cancelled() => {}, _ = origin_force.cancelled() => {}, _ = copy => {} }
        drop(lease);
    })))
}
