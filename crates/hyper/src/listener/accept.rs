//! One persistent accept/recovery state per configured socket. The timer is
//! reused, and errors never create tasks or retain an accepted connection.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use claviron_http::ListenerId;
use claviron_http::config::AcceptRetryConfig;
use claviron_sys::net::{AcceptErrorClass, classify_accept_error};
use futures_util::Stream;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Instant, Sleep};
use tracing::{info, warn};

/// The narrow injection seam used by deterministic fault tests. Production
/// always uses Tokio's listener, with its ordinary readiness registration.
pub(super) trait AcceptSource: Unpin {
    fn local_addr(&self) -> io::Result<SocketAddr>;
    fn poll_accept(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<(TcpStream, SocketAddr)>>;
}

impl AcceptSource for TcpListener {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.local_addr()
    }

    fn poll_accept(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<(TcpStream, SocketAddr)>> {
        self.poll_accept(cx)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("listener '{listener}': accept on {address}: {source}")]
pub(super) struct AcceptFailure {
    listener: ListenerId,
    address: SocketAddr,
    #[source]
    source: io::Error,
}

#[derive(Default)]
struct Recovery {
    failures: u64,
    delay: Duration,
    began: Option<Instant>,
}

impl Recovery {
    /// The boolean authorizes exactly one warning per resource-pressure
    /// episode. Only successful accept resets the episode and exponential cap.
    fn failed(
        &mut self,
        policy: &AcceptRetryConfig,
        now: Instant,
    ) -> bool {
        let first = self.failures == 0;
        self.delay = if first {
            self.began = Some(now);
            policy.initial
        } else {
            self.delay.saturating_mul(2).min(policy.max)
        };
        self.failures = self.failures.saturating_add(1);
        first
    }

    fn recovered(&mut self) -> Option<(u64, Duration)> {
        let began = self.began?;
        let summary = (self.failures, began.elapsed());
        *self = Self::default();
        Some(summary)
    }
}

pub(super) struct SocketAccept<A> {
    source: A,
    listener: ListenerId,
    address: SocketAddr,
    policy: AcceptRetryConfig,
    recovery: Recovery,
    timer: Pin<Box<Sleep>>,
    waiting: bool,
    failed: bool,
}

impl<A: AcceptSource> SocketAccept<A> {
    pub(super) fn new(
        source: A,
        listener: ListenerId,
        policy: AcceptRetryConfig,
    ) -> io::Result<Self> {
        Ok(Self {
            address: source.local_addr()?,
            source,
            listener,
            policy,
            recovery: Recovery::default(),
            timer: Box::pin(tokio::time::sleep(Duration::ZERO)),
            waiting: false,
            failed: false,
        })
    }
}

impl<A: AcceptSource> Stream for SocketAccept<A> {
    type Item = Result<(TcpStream, SocketAddr), AcceptFailure>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.failed {
            return Poll::Ready(None);
        }
        if this.waiting {
            if this.timer.as_mut().poll(cx).is_pending() {
                return Poll::Pending;
            }
            this.waiting = false;
        }
        match this.source.poll_accept(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(accepted)) => {
                if let Some((failures, elapsed)) = this.recovery.recovered() {
                    info!(
                        listener = %this.listener, address = %this.address,
                        failures, ?elapsed, "TCP accept recovered from resource pressure"
                    );
                }
                Poll::Ready(Some(Ok(accepted)))
            }
            Poll::Ready(Err(error)) => match classify_accept_error(&error) {
                AcceptErrorClass::ResourcePressure => {
                    let now = Instant::now();
                    if this.recovery.failed(&this.policy, now) {
                        warn!(
                            listener = %this.listener, address = %this.address,
                            %error, retry = ?this.recovery.delay,
                            "TCP accept resource pressure; backing off this socket"
                        );
                    }
                    let Some(deadline) = now.checked_add(this.recovery.delay)
                    else {
                        this.failed = true;
                        return Poll::Ready(Some(Err(AcceptFailure {
                            listener: this.listener.clone(),
                            address: this.address,
                            source: io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "accept retry deadline overflow",
                            ),
                        })));
                    };
                    this.timer.as_mut().reset(deadline);
                    this.waiting = true;
                    // Register this deadline before yielding. No socket/peer
                    // future remains active while the socket is cooling down.
                    if this.timer.as_mut().poll(cx).is_ready() {
                        // A preemption or blocked log sink may have consumed
                        // the whole delay already; Ready registers no wake.
                        cx.waker().wake_by_ref();
                    }
                    Poll::Pending
                }
                AcceptErrorClass::Transient => {
                    // At most one failed syscall per poll, with no peer log.
                    // Yield to other sockets, completion polling and shutdown.
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                AcceptErrorClass::Fatal => {
                    this.failed = true;
                    Poll::Ready(Some(Err(AcceptFailure {
                        listener: this.listener.clone(),
                        address: this.address,
                        source: error,
                    })))
                }
            },
        }
    }
}

#[cfg(test)]
#[path = "../../tests/unit/accept.rs"]
mod tests;
