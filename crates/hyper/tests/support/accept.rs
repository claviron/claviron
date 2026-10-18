//! Accept-error injection uses ordinary loopback sockets, never real resource
//! exhaustion. Controls have only the explicitly supplied, finite test queue.

use std::collections::VecDeque;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures_util::task::AtomicWaker;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

use crate::listener::accept::AcceptSource;

#[derive(Default)]
pub(crate) struct Control {
    errors: Mutex<VecDeque<io::Error>>,
    polls: AtomicUsize,
    errors_seen: AtomicUsize,
    transient: AtomicBool,
    waker: AtomicWaker,
    changed: Notify,
}

impl Control {
    pub(crate) fn inject(
        &self,
        error: io::ErrorKind,
    ) {
        self.inject_error(error.into());
    }

    pub(crate) fn inject_error(
        &self,
        error: io::Error,
    ) {
        self.errors.lock().unwrap().push_back(error);
        self.waker.wake();
    }

    pub(crate) fn polls(&self) -> usize {
        self.polls.load(Ordering::SeqCst)
    }

    pub(crate) fn transient(
        &self,
        enabled: bool,
    ) {
        self.transient.store(enabled, Ordering::SeqCst);
        self.waker.wake();
    }

    pub(crate) async fn wait_errors(
        &self,
        count: usize,
    ) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.errors_seen.load(Ordering::SeqCst) >= count {
                return;
            }
            changed.await;
        }
    }
}

pub(crate) struct Source {
    socket: TcpListener,
    control: Arc<Control>,
}

impl Source {
    pub(crate) fn new(socket: TcpListener) -> (Self, Arc<Control>) {
        let control = Arc::new(Control::default());
        (Self { socket, control: control.clone() }, control)
    }
}

impl AcceptSource for Source {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    fn poll_accept(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<(TcpStream, SocketAddr)>> {
        self.control.polls.fetch_add(1, Ordering::SeqCst);
        self.control.waker.register(cx.waker());
        let error =
            self.control.errors.lock().unwrap().pop_front().or_else(|| {
                self.control
                    .transient
                    .load(Ordering::SeqCst)
                    .then(|| io::ErrorKind::ConnectionAborted.into())
            });
        if let Some(error) = error {
            self.control.errors_seen.fetch_add(1, Ordering::SeqCst);
            self.control.changed.notify_waiters();
            Poll::Ready(Err(error))
        } else {
            self.socket.poll_accept(cx)
        }
    }
}
