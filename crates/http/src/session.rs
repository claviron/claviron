//! Listener-owned native sessions that outlive an HTTP response.
//!
//! This composes Continuo's existing gate with Tokio task ownership. It adds
//! neither a runtime nor a shutdown deadline: the listener decides when to
//! close admission, request graceful drain and force its remaining work.

use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex, Weak};

use claviron_runtime::gate::Permit;
use futures_util::FutureExt;
use futures_util::future::{BoxFuture, Shared};
use tokio::task::JoinSet;
use tracing::error;

use crate::ListenerId;

/// Shared cooperative close signal. It owns no task or forced deadline.
pub type GracefulSignal = Shared<BoxFuture<'static, ()>>;

/// One live listener generation owns every submitted upgrade/tunnel task.
/// Drop aborts as a fallback; orderly shutdown must close and await the drain.
pub struct SessionOwner {
    inner: Arc<Inner>,
}

struct Inner {
    listener: ListenerId,
    tasks: Mutex<Option<JoinSet<()>>>,
}

impl SessionOwner {
    pub fn new(listener: ListenerId) -> Self {
        Self {
            inner: Arc::new(Inner {
                listener,
                tasks: Mutex::new(Some(JoinSet::new())),
            }),
        }
    }

    /// Bind session submissions to the original connection capacity lease.
    pub fn bind(
        &self,
        permit: Permit,
    ) -> ConnectionScope {
        let permit = Arc::new(permit);
        let sessions = SessionSpawner {
            owner: Arc::downgrade(&self.inner),
            permit: Arc::downgrade(&permit),
            graceful: None,
        };
        ConnectionScope { permit, sessions }
    }

    /// Seal submissions before signalling the listener gate's graceful drain.
    /// Submission and close are serialized: no accepted task can escape this set.
    pub fn close(self) -> SessionDrain {
        let tasks = self
            .inner
            .tasks
            .lock()
            .expect("session owner poisoned")
            .take()
            .expect("session owner already closed");
        SessionDrain { listener: self.inner.listener.clone(), tasks }
    }
}

/// The native connection retains its permit; accepted sessions share that same
/// permit rather than acquiring extra connection slots or releasing it at 101.
pub struct ConnectionScope {
    permit: Arc<Permit>,
    sessions: SessionSpawner,
}

impl ConnectionScope {
    /// Preserve a connection-specific retirement signal through native session
    /// hand-off, without stopping the listener or allocating a capacity slot.
    pub fn with_graceful_signal(
        mut self,
        signal: impl Future<Output = ()> + Send + 'static,
    ) -> Self {
        self.sessions.graceful = Some(signal.boxed().shared());
        self
    }

    pub async fn wait_graceful_shutdown(&self) {
        self.sessions.wait_graceful_shutdown().await;
    }
    pub fn permit(&self) -> &Permit {
        &self.permit
    }

    pub fn sessions(&self) -> SessionSpawner {
        self.sessions.clone()
    }
}

/// Request/session capability, injected by the accepting native transport.
/// Keeping this handle alone alive retains neither a listener nor capacity.
#[derive(Clone)]
pub struct SessionSpawner {
    owner: Weak<Inner>,
    permit: Weak<Permit>,
    graceful: Option<GracefulSignal>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionSpawnError {
    ListenerClosed,
    ConnectionClosed,
    Draining,
}

impl fmt::Display for SessionSpawnError {
    fn fmt(
        &self,
        formatter: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        formatter.write_str(match self {
            Self::ListenerClosed => "native session listener is closed",
            Self::ConnectionClosed => "native session connection is closed",
            Self::Draining => "native session listener is draining",
        })
    }
}

impl std::error::Error for SessionSpawnError {}

impl SessionSpawner {
    /// Hand off work before returning the 101/CONNECT response. The owner
    /// retains and joins it even if the HTTP connection task has already ended.
    pub fn spawn<F>(
        &self,
        future: F,
    ) -> Result<(), SessionSpawnError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let owner =
            self.owner.upgrade().ok_or(SessionSpawnError::ListenerClosed)?;
        let mut guard = owner.tasks.lock().expect("session owner poisoned");
        let tasks = guard.as_mut().ok_or(SessionSpawnError::ListenerClosed)?;
        let permit =
            self.permit.upgrade().ok_or(SessionSpawnError::ConnectionClosed)?;
        if permit.is_shutting_down()
            || self
                .graceful
                .as_ref()
                .is_some_and(|signal| signal.peek().is_some())
        {
            return Err(SessionSpawnError::Draining);
        }
        // Completed tasks retain no session resources; reap their join records
        // on subsequent submissions instead of growing a lifetime-long history.
        while let Some(result) = tasks.try_join_next() {
            report(&owner.listener, result);
        }
        tasks.spawn(async move {
            let _permit = permit;
            future.await;
        });
        Ok(())
    }

    /// A cooperative signal, not permission to drop unfinished work. Protocol
    /// handlers may initiate their own close handshake; only the owner forces.
    pub async fn wait_graceful_shutdown(&self) {
        let listener = async {
            if let Some(permit) = self.permit.upgrade() {
                permit.wait_graceful_shutdown().await;
            }
        };
        let connection = async {
            match &self.graceful {
                Some(signal) => signal.clone().await,
                None => std::future::pending().await,
            }
        };
        tokio::select! { () = listener => {}, () = connection => {} }
    }
}

/// Sealed task set. The transport supplies the force decision from its gate.
#[must_use = "native sessions must be joined before the listener stops"]
pub struct SessionDrain {
    listener: ListenerId,
    tasks: JoinSet<()>,
}

impl SessionDrain {
    pub fn abort_all(&mut self) {
        self.tasks.abort_all();
    }

    pub async fn join(mut self) {
        while let Some(result) = self.tasks.join_next().await {
            report(&self.listener, result);
        }
    }
}

fn report(
    listener: &ListenerId,
    result: Result<(), tokio::task::JoinError>,
) {
    if let Err(error) = result
        && !error.is_cancelled()
    {
        error!(%listener, %error, "native session task failed");
    }
}
