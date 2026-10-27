//! A worker process held together with its exact identity, handle kind and the
//! one record of its exit.

use std::process::{Child, ExitStatus};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{fmt, io};

use claviron_sys::process::{self, ProcessHandle, ProcessIdentity};
use futures_util::FutureExt;
use futures_util::future::{BoxFuture, Shared};
use nix::sys::signal::Signal;
use tokio::sync::Notify;

use crate::{Error, Result};

/// The kernel identity and handle of one spawned or explicitly adopted worker.
///
/// A spawned identity is captured while the child handle is fresh. An adopted
/// identity comes from worker-written presence and a completed ownership
/// handshake. In both cases `identity().is_alive()` can tell this process apart
/// from a later one that inherited its PID.
///
/// The handle never leaves the table that owns it. Whoever needs to know that
/// the process is gone awaits its [`ExitObservation`]; whoever needs to signal
/// it does so through the identity-validated [`ProcessHandle`] kept here; and
/// the reap happens where the handle lies, under the owner's lock.
pub(crate) struct OwnedProcess {
    identity: ProcessIdentity,
    handle: ManagedWorkerHandle,
    /// Runtime age used by rotation and operator snapshots. Spawned workers
    /// use this master's monotonic clock; adopted workers retain the wall-clock
    /// runtime start the worker itself published.
    age_origin: ProcessAgeOrigin,
    exit: Arc<ExitObservation>,
}

/// Where this master's process handle came from.
///
/// Origin is a shutdown fact, not merely a display label. The current master
/// must reap a child it spawned. An adopted process still belongs to the
/// previous parent at the kernel boundary, even after the worker accepts this
/// master as its logical supervisor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProcessOrigin {
    Spawned,
    Adopted,
}

/// How this master can wait for and signal the worker process.
pub(crate) enum ManagedWorkerHandle {
    /// A process this master spawned and therefore must reap.
    Child { child: Child, process: Arc<ProcessHandle> },
    /// A process accepted from an earlier master. The kernel handle proves
    /// exit, but only the original parent can reap it.
    Adopted(Arc<ProcessHandle>),
}

enum ProcessAgeOrigin {
    Spawned(Instant),
    RuntimeStartedAt(u64),
}

/// What the current master can know about a finished process.
pub(crate) enum ProcessExit {
    Reaped(ExitStatus),
    Observed,
}

impl fmt::Display for ProcessExit {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match self {
            Self::Reaped(status) => status.fmt(f),
            Self::Observed => f.write_str(
                "exit observed; status belongs to the previous parent",
            ),
        }
    }
}

impl OwnedProcess {
    /// Bind Orbit recovery before publishing the process handle. The
    /// observer is inert until polled; neither signalling nor withdrawal
    /// runs this callback. Exact kernel exit does, once, before waking waiters.
    pub(crate) fn recover_orbit_on_exit(
        self,
        state: &claviron_runtime::AppState,
        incarnation: u64,
    ) -> Self {
        let fleet = state.fleet_name().to_owned();
        let pid = self.pid();
        let transport = state
            .registry_ref()
            .resolve::<claviron_orbit::transport::TransportService>(
        );
        let cells =
            state.registry_ref().resolve::<claviron_orbit::CellsProvider>();
        assert!(self.exit.cleanup.set(Box::new(move || {
            if let Some(transport) = &transport {
                transport.process_exited(&fleet, pid, incarnation);
            } else {
                let result = claviron_orbit::transport::TransportSegment::existing_for_fleet(&fleet)
                    .and_then(|segment| match segment {
                        Some(segment) => segment.process_exited(pid, incarnation),
                        None => Ok(()),
                    });
                if let Err(error) = result {
                    tracing::error!(%fleet, pid, incarnation, %error, "could not release exited worker Link streams");
                }
            }
            if let Some(cells) = cells.as_ref().and_then(|provider| provider.handle()) {
                match cells.recover_text_writers() {
                    Ok(recovered) if recovered != 0 => {
                        tracing::debug!(%fleet, pid, incarnation, recovered,
                            "text cell writers recovered after worker exit");
                    }
                    Ok(_) => {},
                    Err(error) => {
                        tracing::error!(%fleet, pid, incarnation, %error,
                            "could not recover text cells after worker exit");
                    }
                }
            }
        })).is_ok(), "exit cleanup is installed only once before publication");
        self
    }

    /// Take ownership of a child this master spawned and start its one exit
    /// observer, which wakes `on_exit` once the kernel says the identity is
    /// gone.
    ///
    /// The kernel keeps a just-spawned child's record until it is reaped, so
    /// an unreadable identity means the child is already a reaped-or-reapable
    /// zombie: it is reaped here and reported as an early exit.
    pub(crate) fn from_child(
        mut child: Child,
        on_exit: Arc<Notify>,
    ) -> Result<Self> {
        let pid = child.id();
        let Some(identity) = process::identity(pid) else {
            return Err(match child.try_wait() {
                Ok(Some(status)) => Error::service(format!(
                    "process {pid} exited before it could be identified: {status}"
                )),
                _ => Error::service(format!(
                    "process {pid} was spawned but its kernel identity is unreadable"
                )),
            });
        };
        let handle = ProcessHandle::open(identity).map_err(|error| {
            Error::service(format!(
                "process {pid}: cannot open a process handle: {error}"
            ))
        })?;
        let handle = Arc::new(handle);
        let exit = Arc::new(ExitObservation::default());
        Self::observe(identity, handle.clone(), Arc::clone(&exit), on_exit);
        Ok(Self {
            identity,
            handle: ManagedWorkerHandle::Child { child, process: handle },
            age_origin: ProcessAgeOrigin::Spawned(Instant::now()),
            exit,
        })
    }

    /// Attach to a worker that explicitly accepted this master. Unlike a
    /// child, it has no wait status available here; its exact kernel identity
    /// remains asynchronously waitable and signal-safe.
    pub(crate) fn from_adopted(
        identity: ProcessIdentity,
        runtime_started_at_unix_ms: u64,
        on_exit: Arc<Notify>,
    ) -> Result<Self> {
        let handle = ProcessHandle::open(identity).map_err(|error| {
            Error::service(format!(
                "process {}: cannot adopt its exact kernel identity: {error}",
                identity.pid()
            ))
        })?;
        let handle = Arc::new(handle);
        let exit = Arc::new(ExitObservation::default());
        Self::observe(identity, handle.clone(), Arc::clone(&exit), on_exit);
        Ok(Self {
            identity,
            handle: ManagedWorkerHandle::Adopted(handle),
            age_origin: ProcessAgeOrigin::RuntimeStartedAt(
                runtime_started_at_unix_ms,
            ),
            exit,
        })
    }

    /// The one observer of this identity. It proves that the exact process is
    /// gone and nothing more; the reap belongs to the owner of the handle.
    fn observe(
        identity: ProcessIdentity,
        handle: Arc<ProcessHandle>,
        exit: Arc<ExitObservation>,
        on_exit: Arc<Notify>,
    ) {
        let observed = Arc::downgrade(&exit);
        let future = async move {
            match handle.wait().await {
                Ok(()) => {
                    if let Some(exit) = observed.upgrade() { exit.mark_exited(); }
                    on_exit.notify_waiters();
                }
                Err(error) => {
                    // Failure to observe is not evidence of death. Reaping
                    // may still establish it; never release ownership here.
                    tracing::error!(pid = identity.pid(), %error, "process exit observation failed");
                    std::future::pending::<()>().await;
                }
            }
        }.boxed().shared();
        let _ = exit.observation.set(future);
    }

    pub(crate) const fn identity(&self) -> ProcessIdentity {
        self.identity
    }

    pub(crate) const fn pid(&self) -> u32 {
        self.identity.pid()
    }

    /// How long this worker runtime has been alive according to its handle kind.
    pub(crate) fn age(&self) -> Duration {
        match self.age_origin {
            ProcessAgeOrigin::Spawned(started_at) => started_at.elapsed(),
            ProcessAgeOrigin::RuntimeStartedAt(started_at) => UNIX_EPOCH
                .checked_add(Duration::from_millis(started_at))
                .and_then(|started_at| {
                    SystemTime::now().duration_since(started_at).ok()
                })
                .unwrap_or_default(),
        }
    }

    /// The shared record of this process's exit, for anyone who must wait on it.
    pub(crate) fn exit(&self) -> Arc<ExitObservation> {
        Arc::clone(&self.exit)
    }

    pub(crate) fn has_exited(&self) -> bool {
        self.exit.is_exited()
    }

    /// Send `signal` to exactly this process. `NotFound`/`ESRCH` means it is
    /// already gone.
    pub(crate) fn signal(
        &self,
        signal: Signal,
    ) -> io::Result<()> {
        match &self.handle {
            ManagedWorkerHandle::Child { process, .. }
            | ManagedWorkerHandle::Adopted(process) => {
                process.signal(signal as i32)
            }
        }
    }

    /// Observe exit, reaping only when this master owns the child relationship.
    /// Idempotent: a reaped child reports its status again, while an adopted
    /// process reports an observed exit without inventing a status.
    pub(crate) fn try_reap(&mut self) -> io::Result<Option<ProcessExit>> {
        match &mut self.handle {
            ManagedWorkerHandle::Child { child, .. } => {
                if let Some(status) = self.exit.status() {
                    return Ok(Some(ProcessExit::Reaped(status)));
                }
                let status = child.try_wait()?;
                if let Some(status) = status {
                    self.exit.record(status);
                }
                Ok(status.map(ProcessExit::Reaped))
            }
            ManagedWorkerHandle::Adopted(_) => {
                if self.exit.is_exited() || !self.identity.is_alive() {
                    self.exit.mark_exited();
                    Ok(Some(ProcessExit::Observed))
                } else {
                    Ok(None)
                }
            }
        }
    }

    pub(crate) const fn is_adopted(&self) -> bool {
        matches!(self.origin(), ProcessOrigin::Adopted)
    }

    pub(crate) const fn origin(&self) -> ProcessOrigin {
        match &self.handle {
            ManagedWorkerHandle::Child { .. } => ProcessOrigin::Spawned,
            ManagedWorkerHandle::Adopted(_) => ProcessOrigin::Adopted,
        }
    }

    #[cfg(test)]
    pub(crate) fn child_mut(&mut self) -> &mut Child {
        match &mut self.handle {
            ManagedWorkerHandle::Child { child, .. } => child,
            ManagedWorkerHandle::Adopted(_) => {
                panic!("an adopted process has no child handle")
            }
        }
    }
}

/// Everything that is known about one process's exit, in one place.
///
/// `exited` is set by the observer as soon as the kernel no longer knows the
/// identity, and by the reap; `status` only by the reap. Waiters do not poll a
/// handle of their own — they await this, and there is one per process.
#[derive(Default)]
pub(crate) struct ExitObservation {
    exited: AtomicBool,
    status: OnceLock<ExitStatus>,
    changed: Notify,
    /// Inert until a service generation or a drain waiter polls it. Shared
    /// observation, not a detached runtime task; cancellation never signals.
    observation: OnceLock<Shared<BoxFuture<'static, ()>>>,
    cleanup: OnceLock<Box<dyn Fn() + Send + Sync>>,
    cleaned: OnceLock<()>,
}

impl ExitObservation {
    pub(crate) fn is_exited(&self) -> bool {
        self.exited.load(Ordering::Acquire)
    }

    pub(crate) fn status(&self) -> Option<ExitStatus> {
        self.status.get().copied()
    }

    fn mark_exited(&self) {
        self.cleaned.get_or_init(|| {
            if let Some(cleanup) = self.cleanup.get() {
                cleanup();
            }
        });
        self.exited.store(true, Ordering::Release);
        self.changed.notify_waiters();
    }

    fn record(
        &self,
        status: ExitStatus,
    ) {
        let _ = self.status.set(status);
        self.mark_exited();
    }

    /// Resolve once the process is known to be gone; immediately if it already is.
    pub(crate) async fn exited(&self) {
        loop {
            // Created before the check so a notification between the check and
            // the await is not lost.
            let changed = self.changed.notified();
            if self.is_exited() {
                return;
            }
            if let Some(observation) = self.observation.get() {
                tokio::select! {
                    _ = changed => {},
                    _ = observation.clone() => {},
                }
            } else {
                changed.await;
            }
        }
    }
}

#[cfg(test)]
#[path = "../../tests/process/owned.rs"]
mod tests;
