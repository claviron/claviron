//! Process-family server management through the existing Unix signal boundary.

use std::mem::size_of;

use claviron_orbit::{FleetEventCodec, FleetEventRegistry};
use claviron_runtime::events::Event;
use claviron_runtime::{AppState, RunMode};
use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use tokio::sync::broadcast;

const SERVER_RELOADED_BYTES: usize = size_of::<u64>();

/// Terminal event emitted by the main process after its complete reload cycle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ServerReloaded {
    revision: u64,
}

impl ServerReloaded {
    pub(crate) fn new(revision: u64) -> Self {
        Self { revision }
    }

    pub fn revision(self) -> u64 {
        self.revision
    }
}

impl Event for ServerReloaded {}

impl FleetEventCodec for ServerReloaded {
    const TOPIC: &'static str = "claviron.supervisor.server.reloaded.v1";

    fn encode_orbit(&self) -> Vec<u8> {
        self.revision.to_be_bytes().to_vec()
    }

    fn decode_orbit(bytes: &[u8]) -> Result<Self, String> {
        let revision = bytes.try_into().map_err(|_| {
            format!(
                "invalid server reloaded payload length: expected {SERVER_RELOADED_BYTES}, got {}",
                bytes.len()
            )
        })?;
        Ok(Self::new(u64::from_be_bytes(revision)))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ServerManagerError {
    #[error("server management is unavailable during shutdown")]
    ShuttingDown,

    #[error("control process id {0} exceeds the platform pid range")]
    InvalidControlPid(u32),

    #[error("failed to signal control process {pid}: {source}")]
    Signal {
        pid: u32,
        #[source]
        source: nix::errno::Errno,
    },

    #[error("server reload completion stream lagged by {0} event(s)")]
    ReloadCompletionLagged(u64),

    #[error("server reload completion stream closed")]
    ReloadCompletionClosed,
}

pub type ServerManagerResult<T> = Result<T, ServerManagerError>;

/// Owned domain handle for process-family server lifecycle commands.
///
/// Commands preserve the existing operator boundary by signalling the
/// process-family control PID. The master or standalone signal listeners own
/// the actual reload and shutdown lifecycles.
#[derive(Clone)]
pub struct ServerManager {
    state: AppState,
}

impl ServerManager {
    pub fn new(state: &AppState) -> Self {
        Self { state: state.clone() }
    }

    /// Request a whole-server reload.
    ///
    /// Subscribe before signalling so the terminal event cannot race the
    /// caller. The temporary receiver is removed when this future completes or
    /// is cancelled.
    pub async fn reload(&self) -> ServerManagerResult<ServerReloaded> {
        let mut completion = self.state.events().subscribe::<ServerReloaded>();
        self.signal(Signal::SIGHUP)?;
        match completion.recv().await {
            Ok(event) => Ok(event),
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                Err(ServerManagerError::ReloadCompletionLagged(skipped))
            }
            Err(broadcast::error::RecvError::Closed) => {
                Err(ServerManagerError::ReloadCompletionClosed)
            }
        }
    }

    /// Request a graceful whole-server shutdown.
    ///
    /// Success means that the kernel accepted `SIGTERM`; shutdown completion
    /// remains asynchronous and owned by the server lifecycle.
    pub fn shutdown(&self) -> ServerManagerResult<()> {
        self.signal(Signal::SIGTERM)
    }

    fn signal(
        &self,
        signal: Signal,
    ) -> ServerManagerResult<()> {
        if self.state.is_shutting_down() {
            return Err(ServerManagerError::ShuttingDown);
        }

        let control_pid = self.state.control_pid();
        let raw_pid = i32::try_from(control_pid)
            .ok()
            .filter(|pid| *pid > 0)
            .ok_or(ServerManagerError::InvalidControlPid(control_pid))?;
        kill(Pid::from_raw(raw_pid), signal).map_err(|source| {
            ServerManagerError::Signal { pid: control_pid, source }
        })
    }
}

pub(crate) fn install_events(state: &AppState) -> crate::Result<()> {
    if matches!(state.run_mode(), RunMode::Standalone) {
        return Ok(());
    }

    let event_registry = state
        .registry_ref()
        .resolve::<FleetEventRegistry>()
        .ok_or_else(|| {
        crate::Error::bootstrap(
            "FleetEventRegistry is unavailable for server reload events",
        )
    })?;
    event_registry
        .register_bridge::<ServerReloaded>(state)
        .map_err(crate::Error::Bootstrap)
}

#[cfg(test)]
#[path = "../../tests/management/server.rs"]
mod tests;
