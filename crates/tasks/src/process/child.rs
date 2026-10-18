use std::time::Duration;

use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use tokio::process::Child;
use tokio::time::timeout;

use super::instance::AttachedInstance;
use super::{ExitAction, ProcessSignal, ReloadAction};
use crate::{Error, TaskResult};

pub(super) enum Completion {
    Spawned(std::process::ExitStatus),
    Attached,
}

/// A live command, whether this runtime spawned it or attached to an existing
/// persistent instance.
pub(super) enum Running {
    Spawned(Child),
    Attached(AttachedInstance),
}

impl Running {
    pub(super) fn identity(
        &self
    ) -> std::io::Result<claviron_sys::process::ProcessIdentity> {
        match self {
            Self::Attached(instance) => Ok(instance.identity()),
            Self::Spawned(child) => child
                .id()
                .and_then(claviron_sys::process::identity)
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "command identity unavailable",
                    )
                }),
        }
    }

    #[cfg(feature = "metrics")]
    pub(super) fn pid(&self) -> u32 {
        match self {
            Self::Spawned(child) => {
                child.id().expect("running command must retain its PID")
            }
            Self::Attached(instance) => instance.pid(),
        }
    }

    pub(super) async fn wait(&mut self) -> std::io::Result<Completion> {
        match self {
            Self::Spawned(child) => child.wait().await.map(Completion::Spawned),
            Self::Attached(instance) => {
                instance.wait().await?;
                Ok(Completion::Attached)
            }
        }
    }

    fn signal(
        &mut self,
        signal: Signal,
    ) -> std::io::Result<()> {
        match self {
            Self::Spawned(child) => {
                let pid = child.id().ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "command already exited",
                    )
                })?;
                kill(Pid::from_raw(pid as i32), signal)
                    .map_err(std::io::Error::from)
            }
            Self::Attached(instance) => instance.signal(signal as i32),
        }
    }

    fn kill(&mut self) -> std::io::Result<()> {
        match self {
            Self::Spawned(child) => child.start_kill(),
            Self::Attached(instance) => instance.signal(Signal::SIGKILL as i32),
        }
    }

    pub(super) async fn apply(
        &mut self,
        action: Action,
        graceful_timeout: Option<Duration>,
    ) -> TaskResult<()> {
        match action {
            Action::KeepRunning => Ok(()),
            Action::Signal(ProcessSignal::Kill) => {
                ignore_missing(self.kill())?;
                self.wait().await.map(|_| ()).map_err(Error::Io)
            }
            Action::Signal(signal) => {
                ignore_missing(self.signal(into_signal(signal)))?;
                Ok(())
            }
            Action::Terminate => {
                ignore_missing(self.signal(Signal::SIGTERM))?;
                match graceful_timeout {
                    Some(grace) => match timeout(grace, self.wait()).await {
                        Ok(Ok(_)) => Ok(()),
                        Ok(Err(error)) => Err(Error::Io(error)),
                        Err(_) => {
                            ignore_missing(self.kill())?;
                            self.wait().await.map(|_| ()).map_err(Error::Io)
                        }
                    },
                    None => self.wait().await.map(|_| ()).map_err(Error::Io),
                }
            }
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum Action {
    Terminate,
    Signal(ProcessSignal),
    KeepRunning,
}

impl From<ExitAction> for Action {
    fn from(action: ExitAction) -> Self {
        match action {
            ExitAction::Terminate => Self::Terminate,
            ExitAction::Signal(signal) => Self::Signal(signal),
            ExitAction::KeepRunning => Self::KeepRunning,
        }
    }
}

impl From<ReloadAction> for Action {
    fn from(action: ReloadAction) -> Self {
        match action {
            ReloadAction::Restart => Self::Terminate,
            ReloadAction::Signal(signal) => Self::Signal(signal),
        }
    }
}

impl From<Option<ReloadAction>> for Action {
    fn from(action: Option<ReloadAction>) -> Self {
        action.map(Self::from).unwrap_or(Self::Terminate)
    }
}

fn ignore_missing(result: std::io::Result<()>) -> std::io::Result<()> {
    match result {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        other => other,
    }
}

fn into_signal(signal: ProcessSignal) -> Signal {
    match signal {
        ProcessSignal::Hup => Signal::SIGHUP,
        ProcessSignal::Int => Signal::SIGINT,
        ProcessSignal::Kill => Signal::SIGKILL,
        ProcessSignal::Quit => Signal::SIGQUIT,
        ProcessSignal::Term => Signal::SIGTERM,
        ProcessSignal::Usr1 => Signal::SIGUSR1,
        ProcessSignal::Usr2 => Signal::SIGUSR2,
    }
}
