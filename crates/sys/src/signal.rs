//! A portable, deliberately bounded set of process signals, as configuration
//! names them (`hup`, `usr1`, …).
//!
//! What a signal means is the binary's decision: a task's reload action, the
//! supervisor's restart, a log reopen. This type only names the signal, so
//! every place that lets an operator choose one reads the same words.

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Signal {
    Hup,
    Int,
    Kill,
    Quit,
    Term,
    Usr1,
    Usr2,
}

impl Signal {
    /// The platform's signal number.
    #[cfg(unix)]
    pub fn raw(self) -> libc::c_int {
        match self {
            Self::Hup => libc::SIGHUP,
            Self::Int => libc::SIGINT,
            Self::Kill => libc::SIGKILL,
            Self::Quit => libc::SIGQUIT,
            Self::Term => libc::SIGTERM,
            Self::Usr1 => libc::SIGUSR1,
            Self::Usr2 => libc::SIGUSR2,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Hup => "hup",
            Self::Int => "int",
            Self::Kill => "kill",
            Self::Quit => "quit",
            Self::Term => "term",
            Self::Usr1 => "usr1",
            Self::Usr2 => "usr2",
        }
    }
}

impl std::fmt::Display for Signal {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Signal {
    /// The signals an operator may give a server role. `term`, `int`,
    /// `quit` and `kill` end processes and belong to shutdown; they are not
    /// choosable.
    pub const OPERATOR: [Signal; 3] = [Signal::Hup, Signal::Usr1, Signal::Usr2];

    pub fn is_operator(self) -> bool {
        Self::OPERATOR.contains(&self)
    }
}

/// Which signal asks a server for what — the binary's choice, set on its
/// `Server` before it starts. The defaults are the convention nginx, PHP-FPM,
/// Gunicorn and Unicorn share: HUP reloads, USR1 reopens the log files, USR2
/// replaces the workers.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerSignals {
    pub reload: Signal,
    pub reopen_logs: Signal,
    /// A rolling restart of the workers; a standalone process has none.
    pub restart: Signal,
}

impl Default for ServerSignals {
    fn default() -> Self {
        Self {
            reload: Signal::Hup,
            reopen_logs: Signal::Usr1,
            restart: Signal::Usr2,
        }
    }
}

impl ServerSignals {
    /// Every role on an operator signal, and no two on the same one.
    pub fn validate(&self) -> Result<(), String> {
        let roles = [
            ("reload", self.reload),
            ("reopen_logs", self.reopen_logs),
            ("restart", self.restart),
        ];
        for (role, signal) in roles {
            if !signal.is_operator() {
                return Err(format!(
                    "signal for {role} must be hup, usr1 or usr2, not {signal}"
                ));
            }
        }
        for (index, (role, signal)) in roles.iter().enumerate() {
            if let Some((other, _)) =
                roles[index + 1..].iter().find(|(_, s)| s == signal)
            {
                return Err(format!(
                    "{role} and {other} cannot share signal {signal}"
                ));
            }
        }
        Ok(())
    }
}
