//! Execution-owned environment: a one-shot read or a daemon's RAII watch.

use std::collections::HashMap;
use std::future::{Future, pending};
use std::path::Path;
use std::pin::Pin;

use claviron_runtime::AppState;
use dotenw::{EnvNotification, Snapshot};
use tokio_util::sync::CancellationToken;

use crate::{Error, Reason, TaskResult};

pub(crate) struct ExecutionEnvironment {
    snapshot: Snapshot,
    notification: Option<EnvNotification>,
}

impl ExecutionEnvironment {
    pub(crate) fn prepare(
        state: &AppState,
        file: Option<&Path>,
        values: &HashMap<String, String>,
        persistent: bool,
    ) -> TaskResult<Self> {
        let values =
            values.iter().map(|(key, value)| (key.clone(), value.clone()));
        if persistent {
            let notification = state
                .observe_program_environment(file, values)
                .map_err(|error| Error::other(error.to_string()))?;
            let snapshot = notification
                .snapshot()
                .map_err(|error| Error::other(error.to_string()))?;
            Ok(Self { snapshot, notification: Some(notification) })
        } else {
            let snapshot = state
                .read_program_environment(file, values)
                .map_err(|error| Error::other(error.to_string()))?;
            Ok(Self { snapshot, notification: None })
        }
    }

    pub(crate) fn values(&self) -> &HashMap<String, String> {
        &self.snapshot
    }

    /// Errors leave the last valid execution values intact. The caller keeps
    /// supervising its live execution and waits for a corrected source.
    pub(crate) async fn notified(&mut self) -> TaskResult<()> {
        let Some(notification) = &mut self.notification else {
            return pending().await;
        };
        self.snapshot = notification
            .notified()
            .await
            .map_err(|error| Error::other(error.to_string()))?;
        Ok(())
    }
}

/// A replacement must not start until the executor has joined its owned work.
/// Its adapter, not this layer, applies the task's graceful timeout.
pub(crate) async fn drain_for_environment<F>(
    execution: Pin<&mut F>,
    cancellation: &CancellationToken,
) -> TaskResult<()>
where
    F: Future<Output = TaskResult<()>> + ?Sized,
{
    cancellation.cancel();
    match execution.await {
        Ok(()) | Err(Error::Exit(Reason::Cancelled | Reason::Shutdown)) => {
            Ok(())
        }
        Err(error) => Err(error),
    }
}

#[cfg(test)]
#[path = "../tests/environment.rs"]
mod tests;
