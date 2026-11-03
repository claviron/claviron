use claviron_runtime::{ExecutionDomain, ExecutionSpec, ExecutorSpec};
use serde::Deserialize;

use crate::ListenerId;

/// Execution of one listener in each serving process.
/// Changes apply to the next listener generation, not an active connection.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ListenerExecution {
    #[default]
    Shared,
    CurrentThread,
}

pub(super) fn default_listener_execution() -> ListenerExecution {
    ListenerExecution::Shared
}

impl ListenerExecution {
    /// Freeze this listener's execution domain for its next generation.
    pub fn for_listener(
        self,
        id: &ListenerId,
    ) -> ExecutionSpec {
        match self {
            Self::Shared => ExecutionSpec::default(),
            Self::CurrentThread => {
                ExecutionSpec::default().with_executor(ExecutorSpec::Shared(
                    ExecutionDomain::named::<Self>(id.clone()),
                ))
            }
        }
    }
}
