use claviron_runtime::{ExecutionDomain, ExecutionSpec, ExecutorSpec};
use serde::Deserialize;

use crate::ListenerId;

/// Execution of plain HTTP listeners in each serving process.
/// Changes apply to the next HttpServer generation, not an active connection.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum PlainExecution {
    #[default]
    Shared,
    CurrentThread,
}

pub(super) fn default_plain_execution() -> PlainExecution {
    PlainExecution::Shared
}

impl PlainExecution {
    /// Freeze this listener's execution domain for its next generation.
    pub fn for_listener(
        self,
        id: &ListenerId,
    ) -> ExecutionSpec {
        match self {
            Self::Shared => ExecutionSpec::default(),
            Self::CurrentThread => {
                ExecutionSpec::default().with_executor(ExecutorSpec::Shared(
                    ExecutionDomain::named::<Self>(id.as_str()),
                ))
            }
        }
    }
}

impl From<PlainExecution> for ExecutionSpec {
    fn from(value: PlainExecution) -> Self {
        match value {
            PlainExecution::Shared => Self::default(),
            PlainExecution::CurrentThread => Self::default().with_executor(
                ExecutorSpec::Shared(ExecutionDomain::of::<PlainExecution>()),
            ),
        }
    }
}
