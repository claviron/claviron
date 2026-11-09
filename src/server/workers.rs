//! Resolve a primary process's worker selection before supervisor bootstrap.

use std::num::NonZeroUsize;

use claviron_runtime::{WorkerCount, registry};

/// Initial worker-process selection. The master resolves it once; worker
/// re-execs inherit the concrete count through the supervisor envelope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Workers {
    /// An explicit count; zero runs standalone.
    Count(u8),
    /// One worker per OS-reported available logical CPU. This is independent
    /// of Tokio runtime threads and listener execution placement.
    Available,
}

impl From<u8> for Workers {
    fn from(count: u8) -> Self {
        Self::Count(count)
    }
}

impl Workers {
    pub(super) fn resolve(
        self,
        available: impl FnOnce() -> std::io::Result<NonZeroUsize>,
    ) -> registry::Result<WorkerCount> {
        let count = match self {
            Self::Count(count) => count,
            Self::Available => {
                let count = available().map_err(|error| registry::Error::msg(
                    format!("cannot determine available worker parallelism: {error}")
                ))?.get();
                u8::try_from(count).map_err(|_| registry::Error::msg(
                    format!("available CPU count {count} exceeds the supported worker count {}", u8::MAX)
                ))?
            }
        };
        Ok(WorkerCount::new(count))
    }
}

#[cfg(test)]
#[path = "../../tests/server/workers.rs"]
mod tests;
