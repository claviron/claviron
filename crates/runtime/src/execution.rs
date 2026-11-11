//! Application-installed execution drivers for Continuo runnable generations.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::registry::{Error, Result};
use crate::{
    Execution, ExecutionDomain, ExecutionDriver, ExecutionFuture,
    ExecutionOwner, ExecutionSpec, ExecutorKind, ExecutorSpec,
    PreparedExecution, SchedulingPolicy,
};

mod resident;
mod thread;

use resident::ResidentExecution;

/// Resolve configured placement for the next runnable generation.
pub fn execution_spec(execution: Execution) -> ExecutionSpec {
    let spec = ExecutionSpec::default();
    match execution {
        Execution::Inherit => spec,
        Execution::Dedicated => spec.with_executor(ExecutorSpec::Dedicated(
            ExecutorKind::CurrentThread,
        )),
    }
}

/// Tokio current-thread execution on owned OS threads.
/// Executors start lazily and survive generation completion and drain.
/// Only generation submission crosses this bridge, never request handling.
#[derive(Clone, Default)]
pub struct CurrentThreadDriver {
    executors: Arc<Mutex<Executors>>,
}

#[derive(Default)]
struct Executors {
    closed: bool,
    dedicated: HashMap<ExecutionOwner, Arc<ResidentExecution>>,
    shared: HashMap<ExecutionDomain, Arc<ResidentExecution>>,
}

impl Executors {
    fn residents(&self) -> Vec<Arc<ResidentExecution>> {
        self.dedicated.values().chain(self.shared.values()).cloned().collect()
    }
}

impl ExecutionDriver for CurrentThreadDriver {
    fn prepare(
        &self,
        owner: &ExecutionOwner,
        spec: ExecutionSpec,
    ) -> Result<Arc<dyn PreparedExecution>> {
        if spec.scheduling != SchedulingPolicy::Inherit {
            return Err(Error::msg(format!(
                "current-thread driver does not support {spec:?}"
            )));
        }
        let mut executors = self.executors.lock().unwrap();
        if executors.closed {
            return Err(Error::msg("current-thread driver is finalized"));
        }
        let resident = match spec.executor {
            ExecutorSpec::Dedicated(ExecutorKind::CurrentThread) => {
                executors.dedicated.entry(owner.clone()).or_default()
            }
            ExecutorSpec::Shared(domain) => {
                executors.shared.entry(domain).or_default()
            }
            _ => {
                return Err(Error::msg(format!(
                    "current-thread driver does not support {spec:?}"
                )));
            }
        };
        Ok(resident.clone())
    }

    fn drain(&self) -> ExecutionFuture {
        let executors = self.executors.clone();
        Box::pin(async move {
            let residents = executors.lock().unwrap().residents();
            let mut failure = None;
            for resident in residents {
                if let Err(error) = resident.drain().await {
                    tracing::error!(%error, "current-thread execution failed during drain");
                    failure.get_or_insert(error);
                }
            }
            failure.map_or(Ok(()), Err)
        })
    }

    fn finalize(&self) -> ExecutionFuture {
        let executors = self.executors.clone();
        Box::pin(async move {
            let residents = {
                let mut executors = executors.lock().unwrap();
                executors.closed = true;
                executors.residents()
            };
            let mut failure = None;
            for resident in residents {
                if let Err(error) = resident.finalize().await {
                    tracing::error!(%error, "current-thread executor finalization failed");
                    failure.get_or_insert(error);
                }
            }
            let mut executors = executors.lock().unwrap();
            executors.dedicated.clear();
            executors.shared.clear();
            failure.map_or(Ok(()), Err)
        })
    }
}

#[cfg(test)]
#[path = "../tests/execution.rs"]
mod tests;
