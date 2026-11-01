//! Application-installed execution drivers for Continuo runnable generations.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::registry::{Error, Result};
use crate::{
    ExecutionDomain, ExecutionDriver, ExecutionFuture, ExecutionSpec,
    ExecutorKind, ExecutorSpec, PreparedExecution, SchedulingPolicy,
};

mod resident;
mod thread;
use resident::ResidentExecution;
mod dedicated;
use dedicated::DedicatedExecution;

/// Tokio current-thread execution on owned OS threads.
/// Shared domains keep their I/O driver through provider finalization;
/// dedicated executors belong to one generation. Threads start lazily.
/// Only generation submission crosses this bridge, never request handling.
#[derive(Clone, Default)]
pub struct CurrentThreadDriver {
    executions: Arc<Mutex<Vec<Arc<ResidentExecution>>>>,
    domains: Arc<Mutex<Domains>>,
}

#[derive(Default)]
struct Domains {
    closed: bool,
    shared: HashMap<ExecutionDomain, Arc<ResidentExecution>>,
}

impl ExecutionDriver for CurrentThreadDriver {
    fn prepare(
        &self,
        spec: ExecutionSpec,
    ) -> Result<Arc<dyn PreparedExecution>> {
        if spec.scheduling != SchedulingPolicy::Inherit {
            return Err(Error::msg(format!(
                "current-thread driver does not support {spec:?}"
            )));
        }
        let mut domains = self.domains.lock().unwrap();
        if domains.closed {
            return Err(Error::msg("current-thread driver is finalized"));
        }
        match spec.executor {
            ExecutorSpec::Dedicated(ExecutorKind::CurrentThread) => {
                Ok(Arc::new(DedicatedExecution::new(
                    self.executions.clone(),
                    self.domains.clone(),
                )))
            }
            ExecutorSpec::Shared(domain) => {
                Ok(domains.shared.entry(domain).or_default().clone())
            }
            _ => Err(Error::msg(format!(
                "current-thread driver does not support {spec:?}"
            ))),
        }
    }

    fn drain(&self) -> ExecutionFuture {
        let executions = self.executions.clone();
        let domains = self.domains.clone();
        Box::pin(async move {
            let owned = executions.lock().unwrap().clone();
            let mut failure = None;
            for execution in owned {
                if let Err(error) = execution.finalize().await {
                    tracing::error!(%error, "current-thread execution failed during drain");
                    failure.get_or_insert(error);
                }
                executions
                    .lock()
                    .unwrap()
                    .retain(|owned| !Arc::ptr_eq(owned, &execution));
            }
            let shared = domains
                .lock()
                .unwrap()
                .shared
                .values()
                .cloned()
                .collect::<Vec<_>>();
            for domain in shared {
                if let Err(error) = domain.drain().await {
                    tracing::error!(%error, "shared current-thread execution failed during drain");
                    failure.get_or_insert(error);
                }
            }
            failure.map_or(Ok(()), Err)
        })
    }

    fn finalize(&self) -> ExecutionFuture {
        let domains = self.domains.clone();
        let drained = self.drain();
        Box::pin(async move {
            let shared = {
                let mut domains = domains.lock().unwrap();
                domains.closed = true;
                domains.shared.values().cloned().collect::<Vec<_>>()
            };
            let mut failure = drained.await.err();
            for domain in shared {
                if let Err(error) = domain.finalize().await {
                    tracing::error!(%error, "shared current-thread executor finalization failed");
                    failure.get_or_insert(error);
                }
            }
            failure.map_or(Ok(()), Err)
        })
    }
}

#[cfg(test)]
#[path = "../tests/execution.rs"]
mod tests;
