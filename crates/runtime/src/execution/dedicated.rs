use std::sync::{Arc, Mutex};

use super::{Domains, ResidentExecution};
use crate::registry::Error;
use crate::{ExecutionFuture, PreparedExecution, RunContext};

/// One prepared generation owns one reactor. Child submissions reuse it;
/// only the root's completion closes and physically joins the reactor.
pub(super) struct DedicatedExecution {
    execution: Arc<ResidentExecution>,
    owned: Arc<Mutex<Vec<Arc<ResidentExecution>>>>,
    domains: Arc<Mutex<Domains>>,
}

impl DedicatedExecution {
    pub(super) fn new(
        owned: Arc<Mutex<Vec<Arc<ResidentExecution>>>>,
        domains: Arc<Mutex<Domains>>,
    ) -> Self {
        Self { execution: Arc::default(), owned, domains }
    }
}

impl PreparedExecution for DedicatedExecution {
    fn execute(
        &self,
        task: ExecutionFuture,
        context: RunContext,
    ) -> ExecutionFuture {
        let execution = self.execution.clone();
        let owned = self.owned.clone();
        let domains = self.domains.clone();
        Box::pin(async move {
            {
                let domains = domains.lock().unwrap();
                if domains.closed {
                    return Err(Error::msg(
                        "current-thread driver is finalized",
                    ));
                }
                owned.lock().unwrap().push(execution.clone());
            }
            // The driver's owner list retains interrupted finalization too.
            let outcome = execution.execute(task, context).await;
            let finalized = execution.finalize().await;
            owned
                .lock()
                .unwrap()
                .retain(|entry| !Arc::ptr_eq(entry, &execution));
            outcome.and(finalized)
        })
    }

    fn execute_child(
        &self,
        task: ExecutionFuture,
        context: RunContext,
    ) -> ExecutionFuture {
        self.execution.execute_child(task, context)
    }
}
