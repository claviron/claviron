use claviron_orbit::transport::{LinkEventRouting, Routing};
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use claviron_runtime::AppState;
use claviron_runtime::registry;

use crate::{
    ApplicationController, ApplicationId, ApplicationRuntime,
    ApplicationSnapshot,
};

#[derive(Debug, thiserror::Error)]
pub enum ApplicationManagerError {
    #[error("application runtime '{0}' is already registered")]
    DuplicateRuntime(ApplicationRuntime),
    #[error("application runtime '{0}' is not registered")]
    RuntimeNotFound(ApplicationRuntime),
    #[error("application '{0}' is not registered")]
    ApplicationNotFound(ApplicationId),
    #[error("application '{id}' {operation} failed: {message}")]
    Operation { id: ApplicationId, operation: &'static str, message: String },
}

/// Process-local manager for user-level application runtime controllers.
///
/// Controllers remain the source of truth. This manager stores no lifecycle
/// flags, generations, counters, errors, or application definitions of its own.
pub struct ApplicationManager {
    controllers:
        RwLock<BTreeMap<ApplicationRuntime, Arc<dyn ApplicationController>>>,
}

impl ApplicationManager {
    pub fn new() -> Self {
        Self { controllers: RwLock::new(BTreeMap::new()) }
    }

    pub fn add(
        &self,
        controller: Arc<dyn ApplicationController>,
    ) -> Result<(), ApplicationManagerError> {
        let runtime = controller.runtime().clone();
        let mut controllers = self.controllers.write().unwrap();
        if controllers.contains_key(&runtime) {
            return Err(ApplicationManagerError::DuplicateRuntime(runtime));
        }
        controllers.insert(runtime, controller);
        Ok(())
    }

    pub fn remove(
        &self,
        runtime: &ApplicationRuntime,
    ) -> bool {
        self.controllers.write().unwrap().remove(runtime).is_some()
    }

    pub fn list(&self) -> Vec<ApplicationSnapshot> {
        self.controllers
            .read()
            .unwrap()
            .values()
            .flat_map(|controller| controller.list())
            .collect()
    }

    pub fn status(
        &self,
        id: &ApplicationId,
    ) -> Result<ApplicationSnapshot, ApplicationManagerError> {
        self.controller(id.runtime())?.status(id.name()).ok_or_else(|| {
            ApplicationManagerError::ApplicationNotFound(id.clone())
        })
    }

    pub async fn start(
        &self,
        id: &ApplicationId,
    ) -> Result<(), ApplicationManagerError> {
        self.apply(id, "start", |controller, name| async move {
            controller.start(&name).await
        })
        .await
    }

    pub async fn stop(
        &self,
        id: &ApplicationId,
    ) -> Result<(), ApplicationManagerError> {
        self.apply(id, "stop", |controller, name| async move {
            controller.stop(&name).await
        })
        .await
    }

    pub async fn restart(
        &self,
        id: &ApplicationId,
    ) -> Result<(), ApplicationManagerError> {
        self.apply(id, "restart", |controller, name| async move {
            controller.restart(&name).await
        })
        .await
    }

    fn controller(
        &self,
        runtime: &ApplicationRuntime,
    ) -> Result<Arc<dyn ApplicationController>, ApplicationManagerError> {
        self.controllers.read().unwrap().get(runtime).cloned().ok_or_else(
            || ApplicationManagerError::RuntimeNotFound(runtime.clone()),
        )
    }

    async fn apply<F, Fut>(
        &self,
        id: &ApplicationId,
        operation: &'static str,
        apply: F,
    ) -> Result<(), ApplicationManagerError>
    where
        F: FnOnce(
            Arc<dyn ApplicationController>,
            crate::ApplicationName,
        ) -> Fut,
        Fut: std::future::Future<Output = crate::ApplicationControlResult<()>>,
    {
        let controller = self.controller(id.runtime())?;
        if controller.status(id.name()).is_none() {
            return Err(ApplicationManagerError::ApplicationNotFound(
                id.clone(),
            ));
        }
        apply(controller, id.name().clone()).await.map_err(|error| {
            ApplicationManagerError::Operation {
                id: id.clone(),
                operation,
                message: error.to_string(),
            }
        })
    }
}

impl Default for ApplicationManager {
    fn default() -> Self {
        Self::new()
    }
}

pub fn register(state: &AppState) -> registry::Result<()> {
    state.registry_ref().insert(Arc::new(ApplicationManager::new()))?;
    // Application events crossing a link are routed by the domain topic
    // inside the envelope, not by the envelope's one ring topic.
    if state.registry_ref().resolve::<LinkEventRouting>().is_none() {
        state.registry_ref().insert(Arc::new(LinkEventRouting(
            Routing::Key(crate::event::link_routing_key),
        )))?;
    }
    crate::link::register(state)?;
    crate::invoke::register(state)
}

#[cfg(test)]
#[path = "../tests/manager.rs"]
mod tests;
