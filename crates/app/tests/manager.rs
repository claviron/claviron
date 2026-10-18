use std::sync::atomic::{AtomicU64, Ordering};

use super::*;
use crate::{
    ApplicationControlResult, ApplicationLifecycle, ApplicationName,
    ApplicationPoolStatus,
};

struct TestController {
    runtime: ApplicationRuntime,
    application: ApplicationName,
    generation: AtomicU64,
}

impl TestController {
    fn new(
        runtime: &str,
        application: &str,
    ) -> Self {
        Self {
            runtime: ApplicationRuntime::new(runtime).unwrap(),
            application: ApplicationName::new(application).unwrap(),
            generation: AtomicU64::new(1),
        }
    }

    fn snapshot(&self) -> ApplicationSnapshot {
        ApplicationSnapshot {
            description: crate::ApplicationDescription::default(),
            id: ApplicationId::new(
                self.runtime.clone(),
                self.application.clone(),
            ),
            lifecycle: ApplicationLifecycle::Running,
            enabled: true,
            generation: self.generation.load(Ordering::Acquire),
            pool: Some(ApplicationPoolStatus {
                accepting: Some(true),
                instances: 1,
                ready: Some(1),
                busy: Some(0),
                retiring: None,
                backing_off: None,
                exhausted: None,
            }),
            last_error: None,
        }
    }
}

#[async_trait::async_trait]
impl ApplicationController for TestController {
    fn runtime(&self) -> &ApplicationRuntime {
        &self.runtime
    }

    fn list(&self) -> Vec<ApplicationSnapshot> {
        vec![self.snapshot()]
    }

    fn status(
        &self,
        name: &ApplicationName,
    ) -> Option<ApplicationSnapshot> {
        (name == &self.application).then(|| self.snapshot())
    }

    async fn start(
        &self,
        _name: &ApplicationName,
    ) -> ApplicationControlResult<()> {
        Ok(())
    }

    async fn stop(
        &self,
        _name: &ApplicationName,
    ) -> ApplicationControlResult<()> {
        Ok(())
    }

    async fn restart(
        &self,
        _name: &ApplicationName,
    ) -> ApplicationControlResult<()> {
        self.generation.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}

#[tokio::test]
async fn manager_routes_by_runtime_without_copying_controller_state() {
    let manager = ApplicationManager::new();
    let controller = Arc::new(TestController::new("php", "alpha"));
    let id = ApplicationId::new(
        controller.runtime.clone(),
        controller.application.clone(),
    );
    manager.add(controller).unwrap();

    assert_eq!(manager.status(&id).unwrap().generation, 1);
    manager.restart(&id).await.unwrap();
    assert_eq!(manager.status(&id).unwrap().generation, 2);
}

#[test]
fn manager_rejects_duplicate_runtime_controllers() {
    let manager = ApplicationManager::new();
    manager.add(Arc::new(TestController::new("php", "alpha"))).unwrap();
    assert!(matches!(
        manager.add(Arc::new(TestController::new("php", "beta"))),
        Err(ApplicationManagerError::DuplicateRuntime(_))
    ));
}

#[test]
fn manager_distinguishes_missing_runtime_and_application() {
    let manager = ApplicationManager::new();
    manager.add(Arc::new(TestController::new("php", "alpha"))).unwrap();

    let missing_application = ApplicationId::new(
        ApplicationRuntime::new("php").unwrap(),
        ApplicationName::new("missing").unwrap(),
    );
    assert!(matches!(
        manager.status(&missing_application),
        Err(ApplicationManagerError::ApplicationNotFound(_))
    ));

    let missing_runtime = ApplicationId::new(
        ApplicationRuntime::new("js").unwrap(),
        ApplicationName::new("alpha").unwrap(),
    );
    assert!(matches!(
        manager.status(&missing_runtime),
        Err(ApplicationManagerError::RuntimeNotFound(_))
    ));
}
