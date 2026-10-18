use super::*;

/// The controller binds during boot regardless of registration order.
/// Only its preparation hook runs here; no PHP engine is started.
#[tokio::test]
async fn registration_contributes_the_application_controller() {
    for manager_first in [false, true] {
        let state = AppState::standalone("php-manager-registration", ".");
        if manager_first {
            claviron_app::register(&state).unwrap();
        }
        crate::PhpEmbedRuntime::builder().build().register(&state).unwrap();
        if !manager_first {
            claviron_app::register(&state).unwrap();
        }

        let manager =
            state.registry_ref().resolve::<ApplicationManager>().unwrap();
        let controller =
            state.registry_ref().resolve::<PhpApplicationController>().unwrap();
        controller.boot(Arc::clone(&manager), &state).await.unwrap();
        let id = ApplicationId::new(
            ApplicationRuntime::new("php").unwrap(),
            ApplicationName::new("missing").unwrap(),
        );
        assert!(matches!(
            manager.status(&id),
            Err(claviron_app::ApplicationManagerError::ApplicationNotFound(_))
        ));
    }
}
