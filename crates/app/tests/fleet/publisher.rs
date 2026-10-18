use claviron_runtime::{
    FleetCapacity, FleetNodeId, RunMode, Runtime, ServiceManagerError,
    WorkerCount,
};

use super::*;

#[tokio::test]
async fn master_keeps_boot_hook_without_a_runtime_service_row() {
    let state = AppState::initialize(
        RunMode::Master,
        FleetNodeId::ZERO,
        WorkerCount::new(1),
        FleetCapacity::new(2).unwrap(),
        Some("application-publisher-test".into()),
        ".",
    );
    let provider = Arc::new(ApplicationSnapshotPublisher::new(
        &state,
        ApplicationSnapshotPolicy::default(),
    ));
    state.registry_ref().register(provider.clone()).unwrap();
    // Its dependencies are registered, as the application's registration
    // guarantees; the fleet is never booted.
    state
        .registry_ref()
        .insert(Arc::new(crate::ApplicationManager::new()))
        .unwrap();
    state
        .registry_ref()
        .register(Arc::new(claviron_orbit::FleetProvider::new()))
        .unwrap();

    // A non-runnable provider still executes its policy boot hook. Physical
    // ring preparation belongs to ApplicationSnapshotShm, so this provider
    // neither requires a live Fleet nor opens SHM itself.
    claviron_runtime::AnyComponent::boot(
        provider.as_ref(),
        state.registry_ref(),
        &state,
    )
    .await
    .unwrap();
    let mut runtime = Runtime::default();
    let manager = runtime.manager();
    assert_eq!(
        runtime.spawn_all(state.registry_ref(), state.clone()).unwrap(),
        0
    );
    let checks = async {
        assert!(manager.list().await.unwrap().is_empty());
        assert!(matches!(
            manager.start("ApplicationSnapshotPublisher").await,
            Err(ServiceManagerError::NotFound(_))
        ));
        assert!(
            state
                .registry_ref()
                .resolve::<ApplicationSnapshotPublisher>()
                .is_some()
        );
        state.initiate_shutdown();
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        let (result, ()) =
            tokio::join!(runtime.wait_until_shutdown(&state), checks);
        result.unwrap();
        runtime.drain().await.unwrap();
    })
    .await
    .unwrap();
}
