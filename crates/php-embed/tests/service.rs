use claviron_runtime::ServiceStartPolicy;
use claviron_runtime::registry::Runnable;

use super::*;
use crate::test_support::ScriptFixture;

#[test]
fn builder_is_configless_and_empty_means_no_threads() {
    let state =
        AppState::standalone("embed-builder", "/nonexistent/embed-config");
    let php = PhpEmbedRuntime::builder().build();
    assert!(php.pool_snapshot().is_none());
    php.register(&state).unwrap();
    assert!(state.registry_ref().resolve::<PhpEmbedRuntime>().is_some());
    assert!(state.registry_ref().resolve::<PhpScriptService>().is_some());
}

#[test]
fn configured_defaults_and_failures_do_not_change_the_explicit_builder() {
    let directory = tempfile::tempdir().unwrap();
    let missing = AppState::standalone("missing-php-config", directory.path());
    crate::register(&missing).unwrap();
    assert_eq!(
        missing.php_embed().unwrap().options.request_pool,
        Some(RequestPool::fixed(1))
    );
    for invalid in [
        "sapi: [",
        "sapi: {pool: {min_idle: 0}}",
        "daemons:\n  queue:\n    script_path: bin/console\n    env_file: /nonexistent/claviron-test.env\n",
    ] {
        std::fs::write(directory.path().join("php.yaml"), invalid).unwrap();
        let state =
            AppState::standalone("invalid-php-config", directory.path());
        assert!(crate::register(&state).is_err());
        assert!(state.php_embed().is_err());
        assert!(
            state
                .registry_ref()
                .resolve::<claviron_orbit::CounterProvider>()
                .is_none()
        );
        PhpEmbedRuntime::builder()
            .request_pool(RequestPool::fixed(2))
            .build()
            .register(&state)
            .unwrap();
        assert_eq!(
            state.php_embed().unwrap().options.request_pool,
            Some(RequestPool::fixed(2))
        );
    }
}

#[test]
fn execution_policies_follow_independent_capabilities() {
    let script = ScriptFixture::new();
    for mask in 0..4 {
        let (requests, applications) = (mask & 1 != 0, mask & 2 != 0);
        let state = AppState::standalone("embed-capabilities", ".");
        let mut builder = PhpEmbedRuntime::builder();
        if requests {
            builder = builder.request_pool(RequestPool::fixed(1));
        }
        if applications {
            builder =
                builder.application(PhpApplication::new("web", script.path()));
        }
        builder.build().register(&state).unwrap();
        let registry = state.registry_ref();
        assert!(state.php_embed().unwrap().as_runnable().is_none());
        let request = registry.resolve::<PhpScriptService>().unwrap();
        let application = registry.resolve::<PhpApplicationService>().unwrap();
        assert!(Arc::ptr_eq(
            &request,
            &registry.resolve::<PhpScriptService>().unwrap()
        ));
        for (enabled, policy) in [
            (requests, request.start_policy(&state)),
            (applications, application.start_policy(&state)),
        ] {
            assert_eq!(
                policy,
                if enabled {
                    ServiceStartPolicy::Automatic
                } else {
                    ServiceStartPolicy::Unavailable
                }
            );
        }
    }
}

#[test]
fn master_keeps_provider_but_never_registers_php_runnable() {
    use claviron_runtime::{FleetCapacity, FleetNodeId, RunMode, WorkerCount};
    let script = ScriptFixture::new();
    let state = AppState::initialize(
        RunMode::Master,
        FleetNodeId::ZERO,
        WorkerCount::new(2),
        FleetCapacity::new(3).unwrap(),
        Some("embed-master-test".into()),
        ".",
    );
    PhpEmbedRuntime::builder()
        .request_pool(RequestPool::fixed(1))
        .application(PhpApplication::new("web", script.path()))
        .build()
        .register(&state)
        .unwrap();
    let php = state.php_embed().unwrap();
    assert!(php.run_priority().is_none());
    assert!(php.as_runnable().is_none());
    assert!(state.registry_ref().resolve::<PhpScriptService>().is_none());
    assert!(state.registry_ref().resolve::<PhpApplicationService>().is_none());
    assert!(
        state
            .registry_ref()
            .resolve::<claviron_orbit::FleetEventRegistry>()
            .is_some()
    );
    assert!(
        state
            .registry_ref()
            .resolve::<bus::PhpInvalidationBusService>()
            .is_none()
    );
}

#[test]
fn invalid_options_do_not_mutate_registry() {
    let state = AppState::standalone("embed-invalid-builder", ".");
    assert!(
        PhpEmbedRuntime::builder()
            .request_pool(RequestPool::fixed(0))
            .build()
            .register(&state)
            .is_err()
    );
    assert!(state.registry_ref().resolve::<PhpEmbedRuntime>().is_none());
    assert!(
        state
            .registry_ref()
            .resolve::<claviron_orbit::CounterProvider>()
            .is_none()
    );
}

#[test]
fn duplicate_service_is_rejected() {
    let state = AppState::standalone("embed-duplicate-builder", ".");
    PhpEmbedRuntime::builder().build().register(&state).unwrap();
    assert!(PhpEmbedRuntime::builder().build().register(&state).is_err());
}
