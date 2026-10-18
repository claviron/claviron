use std::os::unix::process::ExitStatusExt;
use std::process::Command;

use claviron_runtime::{FleetCapacity, FleetNodeId, WorkerCount, WorkerId};

use super::*;

#[test]
fn restart_signal_registration_is_master_only_in_real_processes() {
    const CHILD_MODE: &str = "CLAVIRON_TEST_RESTART_SIGNAL_MODE";
    if let Ok(mode) = std::env::var(CHILD_MODE) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(
            async {
                let master = mode == "master";
                let state = AppState::initialize(
                    if master { RunMode::Master } else { RunMode::Standalone },
                    FleetNodeId::ZERO,
                    WorkerCount::new(if master { 2 } else { 0 }),
                    FleetCapacity::new(3).unwrap(),
                    Some("usr1-subprocess-test".into()),
                    "."
                );
                let handles =
                    spawn_main_signal_handlers(state.clone(), ServerSignals::default()).unwrap();
                // Signal only this test-owned subprocess, never the test
                // runner or a deployed Claviron master. Send before the
                // listener's first poll to cover synchronous registration.
                nix::sys::signal::kill(nix::unistd::getpid(), nix::sys::signal::Signal::SIGUSR2)
                    .unwrap();
                assert!(master, "standalone must retain the restart signal's default disposition");
                // No Fleet is booted. A rejected Manager request must not
                // terminate the listener or initiate process shutdown.
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                assert!(!handles.restart.as_ref().unwrap().is_finished());
                assert!(!state.is_shutting_down());
                state.initiate_shutdown();
                handles.abort_reload();
                handles.wait_shutdown().await;
            }
        );
        return;
    }

    for mode in ["master", "standalone"] {
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "signals::tests::restart_signal_registration_is_master_only_in_real_processes",
                "--nocapture"
            ])
            .env(CHILD_MODE, mode)
            .output()
            .unwrap();
        if mode == "master" {
            assert!(output.status.success(), "{output:?}");
        } else {
            assert_eq!(
                output.status.signal(),
                Some(nix::sys::signal::Signal::SIGUSR2 as i32)
            );
        }
    }
}

#[tokio::test]
async fn signal_spawning_does_not_register_events_or_require_orbit() {
    for (mode, node_id, worker_count, fleet_capacity) in [
        (
            RunMode::Standalone,
            FleetNodeId::ZERO,
            WorkerCount::ZERO,
            FleetCapacity::ONE,
        ),
        (
            RunMode::Master,
            FleetNodeId::ZERO,
            WorkerCount::new(1),
            FleetCapacity::new(2).unwrap(),
        ),
        (
            RunMode::Worker(WorkerId::new(1)),
            FleetNodeId::new(1),
            WorkerCount::new(1),
            FleetCapacity::new(2).unwrap(),
        ),
    ] {
        let state = AppState::initialize(
            mode,
            node_id,
            worker_count,
            fleet_capacity,
            Some("signal-test".into()),
            ".",
        );
        let handles = if state.is_worker() {
            spawn_worker_signal_handlers(state.clone()).unwrap()
        } else {
            spawn_main_signal_handlers(state.clone(), ServerSignals::default())
                .unwrap()
        };
        assert_eq!(state.events().registered_type_count(), 0);
        assert_eq!(handles.reload.is_some(), !state.is_worker());
        assert_eq!(handles.restart.is_some(), state.is_master());
        let restart_abort =
            handles.restart.as_ref().map(JoinHandle::abort_handle);
        state.initiate_shutdown();
        handles.abort_reload();
        handles.wait_shutdown().await;
        if let Some(restart) = restart_abort {
            assert!(
                restart.is_finished(),
                "restart listener must be joined at shutdown"
            );
        }
    }
}
