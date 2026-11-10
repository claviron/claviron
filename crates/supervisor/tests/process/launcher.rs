use std::ffi::OsStr;
use std::sync::Arc;

use claviron_runtime::{
    AppState, FleetCapacity, FleetNodeId, LaunchBlob, WorkerCount, WorkerId,
    registry::Registry,
};
use claviron_sys::process::ProcessIdentity;

use super::*;
use crate::{MasterIdentity, WORKER_BOOTSTRAP_ENV};

fn bootstrap() -> WorkerBootstrap {
    WorkerBootstrap::new(
        WorkerId::new(2),
        FleetNodeId::new(2),
        WorkerCount::new(4),
        FleetCapacity::new(8).unwrap(),
        3,
        MasterIdentity::new(ProcessIdentity::new(123, 456), 7),
        "example",
        "test",
        "config",
        LaunchBlob::default(),
    )
    .unwrap()
    .with_environment("preview")
    .unwrap()
    .with_descriptors(vec![("listener".into(), 9)])
}

#[test]
fn absent_launcher_keeps_current_executable_without_forwarding_cli_args() {
    let command = prepare_command(None, &bootstrap()).unwrap();

    assert_eq!(command.get_program(), std::env::current_exe().unwrap());
    assert_eq!(command.get_args().count(), 0);
    assert!(command.get_envs().any(|(key, value)| {
        key == WORKER_BOOTSTRAP_ENV && value.is_some()
    }));
}

struct CustomLauncher;

impl WorkerLauncher for CustomLauncher {
    fn prepare(
        &self,
        worker: &WorkerBootstrap,
    ) -> io::Result<Command> {
        assert_eq!(worker.master().generation(), 7);
        assert_eq!(worker.fleet_node_id(), FleetNodeId::new(2));
        assert_eq!(worker.config_dir(), std::path::Path::new("config"));
        assert_eq!(worker.environment(), "preview");
        let mut command = Command::new("application-worker");
        command
            .arg("--slot")
            .arg(worker.worker_id().to_string())
            .arg("--incarnation")
            .arg(worker.incarnation().to_string())
            .current_dir("application")
            .env_clear()
            .env("APPLICATION_MODE", "worker")
            .env(WORKER_BOOTSTRAP_ENV, "not the authoritative envelope");
        Ok(command)
    }
}

#[test]
fn bound_launcher_selects_command_but_supervisor_applies_exact_bootstrap() {
    let registry = Registry::<AppState>::new();
    registry.insert(Arc::new(CustomLauncher)).unwrap();
    registry
        .bind_dyn::<dyn WorkerLauncher, CustomLauncher>(|launcher| launcher)
        .unwrap();
    let launcher = registry.resolve_dyn::<dyn WorkerLauncher>().unwrap();
    let bootstrap = bootstrap();
    let command = prepare_command(Some(launcher.as_ref()), &bootstrap).unwrap();

    assert_eq!(command.get_program(), "application-worker");
    assert_eq!(
        command.get_args().collect::<Vec<_>>(),
        ["--slot", "2", "--incarnation", "3"]
    );
    assert_eq!(command.get_current_dir().unwrap(), OsStr::new("application"));
    assert!(command.get_envs().any(|(key, value)| {
        key == "APPLICATION_MODE" && value == Some(OsStr::new("worker"))
    }));

    let mut expected = Command::new("unused");
    bootstrap.apply_to(&mut expected).unwrap();
    let envelope = |command: &Command| {
        command
            .get_envs()
            .find(|(key, _)| *key == WORKER_BOOTSTRAP_ENV)
            .unwrap()
            .1
            .unwrap()
            .to_owned()
    };
    assert_eq!(envelope(&command), envelope(&expected));
}

struct RefusedLauncher;

impl WorkerLauncher for RefusedLauncher {
    fn prepare(
        &self,
        _worker: &WorkerBootstrap,
    ) -> io::Result<Command> {
        Err(io::Error::new(io::ErrorKind::PermissionDenied, "launch refused"))
    }
}

#[test]
fn launcher_failure_names_the_slot_without_falling_back() {
    let error =
        prepare_command(Some(&RefusedLauncher), &bootstrap()).unwrap_err();

    assert!(
        matches!(error, Error::Worker { number, .. } if number == WorkerId::new(2))
    );
    assert!(error.to_string().contains("launch refused"));
}
