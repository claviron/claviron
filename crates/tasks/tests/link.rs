use super::*;

const WORKERS: &str =
    "{ policy: dynamic, min: 1, max: 4, step: 1, retire_idle: off }";

fn entry(yaml: &str) -> Result<TaskConfig, String> {
    let mut config = TaskConfig::from_yaml(yaml)?;
    config.materialize_link()?;
    Ok(config)
}

#[test]
fn a_link_task_settles_into_one_master_daemon_and_chooses_nothing_else() {
    let config = entry(&format!(
        "link: {{ fleet: example, kind: worker, workers: {WORKERS}, command: {{ program: ./app }} }}"
    ))
    .unwrap();
    assert!(config.command.is_some());
    assert_eq!(config.scope, Some(TaskScope::Master));
    assert_eq!(config.dispatch, Some(TaskDispatch::One));
    assert!(config.timing.is_daemon());

    let runner =
        "link: { fleet: example, kind: runner, command: { program: ./app } }";
    assert!(entry(runner).is_ok());
    let grown = format!(
        "link: {{ fleet: example, kind: runner, workers: {WORKERS}, command: {{ program: ./app }} }}"
    );
    assert!(entry(&grown).is_err(), "a runner grows its own workers");
    let bare =
        "link: { fleet: example, kind: worker, command: { program: ./app } }";
    assert!(entry(bare).is_err(), "a worker task declares its workers");
    let idle = "link: { fleet: example, kind: runner }";
    assert!(entry(idle).is_err(), "it declares the process it runs");
    let placed = format!("scope: workers\n{runner}");
    assert!(entry(&placed).is_err(), "the master runs it");

    let refused = |yaml: &str, names: &str| {
        let error = entry(yaml).err().unwrap();
        assert!(error.contains(names), "{error}");
    };
    refused(
        "orbit: { fleet: example, kind: runner, command: { program: ./app } }",
        "now `link:`",
    );
    refused(
        "link: { fleet: example, kind: task, workers: { policy: static, count: 2 }, command: { program: ./app } }",
        "now `kind: worker`",
    );
    refused(
        "link: { fleet: example, kind: worker, workers: { policy: dynamic, min: 1, max: available, step: 1, retire_idle: off }, command: { program: ./app } }",
        "declares workers.max as a number",
    );
}

#[test]
fn one_task_runs_each_fleet_and_never_its_own() {
    let a = entry(
        "link: { fleet: example, kind: runner, command: { program: ./a } }",
    )
    .unwrap();
    let b = entry(
        "link: { fleet: example, kind: runner, command: { program: ./b } }",
    )
    .unwrap();
    assert!(validate_fleets([("a", &a)].into_iter(), "host").is_ok());
    assert!(
        validate_fleets([("a", &a), ("b", &b)].into_iter(), "host").is_err()
    );
    assert!(validate_fleets([("a", &a)].into_iter(), "example").is_err());
}

#[test]
fn static_command_counts_are_owned_by_the_controller_not_dispatch() {
    let config = entry("link: {fleet: example, kind: worker, workers: {policy: static, count: 3, retire_idle: off}, command: {program: ./app}}").unwrap();
    let workers = config.link.unwrap().workers.unwrap();
    assert_eq!(workers, Workers::Static { count: 3 });
    assert_eq!(workers.growth(40), None);
    assert_eq!(config.scope, Some(TaskScope::Master));
    assert_eq!(config.dispatch, Some(TaskDispatch::One));
}
