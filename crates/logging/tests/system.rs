use std::io::Write;

use claviron_runtime::{RunMode, WorkerId};

use super::{AppendWriter, SystemLog, append};

fn fixture(option: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("system.log");
    std::fs::write(&path, "previous run\n").unwrap();
    std::fs::write(
        dir.path().join("logging.yaml"),
        format!(
            "system_log:\n  path: {}\n  format: line\n{option}",
            serde_yaml::to_string(&path).unwrap().trim(),
        ),
    )
    .unwrap();
    dir
}

#[test]
fn startup_preserves_log_by_default() {
    let dir = fixture("");
    let log = SystemLog::from_config_dir_for_startup(
        dir.path(),
        |_| None,
        RunMode::Master,
    )
    .unwrap()
    .unwrap();
    AppendWriter(log.open).write_all(b"new run\n").unwrap();
    assert_eq!(
        std::fs::read_to_string(dir.path().join("system.log")).unwrap(),
        "previous run\nnew run\n",
    );
}

#[test]
fn enabled_reset_belongs_to_master_and_standalone_only() {
    for option in ["truncate_on_startup", "truncate"] {
        for role in [
            RunMode::Master,
            RunMode::Standalone,
            RunMode::Worker(WorkerId::new(1)),
        ] {
            let dir = fixture(&format!("  {option}: true\n"));
            let log = SystemLog::from_config_dir_for_startup(
                dir.path(),
                |_| None,
                role,
            )
            .unwrap()
            .unwrap();
            AppendWriter(log.open).write_all(b"new run\n").unwrap();
            let expected = if matches!(role, RunMode::Worker(_)) {
                "previous run\nnew run\n"
            } else {
                "new run\n"
            };
            assert_eq!(
                std::fs::read_to_string(dir.path().join("system.log")).unwrap(),
                expected
            );
        }
    }
}

#[test]
fn ordinary_opens_and_rotation_reopens_never_reset() {
    let dir = fixture("  truncate: true\n");
    let log =
        SystemLog::from_config_dir(dir.path(), |_| None).unwrap().unwrap();
    AppendWriter(log.open).write_all(b"ordinary open\n").unwrap();
    append(&dir.path().join("system.log"))
        .unwrap()
        .write_all(b"reopened\n")
        .unwrap();
    assert_eq!(
        std::fs::read_to_string(dir.path().join("system.log")).unwrap(),
        "previous run\nordinary open\nreopened\n",
    );
}

#[test]
fn configuration_check_does_not_touch_log() {
    let dir = fixture("  truncate: true\n");
    SystemLog::check_config_dir(dir.path(), |_| None).unwrap();
    let path = dir.path().join("system.log");
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "previous run\n");
    std::fs::remove_file(&path).unwrap();
    SystemLog::check_config_dir(dir.path(), |_| None).unwrap();
    assert!(!path.exists());
}

#[test]
fn direct_policy_resolves_the_boot_environment_without_extra_files() {
    let config: super::SystemLogConfig =
        serde_yaml::from_str("path: '${LOG_DIRECTORY}/system.log'\n").unwrap();
    let config = config
        .resolve(|key| (key == "LOG_DIRECTORY").then(|| "logs".into()))
        .unwrap();
    assert_eq!(config.path, std::path::PathBuf::from("logs/system.log"));
    config.validate().unwrap();
}
