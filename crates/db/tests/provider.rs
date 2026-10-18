use super::*;

#[test]
fn supplied_options_have_no_file_lifecycle() {
    let service = DbService::from_config(DbFileConfig::default());
    assert!(service.config_path.as_os_str().is_empty());
    assert!(service.as_configurable().is_none());
    assert!(service.as_reloadable().is_none());
    assert!(service.as_bootable().is_some());
    assert_eq!(service.boot_priority(), Some(priority::FIRST));
}

#[test]
fn file_configuration_keeps_preparation_and_reload() {
    let service = DbService::empty(PathBuf::from("example/db.yaml"));
    assert_eq!(service.source().path, PathBuf::from("example/db.yaml"));
    assert!(service.as_configurable().is_some());
    assert!(service.as_reloadable().is_some());
    assert!(service.as_bootable().is_some());
}
