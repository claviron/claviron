use std::path::PathBuf;

use super::validate;

#[test]
fn warmup_paths_reject_empty_and_nul_without_touching_the_filesystem() {
    assert!(validate(&[]).is_ok());
    assert!(validate(&[PathBuf::new()]).is_err());
    assert!(validate(&[PathBuf::from("source\0folder")]).is_err());
    assert!(validate(&[PathBuf::from("example/app")]).is_ok());
}
