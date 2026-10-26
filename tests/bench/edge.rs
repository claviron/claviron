use super::*;

#[test]
fn benchmark_hosts_require_explicitly_disabled_access_logging() {
    for setting in ["", "access_log: access"] {
        let config: Config = serde_yaml::from_str(&format!(
            "hosting:\n  bench.test:\n    root: public\n    {setting}\n"
        ))
        .unwrap();
        assert!(
            config
                .declarations()
                .err()
                .unwrap()
                .to_string()
                .contains("requires access_log: off")
        );
    }
    let config: Config = serde_yaml::from_str(
        "hosting:\n  bench.test:\n    access_log: off\n    root: public\n",
    )
    .unwrap();
    assert_eq!(config.declarations().unwrap().len(), 1);
}
