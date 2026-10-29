use super::*;

#[tokio::test]
async fn absent_supervisor_policy_remains_a_reloadable_source() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("supervisor.yaml");
    let policy = SupervisorPolicy {
        accepted: AtomicBool::new(false),
        source: PolicySource { path: path.clone(), registry: Weak::new() },
        current: Arc::new(PolicySnapshot::default()),
    };
    let reader = ConfigReader::new().with_builtin_parsers().unwrap();
    let defaults = SupervisorConfig::default();

    reader.prepare_owners(&[&policy]).await.unwrap();
    assert_eq!(*reader.snapshot::<SupervisorPolicy>().unwrap(), defaults);
    assert!(congif::ConfigRegistration::observes_config(&policy));
    assert_eq!(
        reader.sources_for(&policy).unwrap(),
        std::slice::from_ref(&path)
    );

    let mut configured = defaults.clone();
    configured.heartbeat.interval *= 2;
    std::fs::write(&path, serde_yaml::to_string(&configured).unwrap()).unwrap();
    reader.prepare_owners(&[&policy]).await.unwrap();
    assert_eq!(*reader.snapshot::<SupervisorPolicy>().unwrap(), configured);

    std::fs::write(&path, "heartbeat: [").unwrap();
    assert!(reader.prepare_owners(&[&policy]).await.is_err());
    assert_eq!(*reader.snapshot::<SupervisorPolicy>().unwrap(), configured);

    std::fs::remove_file(&path).unwrap();
    reader.prepare_owners(&[&policy]).await.unwrap();
    assert_eq!(*reader.snapshot::<SupervisorPolicy>().unwrap(), defaults);
    assert_eq!(reader.sources_for(&policy).unwrap(), [path]);
}
