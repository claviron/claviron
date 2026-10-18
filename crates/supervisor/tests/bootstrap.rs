use super::*;

fn master() -> MasterIdentity {
    MasterIdentity::new(ProcessIdentity::new(123, 456), 7)
}

#[test]
fn v6_round_trips_fleet_master_identity_incarnation_and_environment() {
    let expected = WorkerBootstrap::new(
        WorkerId::new(2),
        FleetNodeId::new(2),
        WorkerCount::new(4),
        FleetCapacity::new(8).unwrap(),
        5,
        master(),
        "shop-blue",
        "clv",
        "/srv/shop/config",
        LaunchBlob::default(),
    )
    .expect("valid bootstrap")
    .with_environment("preview")
    .unwrap();

    let encoded = expected.encode().unwrap();
    assert!(encoded.contains(r#""version":6"#), "{encoded}");
    assert!(encoded.contains(r#""fleet_name":"clv""#), "{encoded}");
    assert!(
        encoded.contains(
            r#""master":{"pid":123,"started_at":456,"generation":7}"#
        ),
        "{encoded}"
    );
    let actual = WorkerBootstrap::decode(OsString::from(encoded)).unwrap();

    assert_eq!(actual, expected);
    assert_eq!(actual.run_mode(), RunMode::Worker(WorkerId::new(2)));
    assert_eq!(actual.incarnation(), 5);
    assert_eq!(actual.master(), master());
    assert_eq!(actual.fleet_name(), "clv");
    assert_eq!(actual.environment(), "preview");
}

#[test]
fn rejects_worker_outside_fleet() {
    let err = WorkerBootstrap::new(
        WorkerId::new(5),
        FleetNodeId::new(5),
        WorkerCount::new(4),
        FleetCapacity::new(8).unwrap(),
        1,
        master(),
        "shop-blue",
        "clv",
        "/srv/shop/config",
        LaunchBlob::default(),
    )
    .unwrap_err();

    assert!(err.to_string().contains("exceeds worker count"));
}

#[test]
fn rejects_unknown_schema_version() {
    let wire = r#"{"version":5,"worker_id":1,"fleet_node_id":1,"worker_count":1,"fleet_capacity":2,"incarnation":1,"master":{"pid":123,"started_at":1,"generation":1},"instance_name":"shop","fleet_name":"clv","environment":"production","config_dir":"config"}"#;
    let err = WorkerBootstrap::decode(OsString::from(wire)).unwrap_err();

    assert!(err.to_string().contains("unsupported worker bootstrap version"));
}

#[test]
fn environment_selection_cannot_escape_the_application_directory() {
    let mut wire = WorkerBootstrapWire::from(
        &WorkerBootstrap::new(
            WorkerId::new(1),
            FleetNodeId::new(1),
            WorkerCount::new(1),
            FleetCapacity::new(2).unwrap(),
            1,
            master(),
            "example",
            "clv",
            "config",
            LaunchBlob::default(),
        )
        .unwrap(),
    );
    wire.environment = "../preview".into();
    let encoded = serde_json::to_string(&wire).unwrap();
    assert!(WorkerBootstrap::decode(OsString::from(encoded)).is_err());
}

/// The child is this test binary re-executed with the envelope in its
/// environment, so its parent really is the process named as master. Only
/// the start stamp separates the accepted envelope from the refused one.
#[test]
fn worker_rejects_a_master_identity_that_is_not_its_parent() {
    const MODE: &str = "CLAVIRON_TEST_BOOTSTRAP_MODE";
    if let Ok(mode) = std::env::var(MODE) {
        let result = ProcessEntry::detect();
        match mode.as_str() {
            "accept" => assert!(
                matches!(result, Ok(ProcessEntry::Worker(_))),
                "{result:?}"
            ),
            "reject" => {
                let error = result.unwrap_err().to_string();
                assert!(error.contains("start stamp"), "{error}");
            }
            other => panic!("unknown mode {other}"),
        }
        return;
    }

    let me = process::identity(std::process::id()).expect("own identity");
    let impostor = ProcessIdentity::new(me.pid(), me.started_at() ^ 1);
    for (mode, master) in [("accept", me), ("reject", impostor)] {
        let bootstrap = WorkerBootstrap::new(
            WorkerId::new(1),
            FleetNodeId::new(1),
            WorkerCount::new(1),
            FleetCapacity::new(2).unwrap(),
            1,
            MasterIdentity::new(master, 3),
            "bootstrap-test",
            "clv",
            "/tmp",
            LaunchBlob::default(),
        )
        .unwrap();
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "bootstrap::tests::worker_rejects_a_master_identity_that_is_not_its_parent",
                "--nocapture"
            ])
            .env(MODE, mode)
            .env(WORKER_BOOTSTRAP_ENV, bootstrap.encode().unwrap())
            .output()
            .unwrap();
        assert!(output.status.success(), "{mode}: {output:?}");
    }
}

/// The blob is carried and never interpreted: whatever bytes go in come back,
/// including ones no text format would survive on its own.
#[test]
fn a_launch_survives_the_envelope_unchanged() {
    let launch = LaunchBlob::new(vec![0u8, 255, 10, b'"', b'\\', 128, 0]);
    let sent = WorkerBootstrap::new(
        WorkerId::new(1),
        FleetNodeId::new(1),
        WorkerCount::new(1),
        FleetCapacity::new(8).unwrap(),
        1,
        master(),
        "shop-blue",
        "clv",
        "/srv/shop/config",
        launch.clone(),
    )
    .expect("valid bootstrap");

    let back = WorkerBootstrap::decode(OsString::from(sent.encode().unwrap()))
        .unwrap();
    assert_eq!(back.launch(), &launch);
}

/// A launch nobody set is not an empty one: the worker must be able to tell
/// "use your own defaults" from "use nothing".
#[test]
fn an_absent_launch_is_not_an_empty_launch() {
    let sent = WorkerBootstrap::new(
        WorkerId::new(1),
        FleetNodeId::new(1),
        WorkerCount::new(1),
        FleetCapacity::new(8).unwrap(),
        1,
        master(),
        "shop-blue",
        "clv",
        "/srv/shop/config",
        LaunchBlob::default(),
    )
    .expect("valid bootstrap");
    let encoded = sent.encode().unwrap();
    assert!(
        !encoded.contains("launch"),
        "an unset launch takes no room: {encoded}"
    );
}

/// `execve` bounds the environment and the command line together, so an
/// oversized launch has to be refused where it can be explained rather than
/// at exec, where it cannot.
#[test]
fn an_oversized_launch_is_refused_by_the_master() {
    let error = WorkerBootstrap::new(
        WorkerId::new(1),
        FleetNodeId::new(1),
        WorkerCount::new(1),
        FleetCapacity::new(8).unwrap(),
        1,
        master(),
        "shop-blue",
        "clv",
        "/srv/shop/config",
        LaunchBlob::new(vec![b'x'; LaunchBlob::MAX + 1]),
    )
    .expect_err("must not be carried");
    assert!(error.to_string().contains("carries at most"), "{error}");
}
