use super::*;

#[test]
fn tcp_backlog_survives_split_worker_serialization_and_command_line_override() {
    let (shared, own) =
        split(serde_yaml::from_str("tcp_backlog: 17\n").unwrap()).unwrap();
    assert!(own.as_mapping().unwrap().is_empty());
    let written: RunnerOptions = serde_yaml::from_value(shared).unwrap();
    let written: RunnerOptions =
        serde_json::from_slice(&serde_json::to_vec(&written).unwrap()).unwrap();
    assert_eq!(written.tcp_backlog().get(), 17);
    assert!(written.validate().is_ok());
    assert_eq!(
        RunnerOptions::default().over(written.clone()).tcp_backlog().get(),
        17
    );
    let cli = RunnerOptions {
        tcp_backlog: NonZeroU32::new(31),
        ..Default::default()
    };
    assert_eq!(cli.over(written).tcp_backlog().get(), 31);
    assert_eq!(
        RunnerOptions::default().tcp_backlog(),
        claviron_http::config::TcpConfig::default().backlog
    );
}

#[test]
fn tcp_backlog_rejects_invalid_counts_and_accepts_socket_boundaries() {
    for value in ["0", "-1", "2147483648", "4294967296"] {
        let parsed = serde_yaml::from_str::<RunnerOptions>(&format!(
            "tcp_backlog: {value}\n"
        ));
        assert!(
            parsed.is_err() || parsed.unwrap().validate().is_err(),
            "backlog={value}"
        );
    }
    for value in [1, i32::MAX] {
        let parsed: RunnerOptions =
            serde_yaml::from_str(&format!("tcp_backlog: {value}\n")).unwrap();
        assert!(parsed.validate().is_ok());
        assert_eq!(parsed.tcp_backlog().get(), u32::try_from(value).unwrap());
    }
}

#[cfg(feature = "cli")]
#[test]
fn tcp_backlog_command_line_is_typed_and_overrides_the_file() {
    use clap::Parser;

    #[derive(Parser)]
    struct Args {
        #[command(flatten)]
        runner: RunnerOptions,
    }
    let args =
        Args::try_parse_from(["example", "--tcp-backlog", "31"]).unwrap();
    let written: RunnerOptions =
        serde_yaml::from_str("tcp_backlog: 17\n").unwrap();
    let resolved = args.runner.over(written);
    assert_eq!(resolved.tcp_backlog().get(), 31);
    assert!(resolved.validate().is_ok());
    assert!(Args::try_parse_from(["example", "--tcp-backlog", "0"]).is_err());
}

/// Every serialized field has its key in `KEYS`; input aliases do not add
/// serialized fields.
#[test]
fn keys_name_every_field() {
    let written = serde_yaml::to_value(RunnerOptions::default()).unwrap();
    let serde_yaml::Value::Mapping(fields) = written else {
        panic!("a mapping")
    };
    let mut fields: Vec<&str> =
        fields.keys().filter_map(|key| key.as_str()).collect();
    let mut keys = RunnerOptions::KEYS.to_vec();
    keys.retain(|key| *key != "framework");
    fields.sort_unstable();
    keys.sort_unstable();
    assert_eq!(fields, keys);
}

#[test]
fn framework_alias_is_split_and_serializes_with_the_canonical_name() {
    for input in ["framework: example", "frameworks: [example]"] {
        let (shared, own) =
            split(serde_yaml::from_str(input).unwrap()).unwrap();
        assert!(own.as_mapping().unwrap().is_empty());
        let options: RunnerOptions = serde_yaml::from_value(shared).unwrap();
        assert_eq!(
            options.frameworks.as_deref(),
            Some(["example".to_owned()].as_slice())
        );
        let written = serde_yaml::to_value(options).unwrap();
        let fields = written.as_mapping().unwrap();
        assert!(!fields.contains_key(serde_yaml::Value::from("framework")));
        assert_eq!(
            fields[serde_yaml::Value::from("frameworks")],
            serde_yaml::to_value(["example"]).unwrap()
        );
    }
}

/// A flat file divides into the shared keys and the runner's own, and a key
/// neither knows is refused on the runner's side rather than ignored.
#[test]
fn split_divides_a_flat_file() {
    let file: serde_yaml::Value = serde_yaml::from_str(
        "name: shop\norbit: true\nroot: app\nrequests:\n  head_timeout: 60s\n",
    )
    .unwrap();
    let (shared, own) = split(file).unwrap();
    let options: RunnerOptions = serde_yaml::from_value(shared).unwrap();
    assert_eq!(options.name.as_deref(), Some("shop"));
    assert!(options.orbit);
    assert_eq!(
        options.policy().unwrap().requests.head_timeout,
        Some(std::time::Duration::from_secs(60))
    );
    assert_eq!(serde_yaml::to_string(&own).unwrap().trim(), "root: app");
}

#[test]
fn command_line_fleet_overrides_only_the_fleet_name() {
    #[cfg(feature = "cli")]
    {
        use clap::Parser;

        #[derive(Parser)]
        struct Args {
            #[command(flatten)]
            runner: RunnerOptions,
        }

        for flag in ["--fleet", "--fleet-name"] {
            let parsed =
                Args::try_parse_from(["example", flag, "test"]).unwrap();
            assert_eq!(parsed.runner.fleet_name.as_deref(), Some("test"));
        }
    }
    let written: RunnerOptions =
        serde_yaml::from_str("name: example-app\nfleet_name: example\n")
            .unwrap();
    let command_line =
        RunnerOptions { fleet_name: Some("test".into()), ..Default::default() };
    let options = command_line.over(written);
    assert_eq!(options.name.as_deref(), Some("example-app"));
    assert_eq!(options.fleet_name.as_deref(), Some("test"));
    assert!(options.validate().is_ok());
    let invalid = RunnerOptions {
        fleet_name: Some("example-app".into()),
        ..Default::default()
    };
    assert!(invalid.validate().is_err());
}

#[cfg(feature = "files")]
#[test]
fn assets_and_probe_survive_the_launch_and_command_line_precedence() {
    let written: RunnerOptions =
        serde_yaml::from_str("assets: [build, images]\n").unwrap();
    let back: RunnerOptions =
        serde_json::from_slice(&serde_json::to_vec(&written).unwrap()).unwrap();
    assert_eq!(back.assets, written.assets);
    assert!(!back.try_files);
    assert!(back.file_site().unwrap().is_some());
    let cli = RunnerOptions {
        assets: vec!["static".into()],
        try_files: true,
        ..Default::default()
    };
    let options = cli.over(back);
    assert_eq!(options.assets, vec![PathBuf::from("static")]);
    assert!(options.try_files);
    assert!(options.validate().is_ok());
    let invalid = RunnerOptions {
        assets: vec!["../private".into()],
        ..Default::default()
    };
    assert!(invalid.validate().is_err());
}
