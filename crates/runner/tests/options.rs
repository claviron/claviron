use super::*;

#[test]
fn named_listeners_survive_worker_serialization_and_keep_their_settings() {
    let (shared, own) = split(serde_yaml::from_str(
        "defaults:\n  listener:\n    execution: dedicated\n    uds: {backlog: 17}\nlisteners:\n  private:\n    transport: unix\n    listen: unix:/tmp/example.sock\n    protocols: [h1, h2]\n  shared:\n    transport: unix\n    listen: unix:/tmp/shared.sock\n    protocols: h1\n    execution: shared\n"
    ).unwrap()).unwrap();
    assert!(own.as_mapping().unwrap().is_empty());
    let written: RunnerOptions = serde_yaml::from_value(shared).unwrap();
    let written: RunnerOptions =
        serde_json::from_slice(&serde_json::to_vec(&written).unwrap()).unwrap();
    for options in [
        written.clone().over(RunnerOptions::default()),
        RunnerOptions::default().over(written),
    ] {
        let listeners = options.socket_listeners().unwrap();
        assert_eq!(listeners.len(), 2);
        assert_eq!(listeners[0].id.as_str(), "private");
        assert_eq!(
            options.listener_defaults().unwrap().execution,
            claviron_http::config::Override::Value(
                claviron_http::config::ListenerExecution::Dedicated
            )
        );
        assert_eq!(
            listeners[0].execution,
            claviron_http::config::ListenerExecution::Dedicated
        );
        assert_eq!(
            listeners[1].execution,
            claviron_http::config::ListenerExecution::Shared
        );
        assert_eq!(
            listeners[0].transport(),
            claviron_http::listener::ListenerTransport::Unix
        );
        let claviron_http::config::ListenerSettings::Tcp(settings) =
            &listeners[0].settings
        else {
            panic!("stream settings")
        };
        assert_eq!(settings.uds.backlog.unwrap().get(), 17);
    }
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
        "name: shop\norbit: {link: {}}\nroot: app\nrequests:\n  queue_timeout: 60s\n",
    )
    .unwrap();
    let (shared, own) = split(file).unwrap();
    let options: RunnerOptions = serde_yaml::from_value(shared).unwrap();
    assert_eq!(options.name.as_deref(), Some("shop"));
    assert!(options.link_enabled());
    assert_eq!(
        options.policy().unwrap().requests.queue_timeout,
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

/// File-only defaults and application policy survive explicit-option layering.
#[test]
fn what_only_a_file_says_survives_on_either_side_of_a_layering() {
    let (shared, _) = split(
        serde_yaml::from_str(
            "edge_shutdown_policy: shutdown\n\
             defaults: { listener: { execution: dedicated } }\n\
             requests: { max_body_size: 1 MiB }\n\
             responses: { max_body_size: 1 MiB }\n",
        )
        .unwrap(),
    )
    .unwrap();
    let file: RunnerOptions = serde_yaml::from_value(shared).unwrap();
    for options in [
        file.clone().over(RunnerOptions::default()),
        RunnerOptions::default().over(file),
    ] {
        assert_eq!(options.edge_shutdown_policy, EdgeShutdownPolicy::Shutdown);
        assert!(options.defaults.is_some());
        assert!(options.requests.is_some());
        assert!(options.responses.is_some());
    }
}

#[test]
fn runner_listener_defaults_are_checked_when_read() {
    use claviron_http::config::{ListenerExecution, Override};
    let written = |listener: &str| RunnerOptions {
        defaults: Some(
            serde_yaml::from_str(&format!("{{listener: {listener}}}")).unwrap(),
        ),
        ..RunnerOptions::default()
    };
    let listener = written("{execution: dedicated, max_connections: 8}")
        .listener_defaults()
        .unwrap();
    assert_eq!(
        listener.execution,
        Override::Value(ListenerExecution::Dedicated)
    );
    assert!(
        written("{requests: {max_body_size: off}}").socket_listeners().is_err()
    );
    assert!(
        written("{responses: {idle_timeout: 1s}}").listener_defaults().is_err()
    );
    assert!(written("{mistyped: true}").validate().is_err());
    let unknown_default = RunnerOptions {
        defaults: Some(serde_yaml::from_str("{headers: {}}").unwrap()),
        ..RunnerOptions::default()
    };
    assert!(unknown_default.validate().is_err());
    assert!(
        serde_yaml::from_str::<RunnerOptions>(
            "listener: {execution: dedicated}"
        )
        .is_err()
    );
}

/// A site declares its own files: with `files` beside it, one would silently
/// lose. Its schema is the edge host's, so a mistake in it stops the launch.
#[cfg(feature = "files")]
#[test]
fn a_site_is_read_as_a_host_and_excludes_the_file_shorthands() {
    let site = "{root: public, routes: {/old: {redirect: {to: /, code: 307}}}}";
    let written = RunnerOptions {
        site: Some(serde_yaml::from_str(site).unwrap()),
        ..RunnerOptions::default()
    };
    assert!(written.site().unwrap().is_some());
    let both = RunnerOptions { try_files: true, ..written };
    assert!(both.validate().is_err());
    let wrong = RunnerOptions {
        site: Some(serde_yaml::from_str("{routes: 7}").unwrap()),
        ..RunnerOptions::default()
    };
    assert!(wrong.validate().is_err());
}

#[cfg(feature = "cli")]
#[test]
fn orbit_flag_selects_the_link_object_without_a_boolean_schema() {
    use clap::Parser;
    #[derive(Parser)]
    struct Args {
        #[command(flatten)]
        runner: RunnerOptions,
    }
    let args = Args::try_parse_from(["example", "--orbit"]).unwrap();
    assert!(args.runner.link_enabled());
    assert!(serde_yaml::from_str::<RunnerOptions>("orbit: true").is_err());
}

#[cfg(all(feature = "orbit", feature = "supervisor"))]
#[test]
fn worker_start_count_comes_from_the_declared_owner() {
    for (declaration, expected) in [
        ("{policy: static, count: 4}", 4),
        ("{policy: dynamic, min: 2, max: 8, step: 1, retire_idle: off}", 2),
    ] {
        let mut options: RunnerOptions = serde_yaml::from_str(&format!(
            "orbit: {{link: {{workers: {declaration}}}}}"
        ))
        .unwrap();
        assert_eq!(options.initial_workers().unwrap(), expected);
        options.validate().unwrap();
        options.workers = Some(6);
        assert!(
            options.initial_workers().is_err(),
            "two conflicting count authorities"
        );
        let refused = options.validate().unwrap_err();
        assert!(refused.starts_with("declare workers once"), "{refused}");
    }
    assert_eq!(RunnerOptions::default().initial_workers().unwrap(), 0);
}
