use super::*;

#[test]
fn listener_execution_inherits_and_overrides_for_plain_tls_and_quic() {
    use claviron_runtime::{ExecutionDomain, ExecutionSpec, ExecutorSpec};
    for driver in [
        "    transport: tcp\n    protocols: h1\n",
        "    transport: tcp\n    tls: true\n    protocols: [h1, h2]\n",
        "    transport: quic\n    protocols: h3\n",
    ] {
        for (inherited, local, expected) in [
            ("", "", ListenerExecution::Shared),
            (
                "    execution: current_thread\n",
                "",
                ListenerExecution::CurrentThread,
            ),
            (
                "    execution: current_thread\n",
                "    execution: shared\n",
                ListenerExecution::Shared,
            ),
            (
                "    execution: shared\n",
                "    execution: current_thread\n",
                ListenerExecution::CurrentThread,
            ),
        ] {
            let source = format!(
                "defaults:\n  listener:\n{inherited}    requests: {{max_body_size: off, body_read_timeout: off}}\nlisteners:\n  selected:\n{driver}{local}    listen: 127.0.0.1:0\n"
            );
            let config: HttpSnapshot = serde_yaml::from_str(&source).unwrap();
            let listener = config.listeners.values().next().unwrap();
            assert_eq!(listener.execution, expected);
            let spec = match expected {
                ListenerExecution::Shared => ExecutionSpec::default(),
                ListenerExecution::CurrentThread => ExecutionSpec::default()
                    .with_executor(ExecutorSpec::Shared(
                        ExecutionDomain::named::<ListenerExecution>("selected"),
                    )),
            };
            assert_eq!(listener.execution_spec(), spec);
            let mut changed = listener.as_ref().clone();
            changed.execution = if expected == ListenerExecution::Shared {
                ListenerExecution::CurrentThread
            } else {
                ListenerExecution::Shared
            };
            assert!(listener.same_binding(&changed));
        }
    }
}

#[test]
fn listener_execution_rejects_unknown_and_mistyped_modes() {
    let source = |inherited: &str, local: &str| {
        format!(
            "defaults:\n  listener:\n    execution: {inherited}\n    requests: {{max_body_size: off, body_read_timeout: off}}\nlisteners:\n  selected:\n    execution: {local}\n    transport: tcp\n    listen: 127.0.0.1:0\n    protocols: h1\n"
        )
    };
    assert!(
        serde_yaml::from_str::<HttpSnapshot>(&source("shared", "shared"))
            .is_ok()
    );
    for value in ["null", "false", "4", "multi_thread", "{}", "[]"] {
        for (inherited, local) in [(value, "shared"), ("shared", value)] {
            assert!(
                serde_yaml::from_str::<HttpSnapshot>(&source(inherited, local))
                    .is_err()
            );
        }
    }
}

#[test]
fn tcp_backlog_is_inherited_and_overridden_per_listener() {
    let inherited =
        "defaults:\n  listener:\n    tcp: {backlog: 17, nodelay: false}\n";
    for (defaults, local, expected) in [
        ("", "", TcpConfig::default().backlog.get()),
        (inherited, "", 17),
        (inherited, "    tcp: {backlog: 31}\n", 31),
    ] {
        let source = format!(
            "{defaults}listeners:\n  example:\n{local}    transport: tcp\n    protocols: h1\n    listen: '127.0.0.1:0'\n    requests: {{max_body_size: off, body_read_timeout: off}}\n"
        );
        let snapshot: HttpSnapshot = serde_yaml::from_str(&source).unwrap();
        let listener = snapshot.listeners.values().next().unwrap();
        let ListenerSettings::Tcp(tcp) = &listener.settings else {
            panic!("expected TCP settings")
        };
        assert_eq!(tcp.tcp.backlog.get(), expected);
        assert_eq!(tcp.tcp.nodelay, defaults.is_empty());
        assert_eq!(listener.limits.max_connections, None);
    }
}

#[test]
fn tcp_backlog_rejects_zero_negative_null_and_socket_overflow() {
    for value in ["0", "-1", "null", "2147483648", "4294967296"] {
        for inherited in [false, true] {
            let defaults = if inherited {
                format!(
                    "defaults:\n  listener:\n    tcp: {{backlog: {value}}}\n"
                )
            } else {
                String::new()
            };
            let local = if inherited {
                String::new()
            } else {
                format!("    tcp: {{backlog: {value}}}\n")
            };
            let source = format!(
                "{defaults}listeners:\n  example:\n{local}    transport: tcp\n    protocols: h1\n    listen: '127.0.0.1:0'\n    requests: {{max_body_size: off, body_read_timeout: off}}\n"
            );
            assert!(
                serde_yaml::from_str::<HttpSnapshot>(&source).is_err(),
                "backlog={value}, inherited={inherited}"
            );
        }
    }
}

#[test]
fn tcp_backlog_preserves_full_positive_socket_range() {
    for value in [1, i32::MAX] {
        let source = format!(
            "listeners:\n  example:\n    transport: tcp\n    tcp: {{backlog: {value}}}\n    protocols: h1\n    listen: '127.0.0.1:0'\n    requests: {{max_body_size: off, body_read_timeout: off}}\n"
        );
        let snapshot: HttpSnapshot = serde_yaml::from_str(&source).unwrap();
        let ListenerSettings::Tcp(tcp) =
            &snapshot.listeners.values().next().unwrap().settings
        else {
            panic!("expected TCP settings")
        };
        assert_eq!(tcp.tcp.backlog.get(), u32::try_from(value).unwrap());
    }
}

#[test]
fn tcp_backlog_changes_require_rebinding_for_plain_and_tls() {
    for tls in [false, true] {
        let source = format!(
            "listeners:\n  example:\n    transport: tcp\n    tls: {tls}\n    tcp: {{backlog: 17}}\n    protocols: h1\n    listen: '127.0.0.1:0'\n    requests: {{max_body_size: off, body_read_timeout: off}}\n"
        );
        let original: HttpSnapshot = serde_yaml::from_str(&source).unwrap();
        let changed: HttpSnapshot =
            serde_yaml::from_str(&source.replace("backlog: 17", "backlog: 31"))
                .unwrap();
        let policy: HttpSnapshot = serde_yaml::from_str(
            &source.replace("backlog: 17", "backlog: 17, nodelay: false"),
        )
        .unwrap();
        let original = original.listeners.values().next().unwrap();
        assert!(
            !original.same_binding(changed.listeners.values().next().unwrap())
        );
        assert!(
            original.same_binding(policy.listeners.values().next().unwrap())
        );
    }
}

#[test]
fn connection_limits_are_only_applied_when_declared() {
    let inherited = "defaults:\n  listener:\n    max_connections: 11\n";
    for (defaults, local, expected) in [
        ("", "", None),
        ("", "    max_connections: 7\n", Some(7)),
        (inherited, "", Some(11)),
        (inherited, "    max_connections: null\n", None),
        (inherited, "    max_connections: 7\n", Some(7)),
    ] {
        let source = format!(
            "{defaults}listeners:
  test:
{local}    transport: tcp
    protocols: h1
    listen: '127.0.0.1:0'
    requests: {{max_body_size: off, body_read_timeout: off}}
"
        );
        let snapshot = serde_yaml::from_str::<HttpSnapshot>(&source).unwrap();
        let listener = snapshot.listeners.values().next().unwrap();
        assert_eq!(
            listener.limits.max_connections.map(|value| value.get()),
            expected,
            "defaults={defaults:?}, local={local:?}"
        );
    }
}

#[test]
fn listener_execution_domains_are_scoped_to_listener_identity() {
    let first = "first".parse().unwrap();
    let second = "second".parse().unwrap();
    let configured = ListenerExecution::CurrentThread;
    assert_eq!(
        configured.for_listener(&first),
        configured.for_listener(&first)
    );
    assert_ne!(
        configured.for_listener(&first),
        configured.for_listener(&second)
    );
    assert_eq!(
        ListenerExecution::Shared.for_listener(&first),
        claviron_runtime::ExecutionSpec::default()
    );
}

#[test]
fn deny_defaults_participate_in_snapshot_reload_comparison() {
    let empty: HttpSnapshot = serde_yaml::from_str("{}").unwrap();
    let explicit_empty: HttpSnapshot =
        serde_yaml::from_str("defaults: {deny: []}").unwrap();
    assert!(empty == explicit_empty);
    assert!(empty.deny.is_empty());

    let declared: HttpSnapshot =
        serde_yaml::from_str("defaults: {deny: ['^/private']}").unwrap();
    let same: HttpSnapshot =
        serde_yaml::from_str("defaults: {deny: ['^/private']}").unwrap();
    let changed: HttpSnapshot =
        serde_yaml::from_str("defaults: {deny: ['^/secret']}").unwrap();
    assert!(declared == same);
    assert!(declared != empty);
    assert!(declared != changed);
}
