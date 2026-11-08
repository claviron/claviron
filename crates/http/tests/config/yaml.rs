use super::*;

#[test]
fn http_yaml_rejects_unknown_directives() {
    let rejected = [
        ("watchdog: {}", "watchdog"),
        ("defaults: {headers: {typo: true}}", "typo"),
        #[cfg(not(feature = "metrics"))]
        ("metrics: {}", "metrics"),
        #[cfg(feature = "metrics")]
        ("metrics: {enabled: true}", "enabled"),
        #[cfg(feature = "metrics")]
        ("metrics: {watchdog: {typo: true}}", "typo"),
    ];
    for (yaml, key) in rejected {
        let error = serde_yaml::from_str::<HttpSnapshot>(yaml)
            .err()
            .expect("unknown directive must fail")
            .to_string();
        assert!(error.contains(&format!("unknown field `{key}`")), "{error}");
    }

    #[cfg(feature = "metrics")]
    assert!(
        serde_yaml::from_str::<HttpSnapshot>("metrics: {}")
            .unwrap()
            .metrics
            .is_some()
    );
}

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
            ("    execution: dedicated\n", "", ListenerExecution::Dedicated),
            (
                "    execution: dedicated\n",
                "    execution: shared\n",
                ListenerExecution::Shared,
            ),
            (
                "    execution: shared\n",
                "    execution: dedicated\n",
                ListenerExecution::Dedicated,
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
                ListenerExecution::Dedicated => ExecutionSpec::default()
                    .with_executor(ExecutorSpec::Shared(
                        ExecutionDomain::named::<ListenerExecution>("selected"),
                    )),
            };
            assert_eq!(listener.execution_spec(), spec);
            let mut changed = listener.as_ref().clone();
            changed.execution = if expected == ListenerExecution::Shared {
                ListenerExecution::Dedicated
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
fn uds_settings_read_octal_modes_and_reject_what_cannot_be_bound() {
    let listener = |uds: &str| {
        format!(
            "listeners:\n  example:\n    transport: tcp\n{uds}    protocols: h1\n    listen: '127.0.0.1:0'\n    requests: {{max_body_size: off, body_read_timeout: off}}\n"
        )
    };
    let uds = |source: &str| {
        let snapshot: HttpSnapshot = serde_yaml::from_str(source).unwrap();
        let ListenerSettings::Tcp(tcp) =
            &snapshot.listeners.values().next().unwrap().settings
        else {
            panic!("expected TCP settings")
        };
        tcp.uds.clone()
    };
    let unwritten = uds(&listener(""));
    assert_eq!(unwritten.backlog, None);
    assert_eq!(unwritten.mode.bits(), 0o600);
    assert_eq!(unwritten.group, None);
    // YAML reads 0660 and 660 as decimal 660; both mean the octal mode.
    for written in ["0660", "660", "'0o660'"] {
        let read = uds(&listener(&format!("    uds: {{mode: {written}}}\n")));
        assert_eq!(read.mode.bits(), 0o660, "mode={written}");
    }
    let named = uds(&listener(
        "    uds: {backlog: 4096, group: example, mode: 0600}\n",
    ));
    assert_eq!(named.backlog.map(|value| value.get()), Some(4096));
    assert_eq!(named.group, Some(SocketGroup::Name("example".into())));
    assert_eq!(
        uds(&listener("    uds: {group: 33}\n")).group,
        Some(SocketGroup::Id(33))
    );
    for invalid in [
        "{mode: 0680}",
        "{mode: 17777}",
        "{backlog: 0}",
        "{backlog: 2147483648}",
        "{group: ''}",
    ] {
        let source = listener(&format!("    uds: {invalid}\n"));
        assert!(
            serde_yaml::from_str::<HttpSnapshot>(&source).is_err(),
            "uds={invalid}"
        );
    }
    let original: HttpSnapshot =
        serde_yaml::from_str(&listener("    uds: {mode: 0600}\n")).unwrap();
    let changed: HttpSnapshot =
        serde_yaml::from_str(&listener("    uds: {mode: 0660}\n")).unwrap();
    assert!(
        !original
            .listeners
            .values()
            .next()
            .unwrap()
            .same_binding(changed.listeners.values().next().unwrap())
    );
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
    let configured = ListenerExecution::Dedicated;
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

#[test]
fn unix_listener_resolves_a_named_local_stream_without_an_ip_bind() {
    let snapshot: HttpSnapshot = serde_yaml::from_str(
        "defaults: {listener: {requests: {max_body_size: off, body_read_timeout: off}}}\nlisteners:\n  local:\n    transport: unix\n    listen: unix:/run/example.sock\n    protocols: [h1, h2]\n"
    ).unwrap();
    let listener = snapshot.listeners.values().next().unwrap();
    assert_eq!(listener.transport(), crate::listener::ListenerTransport::Unix);
    assert!(listener.listen[0].socket().is_none());
    for declaration in [
        "transport: unix, listen: '127.0.0.1:8080', protocols: h1",
        "transport: unix, listen: 'unix:/run/example.sock', protocols: h1, tls: true",
        "transport: quic, listen: 'unix:/run/example.sock', protocols: h3",
    ] {
        assert!(
            serde_yaml::from_str::<HttpSnapshot>(&format!(
                "listeners: {{local: {{{declaration}}}}}"
            ))
            .is_err()
        );
    }
}
