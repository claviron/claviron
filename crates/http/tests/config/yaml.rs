use super::*;

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
