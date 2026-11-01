use super::*;

#[test]
fn plain_execution_requires_selected_mode_and_a_cleartext_listener() {
    for (mode, listeners, expected) in [
        ("shared", true, false),
        ("current_thread", true, true),
        ("current_thread", false, false),
    ] {
        let binding = if listeners {
            "listeners:\n  example:\n    transport: tcp\n    listen: '127.0.0.1:0'\n    protocols: h1\n    requests: {max_body_size: off, body_read_timeout: off}\n"
        } else {
            "listeners: {}\n"
        };
        let yaml = format!("plain_execution: {mode}\n{binding}");
        let config: HttpServerConfig = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(config.current_thread(), expected);
    }
}
