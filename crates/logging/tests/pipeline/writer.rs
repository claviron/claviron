use super::{ChannelConfig, Sinks};

fn channel(
    path: &std::path::Path,
    option: &str,
) -> ChannelConfig {
    serde_yaml::from_str(&format!(
        "adapter: file\npath: {}\nformat: line\n{option}",
        serde_yaml::to_string(path).unwrap().trim(),
    ))
    .unwrap()
}

#[test]
fn file_channels_clear_only_when_startup_is_enabled() {
    for option in [
        "",
        "truncate: false\n",
        "truncate: true\n",
        "truncate_on_startup: true\n",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("channel.log");
        std::fs::write(&path, "previous run\n").unwrap();
        let channels = [Some(("example".to_owned(), channel(&path, option)))];
        let mut sinks = Sinks::open(&channels, "example", true).unwrap();
        sinks.write(&mut [b"new run\n".to_vec()]);
        let expected = if option.contains("true") {
            "new run\n"
        } else {
            "previous run\nnew run\n"
        };
        assert_eq!(std::fs::read_to_string(path).unwrap(), expected);
    }
}

#[test]
fn reload_reopen_and_service_restart_preserve_file_channels() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("channel.log");
    let config = channel(&path, "truncate: true\n");
    let channels = [Some(("example".to_owned(), config))];
    let mut sinks = Sinks::open(&channels, "example", true).unwrap();
    sinks.write(&mut [b"startup\n".to_vec()]);
    sinks.reopen();
    sinks.write(&mut [b"reopened\n".to_vec()]);

    // Changing the format forces a reopen through the reload path.
    let config: ChannelConfig = serde_yaml::from_str(&format!(
        "adapter: file\npath: {}\nformat: json\ntruncate: true\n",
        serde_yaml::to_string(&path).unwrap().trim(),
    ))
    .unwrap();
    let channels = [Some(("example".to_owned(), config))];
    sinks.reconfigure(&channels, "example");
    sinks.write(&mut [b"reloaded\n".to_vec()]);
    drop(sinks);

    let mut sinks = Sinks::open(&channels, "example", false).unwrap();
    sinks.write(&mut [b"restarted\n".to_vec()]);
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        "startup\nreopened\nreloaded\nrestarted\n",
    );

    // A newly declared file channel during reload must also preserve history.
    let added = dir.path().join("added.log");
    std::fs::write(&added, "earlier records\n").unwrap();
    let channels =
        [Some(("added".to_owned(), channel(&added, "truncate: true\n")))];
    sinks.reconfigure(&channels, "example");
    sinks.write(&mut [b"added during reload\n".to_vec()]);
    assert_eq!(
        std::fs::read_to_string(added).unwrap(),
        "earlier records\nadded during reload\n",
    );
}

#[test]
fn audit_logging_context_cannot_replace_record_metadata() {
    use super::{Decoded, OwnedValue, json_line};
    use crate::pipeline::record::{ChannelId, Level};
    let reserved = [
        "time",
        "level",
        "channel",
        "node",
        "target",
        "message",
        "truncated",
        "fields",
    ];
    let mut fields: Vec<_> = reserved
        .iter()
        .map(|key| (key.to_string(), OwnedValue::Str("context-value".into())))
        .collect();
    fields.extend([
        ("status".into(), OwnedValue::U64(200)),
        ("delta".into(), OwnedValue::I64(-3)),
        ("ratio".into(), OwnedValue::F64(0.5)),
        ("cached".into(), OwnedValue::Bool(true)),
    ]);
    let mut record = Decoded {
        channel: ChannelId(1),
        level: Level::Error,
        time: 0,
        target: "trusted-target".into(),
        message: "trusted-message".into(),
        fields,
        truncated: false,
    };
    for empty in [false, true] {
        if empty {
            record.target.clear();
            record.message.clear();
        }
        let mut output = Vec::new();
        json_line(&mut output, "audit", 1, &record);
        let value: serde_json::Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(value["level"], "error");
        assert_eq!(value["channel"], "audit");
        assert_eq!(value["node"], 1);
        assert_eq!(value["time"], super::rfc3339(0));
        if !empty {
            assert_eq!(value["target"], "trusted-target");
            assert_eq!(value["message"], "trusted-message");
        } else {
            assert!(value.get("target").is_none());
            assert!(value.get("message").is_none());
        }
        assert!(value.get("truncated").is_none());
        for key in reserved {
            assert_eq!(value["fields"][key], "context-value");
        }
        assert_eq!(value["status"], 200);
        assert_eq!(value["delta"], -3);
        assert_eq!(value["ratio"], 0.5);
        assert_eq!(value["cached"], true);
    }
}
