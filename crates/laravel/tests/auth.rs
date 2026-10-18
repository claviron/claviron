use super::*;
use claviron_auth::events::LoginContext;

#[test]
fn auth_payloads_keep_laravel_semantics_without_credentials() {
    let cases = [
        AuthEvent::Login {
            guard: "web".into(),
            subject: "subject-one".into(),
            remember: true,
            context: None,
        },
        AuthEvent::Logout {
            guard: "web".into(),
            subject: "subject-one".into(),
            username: None,
        },
        AuthEvent::Registered { subject: "subject-one".into() },
    ];
    for (index, event) in cases.iter().enumerate() {
        let frame = application_event(event).unwrap();
        assert_eq!(frame.topic().as_str(), TOPIC);
        let EventValue::Array(fields) = frame.payload() else {
            panic!("array payload required")
        };
        let field = |name: &str| {
            fields.iter().find_map(|(key, value)| {
                matches!(key, EventArrayKey::String(key) if key == name)
                    .then_some(value)
            })
        };
        assert_eq!(field("version"), Some(&EventValue::Int(1)));
        assert_eq!(
            field("subject"),
            Some(&EventValue::String(b"subject-one".to_vec()))
        );
        assert_eq!(
            field("kind"),
            Some(&EventValue::String(
                ["login", "logout", "registered"][index].as_bytes().to_vec()
            ))
        );
        assert_eq!(
            field("remember"),
            if index == 0 { Some(&EventValue::Bool(true)) } else { None }
        );
        assert_eq!(field("guard").is_some(), index != 2);
        assert!(field("token").is_none() && field("password").is_none());
        assert!(field("username").is_none());
        assert!(frame.target().is_none());
    }
}

#[test]
fn logout_snapshot_includes_the_host_username_without_credentials() {
    let event = AuthEvent::Logout {
        guard: "web".into(),
        subject: "subject-one".into(),
        username: Some("example-user".into()),
    };
    let frame = application_event(&event).unwrap();
    assert!(frame.target().is_none());
    assert_eq!(
        frame.payload().to_json().unwrap(),
        serde_json::json!({
            "version": 1,
            "kind": "logout",
            "subject": "subject-one",
            "guard": "web",
            "username": "example-user"
        })
    );
}

#[test]
fn login_snapshot_is_shared_json_with_utc_time_and_typed_connection_facts() {
    let event = AuthEvent::Login {
        guard: "web".into(),
        subject: "subject-one".into(),
        remember: false,
        context: Some(LoginContext {
            username: "example-user".into(),
            name: "Example User".into(),
            occurred_at: time::macros::datetime!(2026-01-01 03:00 +03:00),
            ip: Some("192.0.2.1".parse().unwrap()),
            identity: Some(
                "01234567-89ab-4cde-8fab-0123456789ab".parse().unwrap(),
            ),
        }),
    };
    let frame = application_event(&event).unwrap();
    assert!(frame.target().is_none());
    assert_eq!(
        frame.payload().to_json().unwrap(),
        serde_json::json!({
            "version": 1,
            "kind": "login",
            "subject": "subject-one",
            "guard": "web",
            "remember": false,
            "username": "example-user",
            "name": "Example User",
            "occurred_at": "2026-01-01T00:00:00Z",
            "ip": "192.0.2.1",
            "identity": "01234567-89ab-4cde-8fab-0123456789ab"
        })
    );
}

#[test]
fn login_snapshot_does_not_invent_missing_connection_facts() {
    let event = AuthEvent::Login {
        guard: "web".into(),
        subject: "subject-one".into(),
        remember: false,
        context: Some(LoginContext {
            username: "example-user".into(),
            name: "Example User".into(),
            occurred_at: time::OffsetDateTime::UNIX_EPOCH,
            ip: None,
            identity: None,
        }),
    };
    let payload =
        application_event(&event).unwrap().payload().to_json().unwrap();
    assert_eq!(payload["ip"], serde_json::Value::Null);
    assert_eq!(payload["identity"], serde_json::Value::Null);
}
