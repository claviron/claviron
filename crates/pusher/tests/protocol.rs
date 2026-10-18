use super::*;

#[test]
fn accepts_object_and_encoded_subscription_data() {
    for data in [
        serde_json::json!({"channel": "news"}),
        serde_json::Value::String(r#"{"channel":"news"}"#.to_owned()),
    ] {
        let request = ClientMessage {
            event: "pusher:subscribe".to_owned(),
            data,
            channel: None,
        };
        assert_eq!(subscription(&request).unwrap().channel, "news");
    }
}

#[test]
fn connection_payload_is_pusher_encoded_json() {
    let message = ServerMessage::connection_established("12.3").unwrap();
    let message: serde_json::Value = serde_json::from_str(&message).unwrap();
    assert_eq!(message["event"], "pusher:connection_established");
    let data: serde_json::Value =
        serde_json::from_str(message["data"].as_str().unwrap()).unwrap();
    assert_eq!(data["socket_id"], "12.3");
}

#[test]
fn validates_single_and_multiple_channel_events() {
    let single: TriggerEvent = serde_json::from_value(serde_json::json!({
        "name": "wallet.updated",
        "channel": "wallet",
        "data": {"amount": 5}
    }))
    .unwrap();
    assert_eq!(
        single.validate().unwrap(),
        PublishedEvent {
            name: "wallet.updated".to_owned(),
            data: r#"{"amount":5}"#.to_owned(),
            channels: vec!["wallet".to_owned()],
            socket_id: None
        }
    );

    let ambiguous: TriggerEvent = serde_json::from_value(serde_json::json!({
        "name": "event",
        "channel": "one",
        "channels": ["two"],
        "data": "{}"
    }))
    .unwrap();
    assert_eq!(ambiguous.validate(), Err(TriggerEventError::AmbiguousChannels));
}

#[test]
fn parses_presence_channel_data_without_losing_user_info() {
    assert_eq!(
        PresenceMember::parse(r#"{"user_id":10,"user_info":{"name":"Ada"}}"#)
            .unwrap(),
        PresenceMember {
            user_id: "10".to_owned(),
            user_info: serde_json::json!({ "name": "Ada" })
        }
    );
}
