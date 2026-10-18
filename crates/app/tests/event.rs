use super::*;

#[test]
fn codec_preserves_dynamic_topic_and_native_value() {
    let event = ApplicationEvent::new(
        "  user.logged-in.v1  ",
        EventValue::Array(vec![
            (EventArrayKey::String("id".into()), EventValue::Int(7)),
            (
                EventArrayKey::String("name".into()),
                EventValue::String(b"Ada".to_vec()),
            ),
        ]),
    )
    .unwrap();
    assert!(!event.id().is_nil());
    assert_eq!(event.target(), None);
    assert_eq!(event.topic().as_str(), "user.logged-in.v1");
    assert_eq!(
        ApplicationEvent::decode_orbit(&event.encode_orbit()).unwrap(),
        event
    );
}

#[test]
fn codec_preserves_runtime_qualified_application_target() {
    let target = ApplicationId::new(
        crate::ApplicationRuntime::new("php").unwrap(),
        crate::ApplicationName::new("octane-app").unwrap(),
    );
    let event = ApplicationEvent::for_application(
        target.clone(),
        "App\\Events\\Login",
        EventValue::String(b"serialized".to_vec()),
    )
    .unwrap();

    assert_eq!(event.target(), Some(&target));
    assert_eq!(
        ApplicationEvent::decode_orbit(&event.encode_orbit()).unwrap(),
        event
    );
}

#[test]
fn rejects_invalid_topics_and_oversized_frames() {
    assert_eq!(
        ApplicationEvent::new("  ", EventValue::Null).unwrap_err(),
        ApplicationEventError::EmptyTopic
    );
    assert!(matches!(
        ApplicationEvent::new(
            "oversized",
            EventValue::String(vec![0; EVENT_PAYLOAD_MAX])
        ),
        Err(ApplicationEventError::FrameTooLarge { .. })
    ));

    let mut nested = EventValue::Null;
    for _ in 0..=65 {
        nested = EventValue::Array(vec![(EventArrayKey::Int(0), nested)]);
    }
    assert!(matches!(
        ApplicationEvent::new("nested", nested),
        Err(ApplicationEventError::InvalidPayload(_))
    ));
}
