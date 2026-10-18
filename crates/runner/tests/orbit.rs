use super::*;

#[test]
fn edge_shutdown_application_event_preserves_notice_identity() {
    let notice = EdgeShutdownInitiated {
        edge: "example-edge".to_owned(),
        fleet: FleetName::new("example"),
        application: "example-app".to_owned(),
    };
    let event = notice.application_event().unwrap();
    assert_eq!(event.topic().as_str(), "claviron.edge.shutdown.initiated.v1");
    assert!(event.target().is_none());
    assert_eq!(
        event.payload(),
        &EventValue::Array(vec![
            (
                EventArrayKey::String("edge".into()),
                EventValue::String(b"example-edge".to_vec())
            ),
            (
                EventArrayKey::String("fleet".into()),
                EventValue::String(b"example".to_vec())
            ),
            (
                EventArrayKey::String("application".into()),
                EventValue::String(b"example-app".to_vec())
            ),
        ])
    );
    assert_eq!(
        ApplicationEvent::decode_orbit(&event.encode_orbit()).unwrap(),
        event
    );
}
