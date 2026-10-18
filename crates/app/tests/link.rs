use super::*;
use claviron_runtime::FleetName;

#[test]
fn introduction_describes_only_the_observed_lane() {
    let peer = Peer {
        fleet: FleetName::new("example"),
        name: "example-app".into(),
        role: "php".into(),
        lane: 3,
        incarnation: 42,
        pid: Some(1234),
    };

    assert_eq!(
        introduction(&peer),
        vec![
            field("name", text("example-app")),
            field("fleet", text("example")),
            field("role", text("php")),
            field("pid", EventValue::Int(1234)),
            field("lane", EventValue::Int(3)),
        ],
    );
}

#[test]
fn unknown_owner_is_not_the_observing_process() {
    let peer = Peer {
        fleet: FleetName::new("example"),
        name: "example-edge".into(),
        role: "edge".into(),
        lane: 2,
        incarnation: 7,
        pid: None,
    };

    assert!(introduction(&peer).contains(&field("pid", EventValue::Null)));
}
