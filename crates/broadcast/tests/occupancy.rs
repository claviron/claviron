use super::super::event::{field, transition_payload};
use super::*;
use claviron_app::EventValue;

fn member(name: &str) -> PresenceMember {
    PresenceMember {
        user_id: "7".to_owned(),
        user_info: serde_json::json!({ "name": name }),
    }
}

#[test]
fn transitions_are_fleet_global_and_keyed_by_app_id() {
    let mut occupancy = Occupancy::default();
    let occupied = occupancy
        .joined(
            1,
            "app-a".into(),
            "room".into(),
            "1.1".into(),
            Some(member("Ada")),
        )
        .unwrap();
    assert_eq!(occupied.kind, TransitionKind::Occupied);
    assert!(
        occupancy
            .joined(
                2,
                "app-a".into(),
                "room".into(),
                "2.1".into(),
                Some(member("Lin"))
            )
            .is_none()
    );
    assert!(
        occupancy
            .joined(2, "app-b".into(), "room".into(), "2.2".into(), None)
            .is_some()
    );
    assert!(occupancy.left(1, "app-a", "room", "1.1").is_none());
    let vacated = occupancy.left(2, "app-a", "room", "2.1").unwrap();
    assert_eq!(vacated.kind, TransitionKind::Vacated);
    assert_eq!(
        vacated.member.unwrap().user_info,
        serde_json::json!({ "name": "Lin" })
    );
}

#[test]
fn node_reset_removes_ghost_subscriptions_without_vacating_live_channels() {
    let mut occupancy = Occupancy::default();
    occupancy.joined(1, "app".into(), "shared".into(), "1.1".into(), None);
    occupancy.joined(2, "app".into(), "shared".into(), "2.1".into(), None);
    occupancy.joined(1, "app".into(), "stale".into(), "1.2".into(), None);

    let transitions = occupancy.reset_node(1);
    assert_eq!(transitions.len(), 1);
    assert_eq!(transitions[0].identity.channel, "stale");
    assert!(occupancy.left(2, "app", "shared", "2.1").is_some());
}

#[test]
fn payload_carries_application_and_presence_identity() {
    let payload = transition_payload(&change(Transition {
        kind: TransitionKind::Occupied,
        identity: ChannelIdentity {
            app_id: "2010777".into(),
            channel: "presence-chat".into(),
        },
        member: Some(member("Ada")),
        time: "2026-09-21T07:25:34.710682+03:00".to_owned(),
    }));
    let EventValue::Array(fields) = payload else {
        panic!("occupancy payload must be an array");
    };
    assert_eq!(
        fields[0],
        field("app_id", EventValue::String(b"2010777".to_vec()))
    );
    assert_eq!(
        fields[1],
        field("channel", EventValue::String(b"presence-chat".to_vec()))
    );
    assert_eq!(
        fields[2],
        field(
            "time",
            EventValue::String(b"2026-09-21T07:25:34.710682+03:00".to_vec())
        )
    );
    assert_eq!(fields[3], field("user_id", EventValue::String(b"7".to_vec())));
    assert_eq!(
        fields[4],
        field(
            "user_info",
            EventValue::Array(vec![field(
                "name",
                EventValue::String(b"Ada".to_vec())
            )])
        )
    );
}
