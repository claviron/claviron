use claviron_pusher::PublishedEvent;

use super::*;

#[tokio::test]
async fn publishes_only_to_the_selected_application_and_honors_socket_exclusion()
 {
    let registry = ChannelRegistry::default();
    let (first, mut first_rx) = mpsc::channel(1);
    let (second, mut second_rx) = mpsc::channel(1);
    registry.subscribe(
        Arc::from("app"),
        Arc::from("wallet"),
        Arc::from("1.1"),
        ChannelPeer::test(first, CancellationToken::new()),
    );
    registry.subscribe(
        Arc::from("app"),
        Arc::from("wallet"),
        Arc::from("1.2"),
        ChannelPeer::test(second, CancellationToken::new()),
    );
    let event = PublishedEvent {
        name: "wallet.updated".to_owned(),
        data: "{}".to_owned(),
        channels: vec!["wallet".to_owned()],
        socket_id: Some("1.1".to_owned()),
    };
    assert_eq!(registry.publish("app", &event).unwrap(), 1);
    assert!(first_rx.try_recv().is_err());
    assert!(second_rx.recv().await.unwrap().contains("wallet.updated"));
    assert_eq!(registry.publish("other", &event).unwrap(), 0);
}

#[tokio::test]
async fn a_full_outbound_queue_cancels_and_removes_the_slow_peer() {
    let registry = ChannelRegistry::default();
    let (sender, _receiver) = mpsc::channel(1);
    let cancellation = CancellationToken::new();
    sender.try_send("occupied".to_owned()).unwrap();
    registry.subscribe(
        Arc::from("app"),
        Arc::from("wallet"),
        Arc::from("1.1"),
        ChannelPeer::test(sender, cancellation.clone()),
    );
    let event = PublishedEvent {
        name: "event".to_owned(),
        data: "{}".to_owned(),
        channels: vec!["wallet".to_owned()],
        socket_id: None,
    };
    assert_eq!(registry.publish("app", &event).unwrap(), 0);
    assert!(cancellation.is_cancelled());
    assert_eq!(registry.publish("app", &event).unwrap(), 0);
}

#[tokio::test]
async fn presence_counts_users_and_purges_a_restarted_fleet_node() {
    let registry = ChannelRegistry::default();
    let (sender, mut receiver) = mpsc::channel(8);
    registry.subscribe(
        Arc::from("app"),
        Arc::from("presence-users"),
        Arc::from("observer"),
        ChannelPeer::test(sender, CancellationToken::new()),
    );
    let member = PresenceMember {
        user_id: "7".to_owned(),
        user_info: serde_json::json!({ "name": "Ada" }),
    };

    let first = registry
        .join_presence(
            Arc::from("app"),
            Arc::from("presence-users"),
            Arc::from("1.1"),
            member.clone(),
            2,
        )
        .unwrap();
    assert_eq!(first.count, 1);
    assert!(receiver.recv().await.unwrap().contains("member_added"));

    let second = registry
        .join_presence(
            Arc::from("app"),
            Arc::from("presence-users"),
            Arc::from("1.2"),
            member,
            2,
        )
        .unwrap();
    assert_eq!(second.count, 1);
    assert!(receiver.try_recv().is_err());

    registry.reset_presence_node(2).unwrap();
    let removed = receiver.recv().await.unwrap();
    assert!(removed.contains("member_removed"));
    assert!(removed.contains(r#"\"user_id\":\"7\""#));
    assert!(registry.local_presence(2).is_empty());
}
