use super::*;
use claviron_pusher::{PublishedEvent, authorize_channel};
use claviron_runtime::Gate;
use std::collections::VecDeque;

struct Conversation {
    incoming: VecDeque<Message>,
    generation: Arc<Generation>,
    checked: bool,
}

impl MessageStream for Conversation {
    async fn receive(&mut self) -> crate::Result<Option<Message>> {
        if self.incoming.len() == 1 && !self.checked {
            self.checked = true;
            for channel in [
                "public-example",
                "private-example",
                "presence-example",
                "keep-example",
            ] {
                let event = PublishedEvent {
                    name: "example.updated".into(),
                    data: "{}".into(),
                    channels: vec![channel.into()],
                    socket_id: None,
                };
                assert_eq!(
                    self.generation.channels.publish("example", &event)?,
                    usize::from(channel == "keep-example")
                );
            }
            assert!(self.generation.channels.local_presence(0).is_empty());
        }
        Ok(self.incoming.pop_front())
    }
    async fn send(
        &mut self,
        _: Message,
    ) -> crate::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn audit_unsubscribe_releases_only_selected_memberships_in_both_wire_forms()
 {
    for string_data in [false, true] {
        let generation = Arc::new(Generation {
            cancellation: CancellationToken::new(),
            sessions: Gate::new(None, std::time::Duration::ZERO),
            channels: Default::default(),
            fleet: None,
            receive_fleet: false,
            groups: crate::groups::Groups::new(0),
        });
        let key = PusherAppKey::try_from("example-key".to_owned()).unwrap();
        let secret =
            PusherAppSecret::try_from("example-secret".to_owned()).unwrap();
        let (sender, outbound) = mpsc::channel(16);
        let session = BroadcastSession {
            application: Arc::from("example"),
            app_id: PusherAppId::try_from("1".to_owned()).unwrap(),
            key: key.clone(),
            secret: Some(secret.clone()),
            client_events: true,
            socket_id: Arc::from("1.1"),
            cancellation: CancellationToken::new(),
            generation: Arc::clone(&generation),
            outbound,
            sender,
            _permit: generation.sessions.try_enter().unwrap(),
            authenticator: None,
            authenticated: false,
        };
        let mut incoming = VecDeque::new();
        let frame = |event: &str, data: serde_json::Value| {
            let data = if string_data {
                serde_json::Value::String(data.to_string())
            } else {
                data
            };
            Message::Text(
                serde_json::json!({ "event": event, "data": data }).to_string(),
            )
        };
        for channel in [
            "public-example",
            "private-example",
            "presence-example",
            "keep-example",
        ] {
            let member = r#"{"user_id":"example-user"}"#;
            let data = channel.starts_with("presence-").then_some(member);
            incoming.push_back(frame("pusher:subscribe", serde_json::json!({
                "channel": channel, "channel_data": data,
                "auth": authorize_channel("1.1", channel, data, &key, &secret),
            })));
        }
        for channel in [
            "public-example",
            "private-example",
            "presence-example",
            "public-example",
            "unknown-example",
        ] {
            incoming.push_back(frame(
                "pusher:unsubscribe",
                serde_json::json!({ "channel": channel }),
            ));
        }
        incoming
            .push_back(Message::Close { code: 1000, reason: String::new() });
        let mut conversation = Conversation {
            incoming,
            generation: Arc::clone(&generation),
            checked: false,
        };
        run_messages(&mut conversation, session, std::future::pending())
            .await
            .unwrap();
        assert!(conversation.checked);
        // Disconnect does not double-release the left channels and still retires the retained one.
        let event = PublishedEvent {
            name: "example.updated".into(),
            data: "{}".into(),
            channels: vec!["keep-example".into()],
            socket_id: None,
        };
        assert_eq!(generation.channels.publish("example", &event).unwrap(), 0);
    }
}
