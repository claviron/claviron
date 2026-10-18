use claviron_app::{ApplicationId, ApplicationName, ApplicationRuntime};

use super::*;

fn id(
    runtime: &str,
    name: &str,
) -> ApplicationId {
    ApplicationId::new(
        ApplicationRuntime::new(runtime).unwrap(),
        ApplicationName::new(name).unwrap(),
    )
}

#[test]
fn application_subscription_rejects_other_runtime_and_application_targets() {
    let events = ProcessEventBus::new();
    let mut subscription =
        EventSubscription::new(&events, Some(id("php", "octane-app")), None, 2);
    subscription.subscribe("App\\Events\\DemoEvent".to_owned());

    let own = ReceivedEvent(
        ApplicationEvent::for_application(
            id("php", "octane-app"),
            "App\\Events\\DemoEvent",
            EventValue::Null,
        )
        .unwrap(),
    );
    let other_application = ReceivedEvent(
        ApplicationEvent::for_application(
            id("php", "symfony-app"),
            "App\\Events\\DemoEvent",
            EventValue::Null,
        )
        .unwrap(),
    );
    let other_runtime = ReceivedEvent(
        ApplicationEvent::for_application(
            id("js", "octane-app"),
            "App\\Events\\DemoEvent",
            EventValue::Null,
        )
        .unwrap(),
    );
    let shared = ReceivedEvent(
        ApplicationEvent::new("App\\Events\\DemoEvent", EventValue::Null)
            .unwrap(),
    );

    assert!(subscription.accepts(&own));
    assert!(!subscription.accepts(&other_application));
    assert!(!subscription.accepts(&other_runtime));
    assert!(subscription.accepts(&shared));
}

fn shared(topic: &str) -> ApplicationEvent {
    ApplicationEvent::new(topic, EventValue::Null).unwrap()
}

#[test]
fn unrelated_topics_and_other_targets_cannot_exhaust_the_php_inbox() {
    // The process channel and PHP inbox deliberately have different bounds.
    let events = ProcessEventBus::with_capacity(1);
    let application = id("php", "example-app");
    let mut subscription =
        EventSubscription::new(&events, Some(application.clone()), None, 2);
    subscription.subscribe("orders.changed".into());
    for _ in 0..256 {
        events.emit(shared("unrelated"));
        events.emit(
            ApplicationEvent::for_application(
                id("js", "example-app"),
                "orders.changed",
                EventValue::Null,
            )
            .unwrap(),
        );
        events.emit(
            ApplicationEvent::for_application(
                id("php", "other-app"),
                "orders.changed",
                EventValue::Null,
            )
            .unwrap(),
        );
    }
    assert!(subscription.try_receive().unwrap().is_none());
    events.emit(shared("orders.changed"));
    events.emit(
        ApplicationEvent::for_application(
            application,
            "orders.changed",
            EventValue::Null,
        )
        .unwrap(),
    );
    assert_eq!(
        subscription.try_receive().unwrap().unwrap().topic(),
        "orders.changed"
    );
    assert_eq!(
        subscription.try_receive().unwrap().unwrap().topic(),
        "orders.changed"
    );
    assert!(subscription.try_receive().unwrap().is_none());
}

#[test]
fn topics_start_at_registration_without_replay_and_can_be_changed() {
    let events = ProcessEventBus::new();
    events.emit(shared("first"));
    let mut subscription = EventSubscription::new(&events, None, None, 2);
    events.emit(shared("first"));
    assert!(subscription.subscribe("first".into()));
    assert!(!subscription.subscribe("first".into()));
    assert!(subscription.try_receive().unwrap().is_none());
    events.emit(shared("second"));
    assert_eq!(
        subscription.subscribe_many(["first".into(), "second".into()]),
        1
    );
    assert!(subscription.try_receive().unwrap().is_none());
    events.emit(shared("first"));
    subscription.defer(ReceivedEvent(shared("first")));
    assert!(subscription.unsubscribe("first"));
    events.emit(shared("first"));
    events.emit(shared("second"));
    assert_eq!(subscription.try_receive().unwrap().unwrap().topic(), "second");
    assert!(subscription.try_receive().unwrap().is_none());
    assert!(!subscription.is_empty());
    assert!(subscription.unsubscribe("second"));
    assert!(subscription.is_empty());
}

#[test]
fn task_subscription_accepts_only_shared_topics_and_reports_real_overflow() {
    let events = ProcessEventBus::new();
    let mut subscription = EventSubscription::new(&events, None, None, 2);
    subscription.subscribe("jobs.completed".into());
    for _ in 0..256 {
        events.emit(
            ApplicationEvent::for_application(
                id("php", "example-app"),
                "jobs.completed",
                EventValue::Null,
            )
            .unwrap(),
        );
    }
    assert!(subscription.try_receive().unwrap().is_none());
    for _ in 0..4 {
        events.emit(shared("jobs.completed"));
    }
    assert_eq!(
        subscription.try_receive().unwrap_err(),
        "PHP application event subscription lagged by 2 event(s)"
    );
}

#[tokio::test]
async fn application_receive_reports_matching_overflow_too() {
    let events = ProcessEventBus::new();
    let mut subscription = EventSubscription::new(
        &events,
        Some(id("php", "example-app")),
        None,
        2,
    );
    subscription.subscribe("orders.changed".into());
    for _ in 0..4 {
        events.emit(shared("orders.changed"));
    }
    assert_eq!(
        subscription.receive().await.unwrap_err(),
        "PHP application event subscription lagged by 2 event(s)"
    );
    assert_eq!(subscription.receive().await.unwrap().topic(), "orders.changed");
}
