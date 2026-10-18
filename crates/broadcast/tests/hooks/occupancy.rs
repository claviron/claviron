use super::*;
use std::convert::Infallible;
use std::sync::Mutex;

fn change(kind: OccupancyKind) -> OccupancyChange {
    OccupancyChange {
        kind,
        app_id: "example-app".into(),
        channel: "presence-room".into(),
        time: "2026-10-08T00:00:00.000000Z".into(),
        user_id: Some("example-user".into()),
        user_info: Some(serde_json::json!({ "name": "Ada" })),
    }
}

#[tokio::test]
async fn callbacks_receive_each_transition_in_registration_order() {
    let hooks = Hooks::default();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mut subscriptions = Vec::new();
    for index in 0..2 {
        let calls = calls.clone();
        subscriptions.push(hooks.on_occupancy(move |change| {
            let calls = calls.clone();
            async move {
                calls.lock().unwrap().push((index, change));
                Ok::<(), Infallible>(())
            }
        }));
    }
    let occupied = change(OccupancyKind::Occupied);
    let vacated = change(OccupancyKind::Vacated);
    hooks.dispatch(occupied.clone()).await;
    hooks.dispatch(vacated.clone()).await;
    assert_eq!(
        *calls.lock().unwrap(),
        vec![
            (0, occupied.clone()),
            (1, occupied),
            (0, vacated.clone()),
            (1, vacated),
        ]
    );
}

#[tokio::test]
async fn dropping_subscription_releases_captures_and_stops_delivery() {
    let hooks = Hooks::default();
    let calls = Arc::new(Mutex::new(0));
    let captured = calls.clone();
    let subscription = hooks.on_occupancy(move |_| {
        let calls = captured.clone();
        async move {
            *calls.lock().unwrap() += 1;
            Ok::<(), Infallible>(())
        }
    });
    hooks.dispatch(change(OccupancyKind::Occupied)).await;
    drop(subscription);
    assert_eq!(Arc::strong_count(&calls), 1);
    hooks.dispatch(change(OccupancyKind::Vacated)).await;
    assert_eq!(*calls.lock().unwrap(), 1);
}

#[tokio::test]
async fn failed_callback_does_not_suppress_the_next_callback() {
    let hooks = Hooks::default();
    let _failed = hooks.on_occupancy(|_| async { Err::<(), _>("rejected") });
    let calls = Arc::new(Mutex::new(0));
    let captured = calls.clone();
    let _next = hooks.on_occupancy(move |_| {
        let calls = captured.clone();
        async move {
            *calls.lock().unwrap() += 1;
            Ok::<(), Infallible>(())
        }
    });
    hooks.dispatch(change(OccupancyKind::Occupied)).await;
    assert_eq!(*calls.lock().unwrap(), 1);
}

#[tokio::test]
async fn callback_can_register_without_holding_the_registry_lock() {
    let hooks = Hooks::default();
    let added = Arc::new(Mutex::new(None));
    let captured = added.clone();
    let registry = hooks.clone();
    let _first = hooks.on_occupancy(move |_| {
        let subscription =
            registry.on_occupancy(|_| async { Ok::<(), Infallible>(()) });
        *captured.lock().unwrap() = Some(subscription);
        async { Ok::<(), Infallible>(()) }
    });
    hooks.dispatch(change(OccupancyKind::Occupied)).await;
    assert!(added.lock().unwrap().is_some());
}
