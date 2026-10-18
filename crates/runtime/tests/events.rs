use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use claviron_runtime::{
    AppState, Event, HasEvents, ProcessEventBus, Runtime, ShutdownInitiated,
};
use tokio::sync::{Notify, mpsc};

#[derive(Clone, Debug, PartialEq, Eq)]
struct Tick(u32);
impl Event for Tick {}

#[derive(Clone)]
struct Other;
impl Event for Other {}

#[test]
fn dispatcher_scope_admits_every_process_role() {
    use claviron_runtime::{Component, EventDispatcher};
    use continuo::fleet::Scope;

    let dispatcher = EventDispatcher::default();
    let scope =
        <EventDispatcher as Component<AppState>>::as_scoped(&dispatcher)
            .expect("local callbacks require explicit all-role admission")
            .scope();
    assert_eq!(scope, Scope::Any);
    for role in [Scope::Master, Scope::Worker, Scope::Any] {
        assert!(scope.includes(role));
    }
}

async fn recv<T>(receiver: &mut mpsc::UnboundedReceiver<T>) -> T {
    tokio::time::timeout(Duration::from_secs(2), receiver.recv())
        .await
        .unwrap()
        .unwrap()
}

async fn drain(runtime: &mut Runtime) {
    tokio::time::timeout(Duration::from_secs(2), runtime.drain())
        .await
        .unwrap()
        .unwrap();
}

#[test]
fn registration_needs_no_executor_and_does_not_retain_state() {
    let state = AppState::standalone("events-test", ".");
    let capture = Arc::new(());
    let weak = Arc::downgrade(&capture);
    state
        .events()
        .listen::<Tick, _>(move |_, _| {
            let capture = capture.clone();
            async move {
                drop(capture);
                Ok(())
            }
        })
        .unwrap();
    assert!(weak.upgrade().is_some());
    drop(state);
    assert!(weak.upgrade().is_none());
}

#[tokio::test]
async fn registration_buffers_future_events_and_preserves_the_shared_primitive()
{
    let state = AppState::standalone("events-test", ".");
    let other_state = AppState::standalone("other-state", ".");
    let primitive: ProcessEventBus = state.events().bus().clone();
    let mut raw = HasEvents::events(&state).subscribe::<Tick>();
    primitive.emit(Tick(0));
    assert_eq!(raw.recv().await.unwrap(), Tick(0));

    let (sender, mut receiver) = mpsc::unbounded_channel();
    state
        .events()
        .listen::<Tick, _>(move |_, event| {
            let sender = sender.clone();
            async move {
                sender.send(event).unwrap();
                Ok(())
            }
        })
        .unwrap();
    primitive.emit(Tick(1));
    primitive.emit(Other);
    other_state.events().emit(Tick(99));
    assert!(receiver.try_recv().is_err());

    let mut runtime = Runtime::default();
    assert_eq!(
        runtime.spawn_all(state.registry_ref(), state.clone()).unwrap(),
        1
    );
    assert_eq!(recv(&mut receiver).await, Tick(1));
    assert_eq!(raw.recv().await.unwrap(), Tick(1));
    state.initiate_shutdown();
    drain(&mut runtime).await;
    assert!(receiver.try_recv().is_err());
}

#[tokio::test]
async fn slow_listener_is_serial_without_blocking_another_listener_of_the_same_type()
 {
    let state = AppState::standalone("events-test", ".");
    let release = Arc::new(Notify::new());
    let wait = release.clone();
    let (slow, mut slow_rx) = mpsc::unbounded_channel();
    state
        .events()
        .listen::<Tick, _>(move |_, event| {
            let slow = slow.clone();
            let wait = wait.clone();
            async move {
                slow.send((event.0, "start")).unwrap();
                if event.0 == 1 {
                    wait.notified().await;
                }
                slow.send((event.0, "end")).unwrap();
                Ok(())
            }
        })
        .unwrap();
    let (fast, mut fast_rx) = mpsc::unbounded_channel();
    state
        .events()
        .listen::<Tick, _>(move |_, event| {
            let fast = fast.clone();
            async move {
                fast.send(event).unwrap();
                Ok(())
            }
        })
        .unwrap();

    let mut runtime = Runtime::default();
    assert_eq!(
        runtime.spawn_all(state.registry_ref(), state.clone()).unwrap(),
        1
    );
    state.events().emit(Tick(1));
    state.events().emit(Tick(2));
    assert_eq!(recv(&mut slow_rx).await, (1, "start"));
    assert_eq!(recv(&mut fast_rx).await, Tick(1));
    assert_eq!(recv(&mut fast_rx).await, Tick(2));
    assert!(slow_rx.try_recv().is_err());

    // Shutdown drains the in-flight handler and its buffered second event.
    state.initiate_shutdown();
    release.notify_one();
    drain(&mut runtime).await;
    assert_eq!(recv(&mut slow_rx).await, (1, "end"));
    assert_eq!(recv(&mut slow_rx).await, (2, "start"));
    assert_eq!(recv(&mut slow_rx).await, (2, "end"));
}

#[tokio::test]
async fn error_does_not_remove_listener_and_callback_can_emit_another_event() {
    let state = AppState::standalone("events-test", ".");
    state
        .events()
        .listen::<Tick, _>(|state, event| async move {
            if event.0 == 1 {
                return Err(claviron_runtime::Error::msg("test failure"));
            }
            state.events().emit(Other);
            Ok(())
        })
        .unwrap();
    let (sender, mut receiver) = mpsc::unbounded_channel();
    state
        .events()
        .listen::<Other, _>(move |_, _| {
            let sender = sender.clone();
            async move {
                sender.send(()).unwrap();
                Ok(())
            }
        })
        .unwrap();
    let mut runtime = Runtime::default();
    runtime.spawn_all(state.registry_ref(), state.clone()).unwrap();
    state.events().emit(Tick(1));
    state.events().emit(Tick(2));
    recv(&mut receiver).await;
    state.initiate_shutdown();
    drain(&mut runtime).await;
}

#[tokio::test]
async fn dispatcher_stop_retains_events_for_the_next_generation() {
    let state = AppState::standalone("events-restart-test", ".");
    let (sender, mut receiver) = mpsc::unbounded_channel();
    state
        .events()
        .listen::<Tick, _>(move |_, event| {
            let sender = sender.clone();
            async move {
                sender.send(event).unwrap();
                Ok(())
            }
        })
        .unwrap();

    let mut runtime = Runtime::default();
    let manager = runtime.manager();
    runtime.spawn_all(state.registry_ref(), state.clone()).unwrap();
    let runtime_state = state.clone();
    let runtime_task = tokio::spawn(async move {
        let result = runtime.wait_until_shutdown(&runtime_state).await;
        (runtime, result)
    });

    state.events().emit(Tick(1));
    assert_eq!(recv(&mut receiver).await, Tick(1));
    manager.stop("EventDispatcher").await.unwrap();

    state.events().emit(Tick(2));
    assert!(receiver.try_recv().is_err());
    manager.start("EventDispatcher").await.unwrap();
    assert_eq!(recv(&mut receiver).await, Tick(2));

    state.initiate_shutdown();
    let (mut runtime, result) = runtime_task.await.unwrap();
    result.unwrap();
    drain(&mut runtime).await;
}

#[tokio::test]
async fn submission_seals_registration_even_when_no_listeners_were_registered()
{
    let state = AppState::standalone("events-test", ".");
    let mut runtime = Runtime::default();
    assert_eq!(
        runtime.spawn_all(state.registry_ref(), state.clone()).unwrap(),
        0
    );
    assert!(state.events().listen::<Tick, _>(|_, _| async { Ok(()) }).is_err());
    drain(&mut runtime).await;
}

#[tokio::test]
async fn shutdown_before_first_dispatcher_poll_still_delivers_its_notification()
{
    let state = AppState::standalone("events-test", ".");
    let (sender, mut receiver) = mpsc::unbounded_channel();
    state
        .events()
        .listen::<ShutdownInitiated, _>(move |state, _| {
            let sender = sender.clone();
            async move {
                sender.send(state.is_shutting_down()).unwrap();
                Ok(())
            }
        })
        .unwrap();
    state.initiate_shutdown();
    let mut runtime = Runtime::default();
    runtime.spawn_all(state.registry_ref(), state.clone()).unwrap();
    drain(&mut runtime).await;
    assert!(recv(&mut receiver).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_notification_is_not_lost_to_a_cancellation_race() {
    for _ in 0..100 {
        let state = AppState::standalone("events-test", ".");
        let (sender, mut receiver) = mpsc::unbounded_channel();
        state
            .events()
            .listen::<ShutdownInitiated, _>(move |state, _| {
                let sender = sender.clone();
                async move {
                    sender.send(state.is_shutting_down()).unwrap();
                    Ok(())
                }
            })
            .unwrap();
        let mut runtime = Runtime::default();
        runtime.spawn_all(state.registry_ref(), state.clone()).unwrap();
        std::thread::spawn(move || state.initiate_shutdown()).join().unwrap();
        drain(&mut runtime).await;
        assert!(recv(&mut receiver).await);
    }
}

struct Dropped(Arc<AtomicBool>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn abort_and_drain_drops_in_flight_callback_futures() {
    let state = AppState::standalone("events-test", ".");
    let dropped = Arc::new(AtomicBool::new(false));
    let flag = dropped.clone();
    let (sender, mut receiver) = mpsc::unbounded_channel();
    state
        .events()
        .listen::<Tick, _>(move |_, _| {
            let flag = flag.clone();
            let sender = sender.clone();
            async move {
                let _guard = Dropped(flag);
                sender.send(()).unwrap();
                std::future::pending::<()>().await;
                Ok(())
            }
        })
        .unwrap();
    let mut runtime = Runtime::default();
    runtime.spawn_all(state.registry_ref(), state.clone()).unwrap();
    state.events().emit(Tick(1));
    recv(&mut receiver).await;
    assert!(!dropped.load(Ordering::SeqCst));
    runtime.abort_and_drain().await;
    assert!(dropped.load(Ordering::SeqCst));
}
