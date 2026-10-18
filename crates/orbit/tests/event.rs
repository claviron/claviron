use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use claviron_runtime::{
    FleetCapacity, FleetNodeId, RunMode, Runtime, WorkerCount, WorkerId,
};
use orbitive::events::{EVENT_RING_KIND, EVENT_RING_SPEC};
use orbitive::{Fleet, NodeId, OrbitTyped, RingSpec};
use tokio::sync::mpsc;

use super::*;

#[test]
fn event_registry_scope_admits_every_process_role() {
    fn assert_all_roles<L: EventRing>() {
        let registry = EventRegistry::<L>::new();
        let scope =
            <EventRegistry<L> as Component<AppState>>::as_scoped(&registry)
                .expect("event transport requires explicit all-role admission")
                .scope();
        assert_eq!(scope, Scope::Any);
        for role in [Scope::Master, Scope::Worker, Scope::Any] {
            assert!(scope.includes(role));
        }
    }

    assert_all_roles::<DefaultEventLayout>();
    assert_all_roles::<ApplicationEventLayout>();
}

/// `FleetEventRegistry` depends on the fleet provider; these tests hand the
/// registry its bus directly, so the provider is registered and never booted.
fn register_fleet_provider(state: &AppState) {
    state
        .registry_ref()
        .register(Arc::new(FleetProvider::new()))
        .expect("register FleetProvider");
}

// Same transport identity, used only to unlink this isolated test segment.
#[derive(Clone)]
struct TestEventRing;
impl OrbitTyped for TestEventRing {
    const KIND: u8 = EVENT_RING_KIND;
    const RING_SPEC: RingSpec = EVENT_RING_SPEC;
}

struct TestFleet(Arc<Fleet>);
impl Drop for TestFleet {
    fn drop(&mut self) {
        self.0.shm_ring::<TestEventRing>().unwrap().unlink().unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn direct_publish_commits_after_cancellation_without_an_executor_turn() {
    let name =
        Box::leak(format!("se{:x}", std::process::id()).into_boxed_str());
    let master =
        TestFleet(Arc::new(Fleet::join_shm_as(name, 2, NodeId::ZERO).unwrap()));
    let reader = FleetEventBus::new(master.0.clone());
    reader.reset_ring().unwrap();
    let mut cursor = reader.cursor_at_head();
    let state = AppState::initialize(
        RunMode::Worker(WorkerId::new(1)),
        FleetNodeId::new(1),
        WorkerCount::new(1),
        FleetCapacity::new(2).unwrap(),
        Some("shutdown-event-test".into()),
        ".",
    );
    let publisher = FleetEventRegistry::new();
    publisher.install_bus(Arc::new(FleetEventBus::new(Arc::new(
        Fleet::join_shm_as(name, 2, NodeId::new(1)).unwrap(),
    ))));
    state.initiate_shutdown();
    // No bridge, poller, ACK, sleep, or yield drives this write.
    let id = publisher
        .publish_raw("test.shutdown.completed", &[1, 0, 0, 0, 42])
        .unwrap();
    drop(publisher);
    let poll = reader.poll(&mut cursor);
    assert_eq!(poll.lagged, 0);
    assert_eq!(poll.events.len(), 1);
    assert_eq!(poll.events[0].id, id);
    assert_eq!(poll.events[0].topic, "test.shutdown.completed");
    assert_eq!(poll.events[0].payload, [1, 0, 0, 0, 42]);
}

#[test]
fn direct_publish_requires_a_booted_bus() {
    let publisher = FleetEventRegistry::new();
    assert!(
        publisher
            .publish_raw("test.shutdown.completed", &[])
            .unwrap_err()
            .contains("not booted")
    );
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Signal(u8);
impl Event for Signal {}
impl FleetEventCodec for Signal {
    const TOPIC: &'static str = "test.signal";
    fn encode_orbit(&self) -> Vec<u8> {
        vec![self.0]
    }
    fn decode_orbit(bytes: &[u8]) -> Result<Self, String> {
        match bytes {
            [value] => Ok(Self(*value)),
            _ => Err("invalid signal".into()),
        }
    }
}

#[derive(Clone)]
struct OtherSignal(u8);
impl Event for OtherSignal {}
impl FleetEventCodec for OtherSignal {
    const TOPIC: &'static str = "test.other";
    fn encode_orbit(&self) -> Vec<u8> {
        vec![self.0]
    }
    fn decode_orbit(bytes: &[u8]) -> Result<Self, String> {
        Signal::decode_orbit(bytes).map(|signal| Self(signal.0))
    }
}

#[derive(Clone)]
struct Collision;
impl Event for Collision {}
impl FleetEventCodec for Collision {
    const TOPIC: &'static str = Signal::TOPIC;
    fn encode_orbit(&self) -> Vec<u8> {
        vec![]
    }
    fn decode_orbit(_: &[u8]) -> Result<Self, String> {
        Ok(Self)
    }
}

fn event_buses() -> (TestFleet, Arc<FleetEventBus>, FleetEventBus) {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let name = Box::leak(
        format!(
            "ev{:x}{:x}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )
        .into_boxed_str(),
    );
    let fleet =
        TestFleet(Arc::new(Fleet::join_shm_as(name, 2, NodeId::ZERO).unwrap()));
    let local = Arc::new(FleetEventBus::new(fleet.0.clone()));
    local.reset_ring().unwrap();
    let remote = FleetEventBus::new(Arc::new(
        Fleet::join_shm_as(name, 2, NodeId::new(1)).unwrap(),
    ));
    (fleet, local, remote)
}

#[tokio::test(flavor = "current_thread")]
async fn no_implicit_termination_event_shm_survives_master_replacement() {
    let (_fleet, local, remote) = event_buses();
    let retained = remote.publish(Signal::TOPIC, &[1]).unwrap();
    let registry = FleetEventRegistry::new();

    registry.boot_transport(local.clone()).unwrap();

    let mut history = local.cursor_from_start();
    let retained_poll = local.poll(&mut history);
    assert_eq!(retained_poll.lagged, 0);
    assert_eq!(retained_poll.events.len(), 1);
    assert_eq!(retained_poll.events[0].id, retained);

    let future = remote.publish(Signal::TOPIC, &[2]).unwrap();
    let mut inbound = registry.inbound.lock().unwrap().take().unwrap();
    let future_poll = inbound.bus.poll(&mut inbound.cursor);
    assert_eq!(future_poll.lagged, 0);
    assert_eq!(future_poll.events.len(), 1);
    assert_eq!(future_poll.events[0].id, future);
}

async fn received(receiver: &mut mpsc::UnboundedReceiver<u8>) -> u8 {
    tokio::time::timeout(Duration::from_secs(2), receiver.recv())
        .await
        .unwrap()
        .unwrap()
}

#[test]
fn bridge_registration_needs_no_executor_and_releases_its_captures() {
    let state = AppState::standalone("bridge-registration", ".");
    let registry = Arc::new(FleetEventRegistry::new());
    let weak = Arc::downgrade(&registry);
    registry.register_bridge::<Signal>(&state).unwrap();
    registry.register_bridge::<Signal>(&state).unwrap();
    assert!(registry.register_bridge::<Collision>(&state).is_err());
    assert_eq!(registry.handlers.read().unwrap().len(), 1);
    drop(registry);
    drop(state);
    assert!(weak.upgrade().is_none());
}

#[tokio::test]
async fn late_bridge_registration_fails_without_installing_an_inbound_handler()
{
    let state = AppState::standalone("late-bridge", ".");
    let registry = Arc::new(FleetEventRegistry::new());
    let mut runtime = Runtime::default();
    assert_eq!(
        runtime.spawn_all(state.registry_ref(), state.clone()).unwrap(),
        0
    );
    assert!(registry.register_bridge::<Signal>(&state).is_err());
    assert!(registry.handlers.read().unwrap().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn inbound_only_delivers_peer_events_without_echo_or_local_forwarding() {
    let (_fleet, local, remote) = event_buses();
    let mut audit = local.cursor_at_head();
    let state = AppState::standalone("inbound-only", ".");
    let registry = Arc::new(FleetEventRegistry::new());
    state.registry_ref().register(registry.clone()).unwrap();
    register_fleet_provider(&state);
    registry.register_inbound::<Signal>().unwrap();
    registry.register_inbound::<Signal>().unwrap();
    assert!(registry.register_inbound::<Collision>().is_err());
    assert!(registry.register_bridge::<Signal>(&state).is_err());
    let mut receiver = state.events().subscribe::<Signal>();
    registry.prepare_inbound(local.clone()).unwrap();
    let mut runtime = Runtime::default();
    assert_eq!(
        runtime.spawn_all(state.registry_ref(), state.clone()).unwrap(),
        1
    );
    remote.publish(Signal::TOPIC, &[1]).unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(500), receiver.recv())
            .await
            .unwrap()
            .unwrap(),
        Signal(1)
    );
    state.events().emit(Signal(2));
    assert_eq!(receiver.try_recv().unwrap(), Signal(2));
    state.initiate_shutdown();
    tokio::time::timeout(Duration::from_millis(500), runtime.drain())
        .await
        .unwrap()
        .unwrap();
    let published = local.poll(&mut audit).events;
    assert_eq!(
        published.len(),
        1,
        "neither peer replay nor local emit is forwarded"
    );
    assert_eq!(published[0].id.node(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn managed_bridge_preserves_the_boot_cursor_and_shares_local_callbacks_without_echo()
 {
    let (_fleet, local, remote) = event_buses();
    // A cursor prepared later must not replay earlier transport history.
    remote.publish(Signal::TOPIC, &[99]).unwrap();
    let mut audit = local.cursor_at_head();
    let state = AppState::standalone("managed-bridge", ".");
    let registry = Arc::new(FleetEventRegistry::new());
    state
        .registry_ref()
        .register(registry.clone())
        .expect("register FleetEventRegistry");
    register_fleet_provider(&state);
    registry.register_bridge::<Signal>(&state).unwrap();
    registry.register_bridge::<Signal>(&state).unwrap();
    registry.register_bridge::<OtherSignal>(&state).unwrap();
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let signal_sender = sender.clone();
    state
        .events()
        .listen::<Signal, _>(move |_, signal| {
            let sender = signal_sender.clone();
            async move {
                sender.send(signal.0).unwrap();
                Ok(())
            }
        })
        .unwrap();
    state
        .events()
        .listen::<OtherSignal, _>(move |_, signal| {
            let sender = sender.clone();
            async move {
                sender.send(signal.0).unwrap();
                Ok(())
            }
        })
        .unwrap();
    registry.prepare_inbound(local.clone()).unwrap();
    remote.publish(Signal::TOPIC, &[1]).unwrap();
    remote.publish(OtherSignal::TOPIC, &[2]).unwrap();
    state.events().emit(Signal(3));
    tokio::task::yield_now().await;
    assert!(receiver.try_recv().is_err(), "preparation must not run callbacks");
    assert_eq!(
        local.poll(&mut audit).events.len(),
        2,
        "no forwarding before run"
    );

    let mut runtime = Runtime::default();
    // One local dispatcher plus one Orbit inbound loop, regardless of types.
    assert_eq!(
        runtime.spawn_all(state.registry_ref(), state.clone()).unwrap(),
        2
    );
    let mut initial = vec![
        received(&mut receiver).await,
        received(&mut receiver).await,
        received(&mut receiver).await,
    ];
    initial.sort();
    assert_eq!(initial, [1, 2, 3]);
    remote.publish(Signal::TOPIC, &[4]).unwrap();
    assert_eq!(received(&mut receiver).await, 4);
    state.initiate_shutdown();
    tokio::time::timeout(Duration::from_secs(2), runtime.drain())
        .await
        .unwrap()
        .unwrap();
    let published = local.poll(&mut audit).events;
    assert_eq!(published.len(), 2, "remote delivery must not create an echo");
    let own: Vec<_> =
        published.iter().filter(|event| event.id.node() == 0).collect();
    assert_eq!(
        own.len(),
        1,
        "duplicate registration must not duplicate publication"
    );
    assert_eq!(own[0].payload, [3]);
    assert!(receiver.try_recv().is_err());
    assert!(
        registry.inbound.lock().unwrap().is_some(),
        "inbound cursor is retained after stop"
    );
    // Final worker publication still works after managed teardown.
    registry.publish_raw(Signal::TOPIC, &[5]).unwrap();
    assert_eq!(local.poll(&mut audit).events[0].payload, [5]);
}

#[tokio::test]
async fn unprepared_inbound_failure_reaches_runtime() {
    let state = AppState::standalone("unprepared-bridge", ".");
    state
        .registry_ref()
        .register(Arc::new(FleetEventRegistry::new()))
        .expect("register FleetEventRegistry");
    register_fleet_provider(&state);
    let mut runtime = Runtime::default();
    assert_eq!(
        runtime.spawn_all(state.registry_ref(), state.clone()).unwrap(),
        1
    );
    let error = tokio::time::timeout(
        Duration::from_secs(2),
        runtime.wait_until_shutdown(&state),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(error.to_string().contains("not prepared"));
}

#[tokio::test]
async fn codec_alone_does_not_enable_forwarding_and_abort_stops_inbound() {
    let (_fleet, local, remote) = event_buses();
    let mut audit = local.cursor_at_head();
    let state = AppState::standalone("unbridged-event", ".");
    let registry = Arc::new(FleetEventRegistry::new());
    state
        .registry_ref()
        .register(registry.clone())
        .expect("register FleetEventRegistry");
    register_fleet_provider(&state);
    registry.prepare_inbound(local.clone()).unwrap();
    let (sender, mut receiver) = mpsc::unbounded_channel();
    state
        .events()
        .listen::<Signal, _>(move |_, signal| {
            let sender = sender.clone();
            async move {
                sender.send(signal.0).unwrap();
                Ok(())
            }
        })
        .unwrap();
    remote.publish(Signal::TOPIC, &[1]).unwrap();
    state.events().emit(Signal(2));
    let mut runtime = Runtime::default();
    assert_eq!(
        runtime.spawn_all(state.registry_ref(), state.clone()).unwrap(),
        2
    );
    assert_eq!(received(&mut receiver).await, 2);
    tokio::time::timeout(Duration::from_secs(2), async {
        while registry.inbound.lock().unwrap().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    runtime.abort_and_drain().await;
    assert!(
        registry.inbound.lock().unwrap().is_none(),
        "the runnable took ownership"
    );
    assert!(receiver.try_recv().is_err());
    let published = local.poll(&mut audit).events;
    assert_eq!(published.len(), 1, "an unbridged local event must stay local");
    assert_eq!(published[0].id.node(), 1);
    // Abort stops consuming but does not revoke direct publication capability.
    registry.publish_raw(Signal::TOPIC, &[3]).unwrap();
    assert_eq!(local.poll(&mut audit).events[0].payload, [3]);
}
