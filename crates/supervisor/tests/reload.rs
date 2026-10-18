use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use claviron_orbit::{EVENT_PAYLOAD_MAX, FleetEventCodec};
use claviron_runtime::registry::{Component, Reloadable, Runnable};
use claviron_runtime::{
    AppState, EventDispatcher, FleetCapacity, FleetNodeId, RunContext, RunMode,
    WorkerCount, WorkerId,
};
use tokio::sync::{Notify, mpsc};

use super::{
    SERVER_RELOADING_BYTES, ServerReloading, WORKER_RELOADED_BYTES,
    WorkerReloaded,
};

#[test]
fn reload_events_roundtrip_within_orbit_frame_limit() {
    let request =
        ServerReloading { generation: u64::MAX, target_worker_id: u8::MAX };
    let request_payload = request.encode_orbit();
    assert_eq!(request_payload.len(), SERVER_RELOADING_BYTES);
    assert_eq!(
        ServerReloading::decode_orbit(&request_payload)
            .expect("decode request"),
        request
    );
    assert!(
        12 + ServerReloading::TOPIC.len() + request_payload.len()
            <= EVENT_PAYLOAD_MAX
    );

    let acknowledgement =
        WorkerReloaded { generation: u64::MAX, worker_id: u8::MAX };
    let acknowledgement_payload = acknowledgement.encode_orbit();
    assert_eq!(acknowledgement_payload.len(), WORKER_RELOADED_BYTES);
    assert_eq!(
        WorkerReloaded::decode_orbit(&acknowledgement_payload)
            .expect("decode acknowledgement"),
        acknowledgement
    );
    assert!(
        12 + WorkerReloaded::TOPIC.len() + acknowledgement_payload.len()
            <= EVENT_PAYLOAD_MAX
    );
}

struct SlowReload {
    started: mpsc::UnboundedSender<()>,
    release: Notify,
    completed: AtomicUsize,
}

impl Component<AppState> for SlowReload {
    type Deps = ();

    fn name(&self) -> &'static str {
        "SlowReload"
    }
    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        Some(self)
    }
}

#[async_trait::async_trait]
impl Reloadable<AppState> for SlowReload {
    async fn reload(
        &self,
        _: &AppState,
    ) -> claviron_runtime::registry::Result<()> {
        self.started.send(()).unwrap();
        self.release.notified().await;
        self.completed.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[derive(Clone)]
struct Notice;
impl claviron_runtime::Event for Notice {}

#[tokio::test]
async fn worker_reload_is_serial_on_its_runner_and_cancellable_at_process_shutdown()
 {
    let state = AppState::initialize(
        RunMode::Worker(WorkerId::new(1)),
        FleetNodeId::new(1),
        WorkerCount::new(1),
        FleetCapacity::new(2).unwrap(),
        Some("reload-test".into()),
        ".",
    );
    state
        .registry_ref()
        .register(Arc::new(claviron_orbit::FleetEventRegistry::new()))
        .expect("register FleetEventRegistry");
    let (started, mut started_rx) = mpsc::unbounded_channel();
    let reloader = Arc::new(SlowReload {
        started,
        release: Notify::new(),
        completed: AtomicUsize::new(0),
    });
    state
        .registry_ref()
        .register(reloader.clone())
        .expect("register TestReloadable");
    let runner = Arc::new(
        crate::management::worker_reload::WorkerReloadRunner::default(),
    );
    state.registry_ref().register(runner.clone()).unwrap();
    let reload_run = tokio::spawn(runner.run(
        (),
        state.clone(),
        RunContext::new(state.shutdown_token()),
    ));
    tokio::task::yield_now().await;
    super::install_events(&state).unwrap();
    let mut acknowledgements = state.events().subscribe::<WorkerReloaded>();
    let (notice, mut notice_rx) = mpsc::unbounded_channel();
    state
        .events()
        .listen::<Notice, _>(move |_, _| {
            let notice = notice.clone();
            async move {
                notice.send(()).unwrap();
                Ok(())
            }
        })
        .unwrap();

    state
        .events()
        .emit(ServerReloading { generation: 17, target_worker_id: 0 });
    assert!(started_rx.try_recv().is_err());
    let dispatcher = tokio::spawn(
        state.registry_ref().resolve::<EventDispatcher>().unwrap().run(
            (),
            state.clone(),
            RunContext::new(state.shutdown_token()),
        ),
    );
    tokio::time::timeout(Duration::from_secs(1), started_rx.recv())
        .await
        .unwrap()
        .unwrap();
    state
        .events()
        .emit(ServerReloading { generation: 18, target_worker_id: 1 });
    state.events().emit(Notice);
    tokio::time::timeout(Duration::from_secs(1), notice_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        started_rx.try_recv().is_err(),
        "reloads of the same worker stay serial"
    );
    reloader.release.notify_one();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), acknowledgements.recv())
            .await
            .unwrap()
            .unwrap(),
        WorkerReloaded { generation: 17, worker_id: 1 }
    );
    tokio::time::timeout(Duration::from_secs(1), started_rx.recv())
        .await
        .unwrap()
        .unwrap();

    state.initiate_shutdown();
    tokio::time::timeout(Duration::from_secs(1), dispatcher)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    reload_run.await.unwrap().unwrap();
    assert_eq!(reloader.completed.load(Ordering::Relaxed), 1);
    assert!(
        acknowledgements.try_recv().is_err(),
        "cancelled reload must not acknowledge completion"
    );
}

#[tokio::test]
async fn worker_ignores_reload_targeted_at_another_worker() {
    let state = AppState::initialize(
        RunMode::Worker(WorkerId::new(1)),
        FleetNodeId::new(1),
        WorkerCount::new(2),
        FleetCapacity::new(3).unwrap(),
        Some("reload-target-test".into()),
        ".",
    );
    state
        .registry_ref()
        .register(Arc::new(claviron_orbit::FleetEventRegistry::new()))
        .expect("register FleetEventRegistry");
    let (started, mut started_rx) = mpsc::unbounded_channel();
    let reloader = Arc::new(SlowReload {
        started,
        release: Notify::new(),
        completed: AtomicUsize::new(0),
    });
    state
        .registry_ref()
        .register(reloader.clone())
        .expect("register TestReloadable");
    let runner = Arc::new(
        crate::management::worker_reload::WorkerReloadRunner::default(),
    );
    state.registry_ref().register(runner.clone()).unwrap();
    let reload_run = tokio::spawn(runner.run(
        (),
        state.clone(),
        RunContext::new(state.shutdown_token()),
    ));
    tokio::task::yield_now().await;
    super::install_events(&state).unwrap();
    let dispatcher = tokio::spawn(
        state.registry_ref().resolve::<EventDispatcher>().unwrap().run(
            (),
            state.clone(),
            RunContext::new(state.shutdown_token()),
        ),
    );

    state
        .events()
        .emit(ServerReloading { generation: 21, target_worker_id: 2 });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), started_rx.recv())
            .await
            .is_err(),
        "worker #1 must not run a reload targeted at worker #2"
    );

    state.initiate_shutdown();
    tokio::time::timeout(Duration::from_secs(1), dispatcher)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    reload_run.await.unwrap().unwrap();
    assert_eq!(reloader.completed.load(Ordering::Relaxed), 0);
}
