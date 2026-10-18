use std::time::Duration;

use claviron_http::config::{HttpYaml, ListenerConfig};
use tokio::sync::{mpsc, oneshot};

use super::*;
use crate::tcp::listener::accept_support as fault;

#[tokio::test]
async fn tcp_port_sharing_is_only_enabled_for_workers() {
    let worker = RunMode::Worker(claviron_runtime::WorkerId::new(1));
    for mode in [RunMode::Standalone, RunMode::Master] {
        let listener =
            bind_socket("127.0.0.1:0".parse().unwrap(), mode).await.unwrap();
        let addr = listener.local_addr().unwrap();
        for competitor in [mode, worker] {
            assert_eq!(
                bind_socket(addr, competitor).await.unwrap_err().kind(),
                std::io::ErrorKind::AddrInUse
            );
        }
    }
    let first =
        bind_socket("127.0.0.1:0".parse().unwrap(), worker).await.unwrap();
    let second_worker = RunMode::Worker(claviron_runtime::WorkerId::new(2));
    let _second =
        bind_socket(first.local_addr().unwrap(), second_worker).await.unwrap();
}

struct Snapshot(Arc<ListenerConfig>);
impl ListenerSnapshot for Snapshot {
    fn listener_id(&self) -> &ListenerId {
        &self.0.id
    }

    fn same_binding(
        &self,
        other: &Self,
    ) -> bool {
        self.0.same_binding(&other.0)
    }
}
impl crate::tcp::listener::TcpListenerSnapshot for Snapshot {
    fn listener(&self) -> &ListenerConfig {
        &self.0
    }
}
fn group(
    name: &str,
    addresses: &[SocketAddr],
    grace: &str,
) -> Arc<Snapshot> {
    let addresses: Vec<_> =
        addresses.iter().map(|addr| format!("'{addr}'")).collect();
    let yaml = format!(
        "listeners: {{{name}: {{transport: tcp, protocols: h1, listen: [{}], max_connections: 1, acquire_timeout: 5s, graceful_timeout: {grace}, requests: {{max_body_size: off, body_read_timeout: off}}}}}}",
        addresses.join(",")
    );
    let http =
        serde_yaml::from_str::<HttpYaml>(&yaml).unwrap().resolve().unwrap();
    Arc::new(Snapshot(http.listeners.into_values().next().unwrap()))
}
async fn socket() -> TcpListener {
    TcpListener::bind("127.0.0.1:0").await.unwrap()
}

#[tokio::test]
async fn addresses_share_one_gate_but_other_groups_keep_accepting() {
    let a1 = socket().await;
    let a2 = socket().await;
    let b1 = socket().await;
    let addresses = [
        a1.local_addr().unwrap(),
        a2.local_addr().unwrap(),
        b1.local_addr().unwrap(),
    ];
    let a = group("a", &addresses[..2], "null");
    let b = group("b", &addresses[2..], "null");
    let snapshots = ListenerSnapshots::default();
    let context = RunContext::new(Default::default());
    let cancel = context.cancellation_token();
    let (started, mut receiver) = mpsc::unbounded_channel();
    let task = tokio::spawn(async move {
        let _generation = snapshots
            .start(BTreeMap::from([
                (a.0.id.clone(), a.clone()),
                (b.0.id.clone(), b.clone()),
            ]))
            .unwrap();
        let handler = move |snapshot: Arc<Snapshot>, stream, _, permit| {
            let started = started.clone();
            async move {
                let _owned = (stream, permit);
                let (release, wait) = oneshot::channel::<()>();
                started.send((snapshot.0.id.clone(), release)).unwrap();
                let _ = wait.await;
            }
        };
        let (a, b) = tokio::join!(
            serve_group(&snapshots, a, vec![a1, a2], &context, handler.clone()),
            serve_group(&snapshots, b, vec![b1], &context, handler)
        );
        a.unwrap();
        b.unwrap();
    });
    let _a1 = TcpStream::connect(addresses[0]).await.unwrap();
    let (id, release_a1) =
        tokio::time::timeout(Duration::from_secs(5), receiver.recv())
            .await
            .unwrap()
            .unwrap();
    assert_eq!(id.as_str(), "a");
    let _a2 = TcpStream::connect(addresses[1]).await.unwrap();
    let _b1 = TcpStream::connect(addresses[2]).await.unwrap();
    let (id, release_b1) =
        tokio::time::timeout(Duration::from_secs(5), receiver.recv())
            .await
            .unwrap()
            .unwrap();
    assert_eq!(
        id.as_str(),
        "b",
        "second address must not create a second gate for a"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(50), receiver.recv())
            .await
            .is_err()
    );
    release_a1.send(()).unwrap();
    let (id, release_a2) =
        tokio::time::timeout(Duration::from_secs(5), receiver.recv())
            .await
            .unwrap()
            .unwrap();
    assert_eq!(id.as_str(), "a");
    release_a2.send(()).unwrap();
    release_b1.send(()).unwrap();
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), task).await.unwrap().unwrap();
}

#[tokio::test]
async fn indefinite_drain_waits_and_bounded_drain_releases_owned_tasks() {
    for grace in ["null", "0s"] {
        let socket = socket().await;
        let address = socket.local_addr().unwrap();
        let snapshot = group("drain", &[address], grace);
        let snapshots = ListenerSnapshots::default();
        let context = RunContext::new(Default::default());
        let cancel = context.cancellation_token();
        let (started, wait_started) = oneshot::channel();
        let started = Arc::new(std::sync::Mutex::new(Some(started)));
        let (release, wait) = oneshot::channel::<()>();
        let wait = Arc::new(std::sync::Mutex::new(Some(wait)));
        let mut task = tokio::spawn(async move {
            let _generation = snapshots
                .start(BTreeMap::from([(
                    snapshot.0.id.clone(),
                    snapshot.clone(),
                )]))
                .unwrap();
            serve_group(
                &snapshots,
                snapshot,
                vec![socket],
                &context,
                move |_, stream, _, permit| {
                    let started = started.lock().unwrap().take().unwrap();
                    let wait = wait.lock().unwrap().take().unwrap();
                    async move {
                        let _owned = (stream, permit);
                        let _ = started.send(());
                        let _ = wait.await;
                    }
                },
            )
            .await
            .unwrap();
        });
        let _client = TcpStream::connect(address).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), wait_started)
            .await
            .unwrap()
            .unwrap();
        cancel.cancel();
        if grace == "null" {
            assert!(
                tokio::time::timeout(Duration::from_millis(50), &mut task)
                    .await
                    .is_err()
            );
            release.send(()).unwrap();
        }
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap();
        // All listeners and accepted task IO have been dropped on return.
        assert!(TcpListener::bind(address).await.is_ok());
    }
}

#[tokio::test]
async fn handed_off_session_outlives_connection_but_not_listener_drain() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    struct Released(Arc<AtomicUsize>);
    impl Drop for Released {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    for grace in ["null", "0s"] {
        let socket = socket().await;
        let address = socket.local_addr().unwrap();
        let snapshot = group("session-drain", &[address], grace);
        let snapshots = ListenerSnapshots::default();
        let context = RunContext::new(Default::default());
        let cancel = context.cancellation_token();
        let (handed_off, mut completed) = mpsc::unbounded_channel();
        let dropped = Arc::new(AtomicUsize::new(0));
        let observed = dropped.clone();
        let mut task = tokio::spawn(async move {
            let _generation = snapshots
                .start(BTreeMap::from([(
                    snapshot.0.id.clone(),
                    snapshot.clone(),
                )]))
                .unwrap();
            serve_group(
                &snapshots,
                snapshot,
                vec![socket],
                &context,
                move |_, mut stream, _, scope| {
                    let handed_off = handed_off.clone();
                    let guard = Released(observed.clone());
                    async move {
                        let sessions = scope.sessions();
                        let signal = sessions.clone();
                        sessions
                            .spawn(async move {
                                let _guard = guard;
                                signal.wait_graceful_shutdown().await;
                                // A close handshake is cooperative, not an automatic drop.
                                stream.write_all(b"closing").await.unwrap();
                                let mut data = [0; 4];
                                let _ = stream.read_exact(&mut data).await;
                            })
                            .unwrap();
                        drop(scope);
                        handed_off.send(()).unwrap();
                    }
                },
            )
            .await
            .unwrap();
        });
        let mut client = TcpStream::connect(address).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), completed.recv())
            .await
            .unwrap()
            .unwrap();
        // The connection callback already returned; its session still consumes
        // the group's only slot. A second accepted socket must not dispatch.
        let _second = TcpStream::connect(address).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(25), completed.recv())
                .await
                .is_err()
        );
        cancel.cancel();
        if grace == "null" {
            let mut data = [0; 7];
            tokio::time::timeout(
                Duration::from_secs(5),
                client.read_exact(&mut data),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(&data, b"closing");
            assert!(
                tokio::time::timeout(Duration::from_millis(25), &mut task)
                    .await
                    .is_err()
            );
            assert_eq!(dropped.load(Ordering::SeqCst), 0);
            client.write_all(b"done").await.unwrap();
        }
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            dropped.load(Ordering::SeqCst),
            1,
            "session cleanup must finish before stopped"
        );
        let mut data = Vec::new();
        tokio::time::timeout(
            Duration::from_secs(5),
            client.read_to_end(&mut data),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(TcpListener::bind(address).await.is_ok());
    }
}

#[tokio::test]
async fn bind_failure_does_not_leave_partial_listeners() {
    let first = socket().await;
    let address = first.local_addr().unwrap();
    drop(first);
    let occupied = socket().await;
    let snapshot =
        group("partial", &[address, occupied.local_addr().unwrap()], "null");
    let snapshots = ListenerSnapshots::default();
    let generation = snapshots
        .start(BTreeMap::from([(snapshot.0.id.clone(), snapshot)]))
        .unwrap();
    let result = serve(
        &snapshots,
        generation.groups(),
        &RunContext::new(Default::default()),
        RunMode::Standalone,
        |_, _, _, _| async { panic!("must never accept on partial bind") },
    )
    .await;
    assert!(result.is_err());
    assert!(TcpListener::bind(address).await.is_ok());
}

#[tokio::test]
async fn resource_pressure_is_socket_local_and_stop_discards_retry_state() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Restart the same real binds after cancelling during an active cooldown.
    let mut addresses = Vec::new();
    for generation in 0..2 {
        let mut sources = Vec::new();
        let mut controls = Vec::new();
        for index in 0..3 {
            let socket = if generation == 0 {
                socket().await
            } else {
                TcpListener::bind(addresses[index]).await.unwrap()
            };
            if generation == 0 {
                addresses.push(socket.local_addr().unwrap());
            }
            let (source, control) = fault::Source::new(socket);
            sources.push(source);
            controls.push(control);
        }
        // The failed socket cannot consume the group's single connection slot.
        for _ in 0..100 {
            controls[0].inject(std::io::ErrorKind::OutOfMemory);
        }
        let mut b_sources = sources.split_off(2);
        let a = group("pressure", &addresses[..2], "null");
        let b = group("healthy", &addresses[2..], "null");
        let snapshots = ListenerSnapshots::default();
        let context = RunContext::new(Default::default());
        let cancel = context.cancellation_token();
        let (started, mut ready) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let _generation = snapshots
                .start(BTreeMap::from([
                    (a.0.id.clone(), a.clone()),
                    (b.0.id.clone(), b.clone()),
                ]))
                .unwrap();
            serve_bound(
                &snapshots,
                vec![(a, sources), (b, vec![b_sources.pop().unwrap()])],
                &context,
                move |snapshot: Arc<Snapshot>, mut stream, _, scope| {
                    let started = started.clone();
                    async move {
                        let _scope = scope;
                        started.send(snapshot.0.id.clone()).unwrap();
                        let mut byte = [0];
                        stream.read_exact(&mut byte).await.unwrap();
                        stream.write_all(&byte).await.unwrap();
                    }
                },
            )
            .await
        });
        tokio::time::timeout(
            Duration::from_secs(5),
            controls[0].wait_errors(1),
        )
        .await
        .unwrap();
        let mut sibling = TcpStream::connect(addresses[1]).await.unwrap();
        let mut independent = TcpStream::connect(addresses[2]).await.unwrap();
        for _ in 0..2 {
            tokio::time::timeout(Duration::from_secs(5), ready.recv())
                .await
                .unwrap()
                .unwrap();
        }
        for stream in [&mut sibling, &mut independent] {
            stream.write_all(b"x").await.unwrap();
            let mut byte = [0];
            tokio::time::timeout(
                Duration::from_secs(5),
                stream.read_exact(&mut byte),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(&byte, b"x");
        }
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(controls[0].polls() < 100, "no immediate resource-error spin");
        for address in &addresses {
            assert!(TcpListener::bind(address).await.is_ok());
        }
    }
}

#[tokio::test]
async fn fatal_accept_failure_drains_all_groups_and_owned_sessions_before_returning_error()
 {
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Released(Arc<AtomicUsize>);
    impl Drop for Released {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    for grace in ["null", "0s"] {
        let broken = socket().await;
        let healthy = socket().await;
        let addresses =
            [broken.local_addr().unwrap(), healthy.local_addr().unwrap()];
        let (broken, fault) = fault::Source::new(broken);
        let (healthy, _) = fault::Source::new(healthy);
        let a = group("broken", &addresses[..1], grace);
        let b = group("running", &addresses[1..], grace);
        let snapshots = ListenerSnapshots::default();
        let context = RunContext::new(Default::default());
        let parent = context.cancellation_token();
        let released = Arc::new(AtomicUsize::new(0));
        let observed = released.clone();
        let (started, mut ready) = mpsc::unbounded_channel();
        let (draining, mut drain_started) = mpsc::unbounded_channel();
        let (release, wait) = tokio::sync::watch::channel(false);
        let mut task = tokio::spawn(async move {
            let _generation = snapshots
                .start(BTreeMap::from([
                    (a.0.id.clone(), a.clone()),
                    (b.0.id.clone(), b.clone()),
                ]))
                .unwrap();
            serve_bound(
                &snapshots,
                vec![(a, vec![broken]), (b, vec![healthy])],
                &context,
                move |_: Arc<Snapshot>, stream, _, scope| {
                    let started = started.clone();
                    let draining = draining.clone();
                    let mut connection_wait = wait.clone();
                    let mut session_wait = wait.clone();
                    let connection_guard = Released(observed.clone());
                    let session_guard = Released(observed.clone());
                    async move {
                        let _connection_guard = connection_guard;
                        let sessions = scope.sessions();
                        let signal = sessions.clone();
                        sessions
                            .spawn(async move {
                                let _owned = (stream, session_guard);
                                signal.wait_graceful_shutdown().await;
                                let _ = draining.send(());
                                let _ = session_wait.changed().await;
                            })
                            .unwrap();
                        started.send(()).unwrap();
                        let _owned = scope;
                        let _ = connection_wait.changed().await;
                    }
                },
            )
            .await
        });
        let _client = TcpStream::connect(addresses[1]).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), ready.recv())
            .await
            .unwrap()
            .unwrap();
        fault.inject_error(std::io::Error::from_raw_os_error(-1234));
        tokio::time::timeout(Duration::from_secs(5), fault.wait_errors(1))
            .await
            .unwrap();
        if grace == "null" {
            tokio::time::timeout(Duration::from_secs(5), drain_started.recv())
                .await
                .unwrap()
                .unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(25), &mut task)
                    .await
                    .is_err()
            );
            assert_eq!(
                released.load(Ordering::SeqCst),
                0,
                "fatal accept does not invent force grace"
            );
            release.send(true).unwrap();
        }
        let result = tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap();
        let error = result.unwrap_err();
        assert!(error.to_string().contains("broken"));
        let Error::Other(source) = error else {
            panic!("expected contextual accept source")
        };
        let os_error =
            source.source().unwrap().downcast_ref::<std::io::Error>().unwrap();
        assert_eq!(os_error.raw_os_error(), Some(-1234));
        assert_eq!(
            released.load(Ordering::SeqCst),
            2,
            "connection and session are joined"
        );
        assert!(
            !parent.is_cancelled(),
            "failure must not stop other runtime services"
        );
        for address in addresses {
            assert!(TcpListener::bind(address).await.is_ok());
        }
    }
}

#[tokio::test(start_paused = true)]
async fn accept_recovery_policy_is_captured_until_the_next_generation() {
    use futures_util::FutureExt;

    let socket = socket().await;
    let address = socket.local_addr().unwrap();
    let original = group("captured", &[address], "null");
    let mut changed = original.0.as_ref().clone();
    let ListenerSettings::Tcp(config) = &mut changed.settings else {
        unreachable!()
    };
    config.tcp.accept_retry.initial = Duration::from_millis(500);
    let changed = Arc::new(Snapshot(Arc::new(changed)));
    let snapshots = ListenerSnapshots::default();
    let mut socket = Some(socket);
    for (generation, initial, delay) in [
        (0, original, Duration::from_millis(50)),
        (1, changed.clone(), Duration::from_millis(500)),
    ] {
        let active = snapshots
            .start(BTreeMap::from([(initial.0.id.clone(), initial.clone())]))
            .unwrap();
        let source = match socket.take() {
            Some(socket) => socket,
            None => TcpListener::bind(address).await.unwrap(),
        };
        let (source, control) = fault::Source::new(source);
        control.inject(std::io::ErrorKind::OutOfMemory);
        control.inject(std::io::ErrorKind::OutOfMemory);
        let context = RunContext::new(Default::default());
        let mut serving = Box::pin(serve_group(
            &snapshots,
            initial,
            vec![source],
            &context,
            |_, _, _, _| async {
                panic!("no real peer in injected timer test")
            },
        ));
        assert!((&mut serving).now_or_never().is_none());
        assert_eq!(control.polls(), 1);
        if generation == 0 {
            snapshots
                .publish(BTreeMap::from([(
                    changed.0.id.clone(),
                    changed.clone(),
                )]))
                .unwrap();
        }
        tokio::time::advance(delay - Duration::from_millis(1)).await;
        assert!((&mut serving).now_or_never().is_none());
        assert_eq!(control.polls(), 1);
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!((&mut serving).now_or_never().is_none());
        assert_eq!(
            control.polls(),
            2,
            "generation captures its initial retry policy"
        );
        context.cancellation_token().cancel();
        serving.await.unwrap();
        drop(active);
    }
}

#[tokio::test]
async fn persistent_transient_errors_yield_to_sibling_socket_and_cancellation()
{
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let broken = socket().await;
    let healthy = socket().await;
    let addresses =
        [broken.local_addr().unwrap(), healthy.local_addr().unwrap()];
    let (broken, control) = fault::Source::new(broken);
    let (healthy, _) = fault::Source::new(healthy);
    control.transient(true);
    let initial = group("transient", &addresses, "null");
    let snapshots = ListenerSnapshots::default();
    let context = RunContext::new(Default::default());
    let cancel = context.cancellation_token();
    let task = tokio::spawn(async move {
        let _generation = snapshots
            .start(BTreeMap::from([(initial.0.id.clone(), initial.clone())]))
            .unwrap();
        serve_bound(
            &snapshots,
            vec![(initial, vec![broken, healthy])],
            &context,
            |_, mut stream, _, scope| async move {
                let _scope = scope;
                let mut byte = [0];
                stream.read_exact(&mut byte).await.unwrap();
                stream.write_all(&byte).await.unwrap();
            },
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), control.wait_errors(1))
        .await
        .unwrap();
    let mut client = tokio::time::timeout(
        Duration::from_secs(5),
        TcpStream::connect(addresses[1]),
    )
    .await
    .unwrap()
    .unwrap();
    client.write_all(b"x").await.unwrap();
    let mut byte = [0];
    tokio::time::timeout(Duration::from_secs(5), client.read_exact(&mut byte))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&byte, b"x");
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    for address in addresses {
        assert!(TcpListener::bind(address).await.is_ok());
    }
}
