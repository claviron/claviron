use futures_util::{FutureExt, StreamExt};

use super::*;
use crate::listener::accept_support as fault;

#[test]
fn recovery_caps_growth_and_warning_transitions_and_resets_on_success() {
    let policy = AcceptRetryConfig {
        initial: Duration::from_millis(50),
        max: Duration::from_secs(1),
    };
    let mut recovery = Recovery::default();
    let now = Instant::now();
    let mut warnings = 0;
    for attempt in 0..10_000 {
        warnings += usize::from(recovery.failed(&policy, now));
        assert!(
            recovery.delay >= policy.initial && recovery.delay <= policy.max
        );
        if attempt > 5 {
            assert_eq!(recovery.delay, policy.max);
        }
    }
    assert_eq!(warnings, 1, "no warning or retained history per retry");
    assert_eq!(recovery.recovered().unwrap().0, 10_000);
    assert!(recovery.recovered().is_none(), "one summary per episode");
    assert!(recovery.failed(&policy, now));
    assert_eq!(recovery.delay, policy.initial);

    recovery.failures = u64::MAX;
    recovery.delay = Duration::MAX;
    assert!(!recovery.failed(&policy, now));
    assert_eq!(recovery.failures, u64::MAX);
    assert_eq!(recovery.delay, policy.max);
}

#[tokio::test(start_paused = true)]
async fn resource_retry_uses_one_timer_and_never_accepts_before_its_deadline() {
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let client = TcpStream::connect(address).await.unwrap();
    let (source, control) = fault::Source::new(socket);
    for _ in 0..4 {
        control.inject(io::ErrorKind::OutOfMemory);
    }
    let policy = AcceptRetryConfig {
        initial: Duration::from_millis(50),
        max: Duration::from_millis(200),
    };
    let mut accepting =
        SocketAccept::new(source, "fault".parse().unwrap(), policy).unwrap();
    let timer = accepting.timer.as_ref().get_ref() as *const Sleep;
    assert!(accepting.next().now_or_never().is_none());
    assert_eq!(control.polls(), 1);
    for (index, delay) in [50_u64, 100, 200, 200].into_iter().enumerate() {
        tokio::time::advance(Duration::from_millis(delay - 1)).await;
        assert!(accepting.next().now_or_never().is_none());
        assert_eq!(control.polls(), index + 1);
        tokio::time::advance(Duration::from_millis(1)).await;
        let next = accepting.next().now_or_never();
        if index < 3 {
            assert!(next.is_none());
        } else {
            let (stream, _) = next.unwrap().unwrap().unwrap();
            drop(stream);
            assert_eq!(accepting.recovery.failures, 0);
        }
        assert_eq!(control.polls(), index + 2);
        assert_eq!(accepting.timer.as_ref().get_ref() as *const Sleep, timer);
    }
    control.wait_errors(4).await;
    control.inject(io::ErrorKind::OutOfMemory);
    assert!(accepting.next().now_or_never().is_none());
    assert_eq!(accepting.recovery.delay, Duration::from_millis(50));
    assert_eq!(accepting.recovery.failures, 1);
    drop(accepting);
    drop(client);
    assert!(
        TcpListener::bind(address).await.is_ok(),
        "cooldown owns no detached timer task"
    );
}

#[tokio::test]
async fn peer_failure_yields_without_backoff_and_local_failure_ends_the_socket_stream()
 {
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let _client = TcpStream::connect(address).await.unwrap();
    let (source, control) = fault::Source::new(socket);
    control.transient(true);
    let mut accepting =
        SocketAccept::new(source, "peer".parse().unwrap(), Default::default())
            .unwrap();
    for attempt in 1..=64 {
        assert!(accepting.next().now_or_never().is_none());
        assert_eq!(control.polls(), attempt, "one transient syscall per poll");
    }
    assert!(!accepting.waiting);
    assert_eq!(accepting.recovery.failures, 0);
    control.transient(false);
    let accepted = accepting.next().await.unwrap().unwrap();
    drop(accepted);
    // A deliberately unknown errno is fatal without depending on libc values
    // in the HTTP driver. Its exact identity must survive the context wrapper.
    control.inject_error(io::Error::from_raw_os_error(-1234));
    let failure = accepting.next().await.unwrap().unwrap_err();
    assert_eq!(failure.source.raw_os_error(), Some(-1234));
    assert!(std::error::Error::source(&failure).unwrap().is::<io::Error>());
    assert!(accepting.next().await.is_none());
}
