//! Observe the real builder's deadline without copying Hyper's default value.

use std::convert::Infallible;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http::Response;
use http_body_util::Empty;
use hyper::rt::{Sleep, Timer};
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::io::AsyncWriteExt;

use super::*;

#[derive(Clone)]
struct RecordingTimer {
    now: Instant,
    sleeps: Arc<Mutex<Vec<Duration>>>,
}

struct PendingSleep;

impl Future for PendingSleep {
    type Output = ();
    fn poll(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<()> {
        Poll::Pending
    }
}

impl Sleep for PendingSleep {}

impl Timer for RecordingTimer {
    fn now(&self) -> Instant {
        self.now
    }
    fn sleep(
        &self,
        duration: Duration,
    ) -> Pin<Box<dyn Sleep>> {
        self.sleeps.lock().unwrap().push(duration);
        Box::pin(PendingSleep)
    }
    fn sleep_until(
        &self,
        deadline: Instant,
    ) -> Pin<Box<dyn Sleep>> {
        self.sleep(deadline.duration_since(self.now))
    }
}

async fn deadlines(mut builder: http1::Builder) -> Vec<Duration> {
    let sleeps = Arc::new(Mutex::new(Vec::new()));
    builder
        .timer(RecordingTimer { now: Instant::now(), sleeps: sleeps.clone() });
    let (mut peer, server) = tokio::io::duplex(1024);
    peer.write_all(b"GET / HTTP/1.1\r\nHost:").await.unwrap();
    let service = service_fn(|_| async {
        Ok::<_, Infallible>(Response::new(Empty::<Bytes>::new()))
    });
    let connection = builder.serve_connection(TokioIo::new(server), service);
    tokio::pin!(connection);
    poll_fn(|cx| {
        assert!(connection.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    sleeps.lock().unwrap().clone()
}

#[tokio::test]
async fn omitted_timeout_preserves_the_native_driver_default() {
    let native = deadlines(http1::Builder::new()).await;
    assert_eq!(native.len(), 1);
    assert!(
        !native[0].is_zero(),
        "native default must remain a finite deadline"
    );
    assert_eq!(deadlines(h1(&H1Config::default())).await, native);
    let yaml: H1Config = serde_yaml::from_str("{}").unwrap();
    assert_eq!(deadlines(h1(&yaml)).await, native);
}

#[tokio::test]
async fn explicit_null_disables_and_duration_replaces_native_timeout() {
    let disabled: H1Config =
        serde_yaml::from_str("header_read_timeout: null").unwrap();
    assert!(deadlines(h1(&disabled)).await.is_empty());
    let explicit: H1Config =
        serde_yaml::from_str("header_read_timeout: 125ms").unwrap();
    assert_eq!(deadlines(h1(&explicit)).await, [Duration::from_millis(125)]);
}

#[tokio::test(start_paused = true)]
async fn native_default_with_installed_tokio_timer_expires_partial_headers() {
    let native = deadlines(http1::Builder::new()).await;
    let (mut peer, server) = tokio::io::duplex(1024);
    peer.write_all(b"GET / HTTP/1.1\r\nHost:").await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let entered = calls.clone();
    let service = service_fn(move |_| {
        entered.fetch_add(1, Ordering::SeqCst);
        async { Ok::<_, Infallible>(Response::new(Empty::<Bytes>::new())) }
    });
    // Keep the production builder's actual TokioTimer. The earlier recording
    // call discovers upstream policy without replacing this connection's timer.
    let connection = h1(&H1Config::default())
        .serve_connection(TokioIo::new(server), service);
    tokio::pin!(connection);
    poll_fn(|cx| {
        assert!(connection.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    tokio::time::advance(native[0]).await;
    let error = connection.await.unwrap_err();
    assert!(error.is_timeout(), "{error}");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
