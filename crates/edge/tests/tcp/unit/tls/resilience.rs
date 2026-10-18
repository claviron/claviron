//! Controlled streams exercise the production H1/H2 drivers and their drain.

use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::transport::BypassHandler;
use bytes::Bytes;
use claviron_http::{RouteBody, RouteFuture, boxed_body, bytes_body};
use claviron_runtime::gate::GateDrainOutcome;
use futures_util::stream;
use http::{Request, Response, StatusCode};
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Empty, StreamBody};
use hyper::body::Frame;
use hyper::client::conn::http2::SendRequest;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinSet;

use super::framing_cases::read_closed;
use super::support::{TEST_TIMEOUT, TlsFixture};

type ClientBody = UnsyncBoxBody<Bytes, Infallible>;

#[derive(Default)]
struct Active {
    count: AtomicUsize,
    changed: Notify,
}

impl Active {
    async fn empty(&self) {
        loop {
            let changed = self.changed.notified();
            if self.count.load(Ordering::SeqCst) == 0 {
                return;
            }
            changed.await;
        }
    }
}

struct Work(Arc<Active>);

impl Work {
    fn new(active: &Arc<Active>) -> Self {
        active.count.fetch_add(1, Ordering::SeqCst);
        Self(active.clone())
    }
}

impl Drop for Work {
    fn drop(&mut self) {
        self.0.count.fetch_sub(1, Ordering::SeqCst);
        self.0.changed.notify_one();
    }
}

struct ControlledApp {
    entered: mpsc::Sender<String>,
    active: Arc<Active>,
    download: Mutex<Option<mpsc::Receiver<Bytes>>>,
}

impl BypassHandler for ControlledApp {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> RouteFuture {
        let work = Work::new(&self.active);
        self.entered.try_send(request.uri().path().to_owned()).unwrap();
        if request.uri().path() == "/download" {
            let receiver = self.download.lock().unwrap().take().unwrap();
            let body = StreamBody::new(stream::unfold(
                (receiver, work),
                |(mut receiver, work)| async move {
                    receiver.recv().await.map(|bytes| {
                        (
                            Ok::<_, Infallible>(Frame::data(bytes)),
                            (receiver, work),
                        )
                    })
                },
            ));
            return Box::pin(
                async move { Ok(Response::new(boxed_body(body))) },
            );
        }
        Box::pin(async move {
            let _work = work;
            let mut body = request.into_body();
            let mut bytes = 0;
            while let Some(frame) = body.frame().await {
                let Ok(frame) = frame else {
                    break;
                }; // Peer RST cancels upload.
                if let Ok(data) = frame.into_data() {
                    bytes += data.len();
                    assert!(bytes <= 1024, "bounded test upload");
                }
            }
            Ok(Response::new(bytes_body(Bytes::from_static(b"healthy"))))
        })
    }
}

struct Control {
    app: Arc<ControlledApp>,
    entered: mpsc::Receiver<String>,
    download: mpsc::Sender<Bytes>,
}

impl Control {
    fn new() -> Self {
        let (entered, receiver) = mpsc::channel(8);
        let (download, body) = mpsc::channel(1);
        Self {
            app: Arc::new(ControlledApp {
                entered,
                active: Arc::new(Active::default()),
                download: Mutex::new(Some(body)),
            }),
            entered: receiver,
            download,
        }
    }
}

fn request(
    path: &str,
    body: ClientBody,
) -> Request<ClientBody> {
    Request::builder()
        .uri(format!("https://app.test{path}"))
        .body(body)
        .unwrap()
}

fn upload() -> (mpsc::Sender<Bytes>, ClientBody) {
    let (sender, receiver) = mpsc::channel(1);
    let body =
        StreamBody::new(stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|bytes| {
                (Ok::<_, Infallible>(Frame::data(bytes)), receiver)
            })
        }))
        .boxed_unsync();
    (sender, body)
}

async fn h2(
    fixture: &mut TlsFixture
) -> (SendRequest<ClientBody>, JoinSet<Result<(), hyper::Error>>) {
    let peer = fixture.connect("app.test", b"h2").await.unwrap();
    let (sender, connection) = hyper::client::conn::http2::handshake::<
        _,
        _,
        ClientBody,
    >(TokioExecutor::new(), TokioIo::new(peer))
    .await
    .unwrap();
    let mut drivers = JoinSet::new();
    drivers.spawn(connection);
    (sender, drivers)
}

async fn healthy(sender: &mut SendRequest<ClientBody>) {
    let response = sender
        .send_request(request("/healthy", Empty::<Bytes>::new().boxed_unsync()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "healthy"
    );
}

async fn join(mut drivers: JoinSet<Result<(), hyper::Error>>) {
    while let Some(result) = drivers.join_next().await {
        result.unwrap().unwrap();
    }
}

async fn join_after_observed_goaway(
    mut drivers: JoinSet<Result<(), h2::Error>>
) {
    while let Some(result) = drivers.join_next().await {
        let result = result.unwrap();
        if let Err(error) = result {
            assert_eq!(
                error.get_io().map(std::io::Error::kind),
                Some(std::io::ErrorKind::BrokenPipe),
                "unexpected H2 client driver failure after GOAWAY: {error}"
            );
        }
    }
}

#[tokio::test]
async fn controlled_h1_upload_completes_during_indefinite_grace() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut control = Control::new();
        let mut fixture = TlsFixture::with_handler(control.app.clone()).await;
        let mut peer = fixture.connect("app.test", b"http/1.1").await.unwrap();
        peer.write_all(b"POST /upload HTTP/1.1\r\nHost: app.test\r\nContent-Length: 6\r\n\r\none")
            .await
            .unwrap();
        assert_eq!(control.entered.recv().await.unwrap(), "/upload");
        fixture.gate.graceful_shutdown(None);
        let mut byte = [0];
        assert!(
            tokio::time::timeout(Duration::from_millis(20), peer.read(&mut byte)).await.is_err()
        );
        assert_eq!(fixture.gate.count(), 1);
        peer.write_all(b"two").await.unwrap();
        assert!(read_closed(&mut peer).await.ends_with("healthy"));
        drop(peer);
        fixture.finish().await;
        control.app.active.empty().await;
    })
    .await
    .expect("grace must retain and complete an already admitted upload");
}

#[tokio::test]
async fn slow_h2_upload_allows_other_streams_and_goaway_drains_it() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut control = Control::new();
        let mut fixture = TlsFixture::with_handler(control.app.clone()).await;
        // Observe protocol-level GOAWAY, not Hyper's dispatch-channel readiness:
        // that channel can remain open while accepted streams are still alive.
        let peer = fixture.connect("app.test", b"h2").await.unwrap();
        let (mut sender, connection) =
            h2::client::handshake(peer).await.unwrap();
        let mut drivers = JoinSet::new();
        drivers.spawn(connection);
        let (response, mut upload) = sender
            .send_request(
                Request::builder()
                    .uri("https://app.test/upload")
                    .body(())
                    .unwrap(),
                false,
            )
            .unwrap();
        assert_eq!(control.entered.recv().await.unwrap(), "/upload");
        let (healthy, _) = sender
            .send_request(
                Request::builder()
                    .uri("https://app.test/healthy")
                    .body(())
                    .unwrap(),
                true,
            )
            .unwrap();
        let mut healthy = healthy.await.unwrap().into_body();
        assert_eq!(healthy.data().await.unwrap().unwrap(), "healthy");
        assert!(healthy.data().await.is_none());
        drop(healthy);
        assert_eq!(control.entered.recv().await.unwrap(), "/healthy");
        fixture.gate.graceful_shutdown(None);
        // The client observes GOAWAY while an accepted stream remains open.
        // Hyper's graceful handshake includes a ping before its final GOAWAY.
        let rejected = loop {
            match futures_util::future::poll_fn(|cx| sender.poll_ready(cx))
                .await
            {
                Ok(()) => tokio::task::yield_now().await,
                Err(error) => break error,
            }
        };
        assert!(rejected.is_go_away());
        assert_eq!(rejected.reason(), Some(h2::Reason::NO_ERROR));
        assert!(
            sender
                .send_request(
                    Request::builder()
                        .uri("https://app.test/late")
                        .body(())
                        .unwrap(),
                    true
                )
                .is_err()
        );
        assert!(control.entered.try_recv().is_err());
        assert_eq!(fixture.gate.count(), 1);
        assert_eq!(control.app.active.count.load(Ordering::SeqCst), 1);
        upload.send_data(Bytes::from_static(b"done"), true).unwrap();
        drop(upload);
        let response = response.await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let mut response = response.into_body();
        assert_eq!(response.data().await.unwrap().unwrap(), "healthy");
        assert!(response.data().await.is_none());
        drop(response);
        drop(sender);
        fixture.finish().await;
        // Linux may report BrokenPipe when the client driver's final write
        // races the peer's TLS close. The protocol outcome has already been
        // proved above: NO_ERROR GOAWAY, rejected late work, and a complete
        // response for the accepted upload. No other driver error is valid.
        join_after_observed_goaway(drivers).await;
        control.app.active.empty().await;
    })
    .await
    .expect("GOAWAY must refuse new work while accepted work completes");
}

#[tokio::test]
async fn controlled_h2_response_does_not_block_other_streams_or_grace() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut control = Control::new();
        let mut fixture = TlsFixture::with_handler(control.app.clone()).await;
        let (mut sender, drivers) = h2(&mut fixture).await;
        let response = sender
            .send_request(request(
                "/download",
                Empty::<Bytes>::new().boxed_unsync(),
            ))
            .await
            .unwrap();
        assert_eq!(control.entered.recv().await.unwrap(), "/download");
        let mut body = response.into_body();
        control.download.send(Bytes::from_static(b"first")).await.unwrap();
        assert_eq!(
            body.frame().await.unwrap().unwrap().into_data().unwrap(),
            "first"
        );
        healthy(&mut sender).await;
        fixture.gate.graceful_shutdown(None);
        assert_eq!(fixture.gate.count(), 1);
        assert_eq!(control.app.active.count.load(Ordering::SeqCst), 1);
        control.download.send(Bytes::from_static(b"last")).await.unwrap();
        drop(control.download);
        assert_eq!(body.collect().await.unwrap().to_bytes(), "last");
        drop(sender);
        fixture.finish().await;
        join(drivers).await;
        control.app.active.empty().await;
    })
    .await
    .expect("gated response streaming and other streams must coexist");
}

#[tokio::test]
async fn stalled_h2_reader_preserves_peer_streams_and_resumes_during_grace() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut control = Control::new();
        let mut fixture = TlsFixture::with_handler(control.app.clone()).await;
        let peer = fixture.connect("app.test", b"h2").await.unwrap();
        let (mut sender, connection) = h2::client::Builder::new()
            .initial_window_size(1024)
            .handshake::<_, Bytes>(peer)
            .await
            .unwrap();
        let mut drivers = JoinSet::new();
        drivers.spawn(connection);
        let (response, _) = sender
            .send_request(
                Request::builder().uri("https://app.test/download").body(()).unwrap(),
                true
            )
            .unwrap();
        let mut response = response.await.unwrap().into_body();
        assert_eq!(control.entered.recv().await.unwrap(), "/download");
        let chunk = Bytes::from_static(&[b'x'; 1024]);
        control.download.send(chunk.clone()).await.unwrap();
        assert_eq!(response.data().await.unwrap().unwrap(), chunk);
        // Consume bytes without releasing receive capacity: the server cannot
        // send the second chunk until this client emits WINDOW_UPDATE.
        control.download.send(chunk.clone()).await.unwrap();
        drop(control.download);
        assert!(tokio::time::timeout(Duration::from_millis(20), response.data()).await.is_err());
        let (healthy, _) = sender
            .send_request(
                Request::builder().uri("https://app.test/healthy").body(()).unwrap(),
                true
            )
            .unwrap();
        let mut healthy = healthy.await.unwrap().into_body();
        assert_eq!(healthy.data().await.unwrap().unwrap(), "healthy");
        assert!(healthy.data().await.is_none());
        drop(healthy);
        assert_eq!(control.entered.recv().await.unwrap(), "/healthy");
        fixture.gate.graceful_shutdown(None);
        assert!(tokio::time::timeout(Duration::from_millis(20), response.data()).await.is_err());
        assert_eq!(fixture.gate.count(), 1);
        response.flow_control().release_capacity(1024).unwrap();
        assert_eq!(response.data().await.unwrap().unwrap(), chunk);
        // A streaming body's EOF can be encoded as an empty END_STREAM DATA.
        while let Some(data) = response.data().await {
            assert!(data.unwrap().is_empty());
        }
        drop(response);
        drop(sender);
        fixture.finish().await;
        while let Some(result) = drivers.join_next().await {
            result.unwrap().unwrap();
        }
        control.app.active.empty().await;
    })
    .await
    .expect("a stalled reader must neither block peer streams nor lose admitted data during grace");
}

#[tokio::test]
async fn h2_reset_releases_stream_and_forced_drain_releases_connection() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut control = Control::new();
        let mut fixture = TlsFixture::with_handler(control.app.clone()).await;
        let (mut sender, drivers) = h2(&mut fixture).await;
        let (upload_sender, body) = upload();
        let response = sender.send_request(request("/upload", body));
        assert_eq!(control.entered.recv().await.unwrap(), "/upload");
        drop(response); // Hyper sends RST_STREAM for this cancelled request.
        tokio::time::timeout(
            Duration::from_secs(1),
            control.app.active.empty(),
        )
        .await
        .expect("cancelled upload must drop its handler");
        drop(upload_sender);
        healthy(&mut sender).await;
        assert_eq!(control.entered.recv().await.unwrap(), "/healthy");
        assert_eq!(fixture.gate.count(), 1);
        let (_held_sender, body) = upload();
        let stalled = sender.send_request(request("/upload", body));
        assert_eq!(control.entered.recv().await.unwrap(), "/upload");
        fixture.gate.graceful_shutdown(Some(Duration::from_millis(25)));
        // Like the listener owner, drive the gate's drain deadline: notifying
        // grace alone intentionally does not start a detached force timer.
        let gate = fixture.gate.clone();
        let (outcome, ()) =
            tokio::join!(gate.wait_all_done(), fixture.reap_connection());
        assert_eq!(outcome, GateDrainOutcome::Forced { remaining: 1 });
        assert!(stalled.await.is_err());
        control.app.active.empty().await;
        drop(sender);
        fixture.finish().await;
        // Force closes a transport with open streams; either normal client
        // completion or a transport error is valid, but no driver may detach.
        let mut drivers = drivers;
        while let Some(result) = drivers.join_next().await {
            let _ = result.unwrap();
        }
    })
    .await
    .expect("reset and forced drain must release tracked work and permits");
}

#[tokio::test]
async fn configured_h2_stream_limit_delays_extra_work_without_losing_it() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut control = Control::new();
        let mut fixture = TlsFixture::with_handler(control.app.clone()).await;
        fixture.h2_max_streams(1);
        let (mut sender, drivers) = h2(&mut fixture).await;
        healthy(&mut sender).await; // Observe the server's initial SETTINGS.
        assert_eq!(control.entered.recv().await.unwrap(), "/healthy");
        let (upload, body) = upload();
        let first = sender.send_request(request("/upload", body));
        assert_eq!(control.entered.recv().await.unwrap(), "/upload");
        let second = sender.send_request(request(
            "/queued",
            Empty::<Bytes>::new().boxed_unsync(),
        ));
        assert!(
            tokio::time::timeout(
                Duration::from_millis(20),
                control.entered.recv()
            )
            .await
            .is_err()
        );
        assert_eq!(control.app.active.count.load(Ordering::SeqCst), 1);
        drop(upload);
        first.await.unwrap().into_body().collect().await.unwrap();
        assert_eq!(second.await.unwrap().status(), StatusCode::OK);
        assert_eq!(control.entered.recv().await.unwrap(), "/queued");
        drop(sender);
        fixture.finish().await;
        join(drivers).await;
        control.app.active.empty().await;
    })
    .await
    .expect("configured stream capacity must bound concurrent handler entry");
}
