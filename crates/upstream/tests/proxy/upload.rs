//! A large request body crosses an h1 upstream whole and as it arrives.
use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use bytes::Bytes;
use http::{Request, StatusCode};
use http_body::{Body, Frame};

const PIECE: usize = 64 * 1024;
/// 32 MiB: several times what loopback socket buffers hold, so an origin
/// that sees the first byte before the last piece was offered proves the
/// body was forwarded while it was still arriving.
const PIECES: usize = 512;

fn byte_at(position: usize) -> u8 {
    (position % 251) as u8
}

/// The client's body: pieces handed over one at a time.
struct Pieces(tokio::sync::mpsc::Receiver<Bytes>);

impl Body for Pieces {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        self.0
            .poll_recv(cx)
            .map(|piece| piece.map(|bytes| Ok(Frame::data(bytes))))
    }
}

#[derive(Default)]
struct Seen {
    bytes: AtomicUsize,
    wrong: AtomicUsize,
    /// Pieces the client had offered when the origin got its first byte.
    offered_at_first: AtomicUsize,
}

/// An origin that checks every byte against its position as it arrives.
async fn origin(
    seen: Arc<Seen>,
    offered: Arc<AtomicUsize>,
) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let (seen, offered) = (Arc::clone(&seen), Arc::clone(&offered));
            let service = hyper::service::service_fn(
                move |request: Request<hyper::body::Incoming>| {
                    let (seen, offered) =
                        (Arc::clone(&seen), Arc::clone(&offered));
                    async move {
                        let mut body = request.into_body();
                        while let Some(frame) = std::future::poll_fn(|cx| {
                            Pin::new(&mut body).poll_frame(cx)
                        })
                        .await
                        {
                            let Ok(data) = frame.unwrap().into_data() else {
                                continue;
                            };
                            let start = seen.bytes.load(Ordering::SeqCst);
                            if start == 0 {
                                seen.offered_at_first.store(
                                    offered.load(Ordering::SeqCst),
                                    Ordering::SeqCst,
                                );
                            }
                            let wrong = data
                                .iter()
                                .enumerate()
                                .filter(|(at, byte)| {
                                    **byte != byte_at(start + at)
                                })
                                .count();
                            seen.wrong.fetch_add(wrong, Ordering::SeqCst);
                            seen.bytes.fetch_add(data.len(), Ordering::SeqCst);
                        }
                        Ok::<_, Infallible>(http::Response::new(
                            http_body_util::Empty::<Bytes>::new(),
                        ))
                    }
                },
            );
            tokio::spawn(async move {
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(
                        hyper_util::rt::TokioIo::new(stream),
                        service,
                    )
                    .await;
            });
        }
    });
    address
}

fn service(
    address: std::net::SocketAddr
) -> crate::proxy::service::UpstreamService {
    let yaml = format!(
        "upstreams:\n  example:\n    protocol: h1\n    origins: [tcp://{address}]\n    connection: {{connect_timeout: off}}\n    capacity: {{max_live: 1}}\n"
    );
    let config: crate::config::UpstreamYaml =
        serde_yaml::from_str(&yaml).unwrap();
    let profile = config
        .compile_profile_with_origins(
            "example",
            &config.upstreams["example"],
            &Default::default(),
            None,
            &Default::default(),
        )
        .unwrap();
    crate::proxy::service::UpstreamService::new(Arc::new(profile))
}

async fn upload(declared: bool) {
    let seen = Arc::new(Seen::default());
    let offered = Arc::new(AtomicUsize::new(0));
    let address = origin(Arc::clone(&seen), Arc::clone(&offered)).await;
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    let producer = {
        let offered = Arc::clone(&offered);
        tokio::spawn(async move {
            for index in 0..PIECES {
                let piece: Vec<u8> =
                    (0..PIECE).map(|at| byte_at(index * PIECE + at)).collect();
                if sender.send(Bytes::from(piece)).await.is_err() {
                    return;
                }
                offered.fetch_add(1, Ordering::SeqCst);
            }
        })
    };
    let mut request = Request::post("http://upload.example/upload");
    if declared {
        request = request.header(http::header::CONTENT_LENGTH, PIECE * PIECES);
    }
    let request =
        request.body(claviron_http::boxed_body(Pieces(receiver))).unwrap();

    let response = service(address).handle(request).await;
    assert_eq!(response.status(), StatusCode::OK);
    producer.await.unwrap();
    assert_eq!(seen.bytes.load(Ordering::SeqCst), PIECE * PIECES);
    assert_eq!(seen.wrong.load(Ordering::SeqCst), 0, "every byte in its place");
    let at_first = seen.offered_at_first.load(Ordering::SeqCst);
    assert!(
        at_first < PIECES,
        "the origin got its first byte only after all {PIECES} pieces were offered: the body was held"
    );
}

#[tokio::test]
async fn a_declared_upload_crosses_whole_while_it_is_still_arriving() {
    upload(true).await;
}

#[tokio::test]
async fn a_chunked_upload_crosses_whole_while_it_is_still_arriving() {
    upload(false).await;
}

/// A client body that delivers some pieces, then fails as the listener's
/// body layer fails it.
struct FailsAfter {
    left: usize,
    error: Option<claviron_http::body::BodyLimit>,
}

impl Body for FailsAfter {
    type Data = Bytes;
    type Error = claviron_http::body::BodyLimit;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        if self.left == 0 {
            return Poll::Ready(self.error.take().map(Err));
        }
        self.left -= 1;
        Poll::Ready(Some(Ok(Frame::data(Bytes::from(vec![0u8; PIECE])))))
    }
}

async fn refused(error: claviron_http::body::BodyLimit) -> StatusCode {
    let seen = Arc::new(Seen::default());
    let address = origin(seen, Arc::new(AtomicUsize::new(0))).await;
    let request = Request::post("http://upload.example/upload")
        .body(claviron_http::boxed_body(FailsAfter {
            left: 4,
            error: Some(error),
        }))
        .unwrap();
    service(address).handle(request).await.status()
}

#[tokio::test]
async fn a_body_past_the_limit_mid_upload_is_the_clients_413_not_a_502() {
    let status =
        refused(claviron_http::body::BodyLimit::TooLarge { limit: PIECE })
            .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn a_body_that_stalls_mid_upload_is_the_clients_408_not_a_502() {
    let status = refused(claviron_http::body::BodyLimit::Stalled {
        after: std::time::Duration::ZERO,
    })
    .await;
    assert_eq!(status, StatusCode::REQUEST_TIMEOUT);
}
