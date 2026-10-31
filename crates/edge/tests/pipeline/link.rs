use http::{HeaderMap, HeaderValue, Uri};

use super::exchange_headers;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker};

use claviron_orbit::transport::{Message, read_message, write_message};
use fastwebsockets::{Frame, OpCode, Payload, Role, WebSocket};
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};
use tokio_util::sync::CancellationToken;

struct UnreadBody(Arc<std::sync::atomic::AtomicUsize>);

impl http_body::Body for UnreadBody {
    type Data = bytes::Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        panic!("native Link selection must not poll an unclaimed body");
    }
}

impl Drop for UnreadBody {
    fn drop(&mut self) { self.0.fetch_add(1, Ordering::Relaxed); }
}

#[test]
fn native_link_miss_keeps_the_body_and_a_matched_unavailable_route_owns_503() {
    use super::{DispatchStep, LinkBackend, LinkRoute, RequestPolicyOverride, ServiceResult};
    let backend = LinkBackend { routes: vec![LinkRoute {
        path: Some(serde_yaml::from_str("/api").unwrap()),
        target: None,
        requests: RequestPolicyOverride::default(),
        shutdown: CancellationToken::new(),
    }].into() };
    for (path, handled) in [("/apiary", false), ("/elsewhere", false), ("/api/item", true)] {
        let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let request = http::Request::builder().uri(path)
            .body(claviron_http::boxed_body(UnreadBody(drops.clone()))).unwrap();
        let mut call = std::pin::pin!(backend.call(request));
        match poll_once(call.as_mut()) {
            Poll::Ready(ServiceResult::Handled(response)) => {
                assert!(handled);
                assert_eq!(response.status(), http::StatusCode::SERVICE_UNAVAILABLE);
                assert_eq!(response.headers()["retry-after"], "1");
                assert_eq!(drops.load(Ordering::Relaxed), 1);
            }
            Poll::Ready(ServiceResult::Unhandled(request)) => {
                assert!(!handled);
                assert_eq!(request.uri().path(), path);
                assert_eq!(drops.load(Ordering::Relaxed), 0);
                drop(request);
            }
            Poll::Pending => panic!("mount selection is synchronous"),
        }
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }
}

#[test]
fn orbit_websocket_h2_connect_uses_200_without_h1_headers() {
    use super::websocket::Handshake;
    let mut request = http::Request::builder()
        .method(http::Method::CONNECT)
        .version(http::Version::HTTP_2)
        .uri("https://app.test/app/key")
        .header("sec-websocket-version", "13")
        .body(())
        .unwrap();
    request
        .extensions_mut()
        .insert(hyper::ext::Protocol::from_static("websocket"));
    let handshake = Handshake::from_request(&request).unwrap().unwrap();
    let response = handshake
        .response(&[
            (b"connection".to_vec(), b"Upgrade".to_vec()),
            (b"upgrade".to_vec(), b"websocket".to_vec()),
            (b"sec-websocket-accept".to_vec(), b"untrusted".to_vec()),
            (b"content-length".to_vec(), b"0".to_vec()),
            (b"sec-websocket-protocol".to_vec(), b"chat".to_vec()),
            (b"set-cookie".to_vec(), b"session=example".to_vec()),
        ])
        .body(())
        .unwrap();
    assert_eq!(response.status(), http::StatusCode::OK);
    for header in
        ["connection", "upgrade", "sec-websocket-accept", "content-length"]
    {
        assert!(!response.headers().contains_key(header));
    }
    assert_eq!(response.headers()["sec-websocket-protocol"], "chat");
    assert_eq!(response.headers()["set-cookie"], "session=example");
    request.headers_mut().remove("sec-websocket-version");
    assert_eq!(
        Handshake::from_request(&request).unwrap_err(),
        http::StatusCode::BAD_REQUEST
    );
}

#[test]
fn orbit_websocket_h1_handshake_keeps_101_and_edge_owned_accept() {
    use super::websocket::Handshake;
    let request = http::Request::builder()
        .uri("/app/key")
        .header("connection", "Upgrade")
        .header("upgrade", "websocket")
        .header("sec-websocket-version", "13")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .body(())
        .unwrap();
    let response = Handshake::from_request(&request)
        .unwrap()
        .unwrap()
        .response(&[])
        .body(())
        .unwrap();
    assert_eq!(response.status(), http::StatusCode::SWITCHING_PROTOCOLS);
    assert_eq!(
        response.headers()["sec-websocket-accept"],
        "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
    );
}

#[tokio::test]
async fn orbit_websocket_h2_wire_connect_carries_the_shared_message_stream() {
    use super::websocket::Handshake;
    use bytes::Bytes;
    use http_body_util::Empty;
    use hyper_util::rt::{TokioExecutor, TokioIo};

    let (server, client) = tokio::io::duplex(4096);
    let (accepted, receiving) = tokio::sync::oneshot::channel();
    let accepted = Arc::new(std::sync::Mutex::new(Some(accepted)));
    let service = hyper::service::service_fn(move |mut request| {
        let accepted = accepted.clone();
        async move {
            let handshake = Handshake::from_request(&request).unwrap().unwrap();
            let upgraded = hyper::upgrade::on(&mut request);
            assert!(
                accepted.lock().unwrap().take().unwrap().send(upgraded).is_ok()
            );
            Ok::<_, std::convert::Infallible>(
                handshake.response(&[]).body(Empty::<Bytes>::new()).unwrap(),
            )
        }
    });
    let mut builder =
        hyper::server::conn::http2::Builder::new(TokioExecutor::new());
    builder.enable_connect_protocol();
    let server =
        tokio::spawn(builder.serve_connection(TokioIo::new(server), service));
    let (mut sender, connection) =
        hyper::client::conn::http2::handshake::<_, _, Empty<Bytes>>(
            TokioExecutor::new(),
            TokioIo::new(client),
        )
        .await
        .unwrap();
    let connection = tokio::spawn(connection);
    sender.ready().await.unwrap();
    let mut request = http::Request::builder()
        .method(http::Method::CONNECT)
        .uri("https://app.test/app/key")
        .header("sec-websocket-version", "13")
        .body(Empty::<Bytes>::new())
        .unwrap();
    request
        .extensions_mut()
        .insert(hyper::ext::Protocol::from_static("websocket"));
    let response = sender.send_request(request).await.unwrap();
    assert_eq!(response.status(), http::StatusCode::OK);
    let client = hyper::upgrade::on(response).await.unwrap();
    let server_io = receiving.await.unwrap().await.unwrap();
    let (to_application, mut application_input) = tokio::io::duplex(128);
    let (from_application, mut application_output) = tokio::io::duplex(128);
    let cancellation = CancellationToken::new();
    let stop = cancellation.clone();
    let pump = tokio::spawn(super::websocket::serve_websocket(
        1,
        TokioIo::new(server_io),
        to_application,
        from_application,
        async move { stop.cancelled().await },
    ));
    let mut socket =
        WebSocket::after_handshake(TokioIo::new(client), Role::Client);
    // Listener drain finishes its H2 stream without waiting for a close reply.
    socket.set_auto_close(false);
    socket
        .write_frame(Frame::text(Payload::Borrowed(b"request")))
        .await
        .unwrap();
    assert_eq!(
        read_message(&mut application_input).await.unwrap(),
        Some(Message::Text("request".into()))
    );
    write_message(&mut application_output, &Message::Text("reply".into()))
        .await
        .unwrap();
    assert_eq!(&*socket.read_frame().await.unwrap().payload, b"reply");
    cancellation.cancel();
    assert_eq!(socket.read_frame().await.unwrap().opcode, OpCode::Close);
    pump.await.unwrap();
    drop(socket);
    drop(sender);
    connection.abort();
    server.abort();
    let _ = connection.await;
    let _ = server.await;
}

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

fn ready<F: Future>(future: F) -> F::Output {
    match poll_once(std::pin::pin!(future)) {
        Poll::Ready(value) => value,
        Poll::Pending => {
            panic!("fixture IO was expected to complete immediately")
        }
    }
}

#[test]
fn orbit_websocket_shutdown_does_not_wait_for_live_peers() {
    let (server, client) = tokio::io::duplex(128);
    let (to_application, mut application_input) = tokio::io::duplex(128);
    let (from_application, _application_output) = tokio::io::duplex(128);
    let shutdown = CancellationToken::new();
    let mut pump = std::pin::pin!(super::websocket::serve_websocket(
        1,
        server,
        to_application,
        from_application,
        shutdown.cancelled(),
    ));
    assert!(poll_once(pump.as_mut()).is_pending());

    shutdown.cancel();
    assert!(poll_once(pump.as_mut()).is_ready());

    let mut client = WebSocket::after_handshake(client, Role::Client);
    client.set_auto_close(false);
    let close = ready(client.read_frame()).unwrap();
    assert_eq!(close.opcode, OpCode::Close);
    assert_eq!(super::websocket::close_details(&close.payload).0, 1001);
    assert!(ready(read_message(&mut application_input)).unwrap().is_none());
}

/// Writes park until the test explicitly allows them; no clock or sleep.
struct HeldWriter {
    stream: DuplexStream,
    allowed: Arc<AtomicBool>,
    held: Arc<AtomicBool>,
}

impl AsyncRead for HeldWriter {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buffer)
    }
}

impl AsyncWrite for HeldWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if !self.allowed.load(Ordering::Relaxed) {
            self.held.store(true, Ordering::Relaxed);
            return Poll::Pending;
        }
        Pin::new(&mut self.stream).poll_write(cx, buffer)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

#[test]
fn orbit_websocket_shutdown_finishes_the_active_message_in_both_directions() {
    for toward_client in [false, true] {
        let (server, client) = tokio::io::duplex(128);
        let (to_application, mut application_input) = tokio::io::duplex(128);
        let (from_application, mut application_output) = tokio::io::duplex(128);
        let allowed = Arc::new(AtomicBool::new(false));
        let held = Arc::new(AtomicBool::new(false));
        let mut client = WebSocket::after_handshake(client, Role::Client);
        client.set_auto_close(false);
        if toward_client {
            ready(write_message(
                &mut application_output,
                &Message::Text("in flight".to_owned()),
            ))
            .unwrap();
        } else {
            ready(client.write_frame(Frame::text(Payload::Owned(
                b"in flight".to_vec(),
            ))))
            .unwrap();
        }
        let writer = |stream, block| HeldWriter {
            stream,
            allowed: if block {
                allowed.clone()
            } else {
                Arc::new(AtomicBool::new(true))
            },
            held: held.clone(),
        };
        let shutdown = CancellationToken::new();
        let mut pump = std::pin::pin!(super::websocket::serve_websocket(
            2,
            writer(server, toward_client),
            writer(to_application, !toward_client),
            from_application,
            shutdown.cancelled(),
        ));
        assert!(poll_once(pump.as_mut()).is_pending());
        assert!(held.load(Ordering::Relaxed));

        shutdown.cancel();
        assert!(poll_once(pump.as_mut()).is_pending());
        allowed.store(true, Ordering::Relaxed);
        assert!(poll_once(pump.as_mut()).is_ready());

        if toward_client {
            let message = ready(client.read_frame()).unwrap();
            assert_eq!(message.opcode, OpCode::Text);
            assert_eq!(&*message.payload, b"in flight");
        } else {
            let message = ready(read_message(&mut application_input)).unwrap();
            assert!(
                matches!(message, Some(Message::Text(text)) if text == "in flight")
            );
        }
        let close = ready(client.read_frame()).unwrap();
        assert_eq!(close.opcode, OpCode::Close);
        assert_eq!(super::websocket::close_details(&close.payload).0, 1001);
        assert!(ready(read_message(&mut application_input)).unwrap().is_none());
    }
}

#[test]
fn orbit_preserves_authority_and_repeated_headers() {
    for (target, host, expected) in [
        ("/page", Some("octane.test"), "octane.test"),
        ("https://octane.test/page", None, "octane.test"),
        ("https://octane.test:8443/page", None, "octane.test:8443"),
        ("https://[::1]:8443/page", None, "[::1]:8443"),
        ("http://octane.test/page", Some("old.test"), "octane.test"),
    ] {
        let mut headers = HeaderMap::new();
        if let Some(host) = host {
            headers.insert("host", HeaderValue::from_static(host));
        }
        headers.append("x-repeat", HeaderValue::from_static("one"));
        headers.append("x-repeat", HeaderValue::from_static("two"));
        let encoded =
            exchange_headers(&target.parse::<Uri>().unwrap(), &headers);
        let hosts: Vec<_> =
            encoded.iter().filter(|(name, _)| name == b"host").collect();
        assert_eq!(hosts.len(), 1);
        assert_eq!(hosts[0].1, expected.as_bytes());
        let repeated: Vec<_> = encoded
            .iter()
            .filter(|(name, _)| name == b"x-repeat")
            .map(|(_, value)| value.as_slice())
            .collect();
        assert_eq!(repeated, [b"one".as_slice(), b"two".as_slice()]);
    }
}
