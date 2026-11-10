use http::{HeaderMap, HeaderValue, Uri};

use super::exchange_headers;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker};

use claviron_orbit::link::application::{Message, read_message, write_message};
use fastwebsockets::{Frame, OpCode, Payload, Role, WebSocket};
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};
use tokio_util::sync::CancellationToken;

struct UnreadBody(Arc<std::sync::atomic::AtomicUsize>);

impl http_body::Body for UnreadBody {
    type Data = bytes::Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        panic!("native Link selection must not poll an unclaimed body");
    }
}

impl Drop for UnreadBody {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

/// `h1.upgrade` is the operator's switch: an application route does not get
/// around it, and the application is never asked.
#[test]
fn a_websocket_upgrade_without_h1_upgrade_is_refused_before_the_application() {
    use super::{
        DispatchCall, DispatchStep, LinkBackend, LinkRoute,
        RequestPolicyOverride, ServiceResult,
    };
    let backend = LinkBackend {
        routes: vec![LinkRoute {
            path: None,
            destination: Arc::new(super::LinkDestination::unavailable()),
            requests: RequestPolicyOverride::default(),
        }]
        .into(),
        shutdown: CancellationToken::new(),
        h1_upgrade: false,
    };
    let request = http::Request::builder()
        .uri("/socket")
        .header(http::header::CONNECTION, "Upgrade")
        .header(http::header::UPGRADE, "websocket")
        .body(claviron_http::empty_body())
        .unwrap();
    let DispatchCall::Ready(ServiceResult::Handled(response)) =
        backend.call(request, None)
    else {
        panic!("the refusal is synchronous");
    };
    assert_eq!(response.status(), http::StatusCode::FORBIDDEN);
}

#[test]
fn native_link_miss_keeps_the_body_and_a_matched_unavailable_route_owns_503() {
    use super::{
        DispatchCall, DispatchStep, LinkBackend, LinkRoute,
        RequestPolicyOverride, ServiceResult,
    };
    let backend = LinkBackend {
        routes: vec![LinkRoute {
            path: Some(serde_yaml::from_str("/api").unwrap()),
            destination: Arc::new(super::LinkDestination::unavailable()),
            // Retry-After follows the declared queue wait, rounded up.
            requests: RequestPolicyOverride {
                queue_timeout: Some(std::time::Duration::from_millis(1500)),
                ..RequestPolicyOverride::default()
            },
        }]
        .into(),
        shutdown: CancellationToken::new(),
        h1_upgrade: true,
    };
    for (path, handled) in
        [("/apiary", false), ("/elsewhere", false), ("/api/item", true)]
    {
        let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let request = http::Request::builder()
            .uri(path)
            .body(claviron_http::boxed_body(UnreadBody(drops.clone())))
            .unwrap();
        let DispatchCall::Ready(result) = backend.call(request, None) else {
            panic!("mount selection is synchronous");
        };
        match result {
            ServiceResult::Handled(response) => {
                assert!(handled);
                assert_eq!(
                    response.status(),
                    http::StatusCode::SERVICE_UNAVAILABLE
                );
                assert_eq!(response.headers()["retry-after"], "2");
                assert_eq!(drops.load(Ordering::Relaxed), 1);
            }
            ServiceResult::Unhandled(request) => {
                assert!(!handled);
                assert_eq!(request.uri().path(), path);
                assert_eq!(drops.load(Ordering::Relaxed), 0);
                drop(request);
            }
        }
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }
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
