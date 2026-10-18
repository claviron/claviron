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
    let mut pump = std::pin::pin!(super::serve_websocket(
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
    assert_eq!(super::close_details(&close.payload).0, 1001);
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
        let mut pump = std::pin::pin!(super::serve_websocket(
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
        assert_eq!(super::close_details(&close.payload).0, 1001);
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
