//! The HTTP/1 head limit is hyper's own `max_header_size`, not the read buffer.

use std::convert::Infallible;

use bytes::Bytes;
use http::Response;
use http_body_util::Empty;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::*;

/// One request with a single header value of `value_len` bytes, over the
/// default H1 builder; the response status line.
async fn status_for_header_of(value_len: usize) -> String {
    let (mut peer, server) = tokio::io::duplex(64 * 1024);
    let service = service_fn(|_| async {
        Ok::<_, Infallible>(Response::new(Empty::<Bytes>::new()))
    });
    let connection = tokio::spawn(
        h1(&H1Config::default())
            .serve_connection(TokioIo::new(server), service),
    );
    let request = format!(
        "GET / HTTP/1.1\r\nHost: example.test\r\nConnection: close\r\nX-Fill: {}\r\n\r\n",
        "a".repeat(value_len)
    );
    peer.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    peer.read_to_end(&mut response).await.unwrap();
    let _ = connection.await;
    String::from_utf8_lossy(&response).lines().next().unwrap_or("").to_owned()
}

#[tokio::test]
async fn default_head_limit_is_sixteen_kib() {
    let limit = H1Config::default().max_header_size;
    assert_eq!(limit, 16 * 1024);
    assert!(
        status_for_header_of(limit / 2).await.starts_with("HTTP/1.1 200"),
        "a head well under the limit is served"
    );
    assert!(
        status_for_header_of(limit + 1).await.starts_with("HTTP/1.1 431"),
        "a head over the limit is answered 431"
    );
}
