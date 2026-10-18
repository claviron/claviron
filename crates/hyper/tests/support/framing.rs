//! Shared raw H1 cases. Both production cleartext and TLS drivers run them.

use tokio::io::{AsyncRead, AsyncReadExt};

// RFC 9112 sections 5.1 and 6.3: reject malformed field syntax and framing;
// do not silently discard a malformed framing field and dispatch the request.
pub(crate) const INVALID_HEADERS: &[&str] = &[
    "Bad Header: value\r\n",
    "Missing-Colon\r\n",
    "Content-Length : 0\r\n",
    "Transfer-Encoding : chunked\r\n",
    "Content-Length: nope\r\n",
    "Content-Length: 3\r\nContent-Length: 4\r\n",
    "Transfer-Encoding: gzip\r\n",
];

pub(crate) fn malformed(
    host: &str,
    headers: &str,
) -> String {
    format!("POST / HTTP/1.1\r\nHost: {host}\r\n{headers}\r\n")
}

pub(crate) async fn read_closed(peer: &mut (impl AsyncRead + Unpin)) -> String {
    let mut response = Vec::new();
    peer.take(8193).read_to_end(&mut response).await.unwrap();
    assert!(response.len() <= 8192, "bounded wire response");
    String::from_utf8(response).unwrap()
}

pub(crate) fn assert_bad_request(
    response: &str,
    headers: &str,
) {
    assert!(response.starts_with("HTTP/1.1 400 "), "{headers:?}: {response:?}");
    assert_eq!(response.matches("HTTP/1.1 ").count(), 1);
    assert!(response.split_once("\r\n\r\n").unwrap().1.is_empty());
}

pub(crate) fn valid_keepalive(host: &str) -> String {
    // Repeated equal Content-Length is explicitly permitted (RFC 9112 6.3).
    // The next chunked request must remain correctly framed on this socket.
    format!(
        "POST / HTTP/1.1\r\nHost: {host}\r\nContent-Length: 3\r\nContent-Length: 3\r\n\r\none\
         POST / HTTP/1.1\r\nHost: {host}\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n3\r\ntwo\r\n0\r\n\r\n"
    )
}

pub(crate) fn ambiguous_framing(host: &str) -> String {
    // RFC 9112 6.3 permits processing Transfer-Encoding over Content-Length,
    // but the connection must then close. Hyper implements this option.
    format!(
        "POST / HTTP/1.1\r\nHost: {host}\r\nContent-Length: 100\r\nTransfer-Encoding: chunked\r\n\r\n3\r\none\r\n0\r\n\r\n\
         GET / HTTP/1.1\r\nHost: {host}\r\n\r\n"
    )
}
