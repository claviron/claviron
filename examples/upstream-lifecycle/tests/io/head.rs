use super::*;
use std::{
    io::ErrorKind,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::ReadBuf;

struct Failing(ErrorKind);
impl AsyncRead for Failing {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Err(self.0.into()))
    }
}

#[tokio::test]
async fn only_empty_eof_is_a_discarded_warm_transport() {
    assert!(!head(&mut &b""[..]).await.unwrap());
    let mut request = &b"GET / HTTP/1.1\r\nHost: example.test\r\n\r\nbody"[..];
    assert!(head(&mut request).await.unwrap());
    assert_eq!(request, b"body");
    let error = head(&mut &b"GET / HTTP/1.1\r\nHost: example.test"[..])
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::UnexpectedEof);
    let oversized = vec![b'x'; 8193];
    assert_eq!(
        head(&mut oversized.as_slice()).await.unwrap_err().kind(),
        ErrorKind::InvalidData
    );
    for kind in [
        ErrorKind::ConnectionReset,
        ErrorKind::BrokenPipe,
        ErrorKind::PermissionDenied,
        ErrorKind::TimedOut,
    ] {
        assert_eq!(head(&mut Failing(kind)).await.unwrap_err().kind(), kind);
    }
}
