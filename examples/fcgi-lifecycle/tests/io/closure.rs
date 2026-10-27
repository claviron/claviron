use super::*;
use std::io::ErrorKind;

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
async fn closure_distinguishes_abandonment_from_graceful_and_unrelated_errors()
{
    for closure in [Closure::Abandoned, Closure::Graceful] {
        peer_closed(&mut &b"queued bytes"[..], closure).await.unwrap();
        for kind in [
            ErrorKind::ConnectionReset,
            ErrorKind::BrokenPipe,
            ErrorKind::PermissionDenied,
            ErrorKind::TimedOut,
            ErrorKind::UnexpectedEof,
        ] {
            let result = peer_closed(&mut Failing(kind), closure).await;
            if matches!(closure, Closure::Abandoned)
                && kind == ErrorKind::ConnectionReset
            {
                assert!(result.is_ok());
            } else {
                let error = result.unwrap_err();
                assert_eq!(
                    error.downcast_ref::<std::io::Error>().unwrap().kind(),
                    kind
                );
            }
        }
    }
}
