use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use claviron_http::body::BodyLimit;
use http::{HeaderMap, HeaderValue};
use http_body_util::{Full, StreamBody};
use hyper::body::Frame;

use super::*;

struct Ended(Arc<AtomicBool>);

impl Body for Ended {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        panic!("an ended native body must not be polled")
    }

    fn is_end_stream(&self) -> bool {
        true
    }
}

impl Drop for Ended {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[test]
fn ended_native_body_is_released_without_a_guard_or_poll() {
    for (limit, timeout) in [
        (None, None),
        (Some(0), None),
        (None, Some(Duration::from_secs(1))),
        (Some(0), Some(Duration::from_secs(1))),
    ] {
        let dropped = Arc::new(AtomicBool::new(false));
        let body = adapt(Ended(dropped.clone()), limit, timeout);
        assert!(dropped.load(Ordering::SeqCst));
        assert!(body.is_end_stream());
        assert_eq!(body.size_hint().exact(), Some(0));
    }
}

#[tokio::test]
async fn unlimited_native_body_stays_lazy_and_releases_its_source_on_cancel() {
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    let frames =
        futures_util::stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|frame| (frame, receiver))
        });
    let mut body = adapt(StreamBody::new(frames), None, None);
    assert!(!sender.is_closed());
    assert!(futures_util::poll!(body.frame()).is_pending());
    let data = Bytes::from_static(b"payload");
    sender.send(Ok::<_, Infallible>(Frame::data(data.clone()))).unwrap();
    let frame = body.frame().await.unwrap().unwrap().into_data().unwrap();
    assert_eq!(frame.as_ptr(), data.as_ptr());
    assert_eq!(frame, data);
    assert!(!body.is_end_stream());
    drop(body);
    assert!(sender.is_closed());
}

#[tokio::test]
async fn zero_length_hint_does_not_discard_pending_trailers() {
    let mut trailers = HeaderMap::new();
    trailers.insert("x-checksum", HeaderValue::from_static("verified"));
    let source =
        Empty::<Bytes>::new().with_trailers(async move { Some(Ok(trailers)) });
    assert_eq!(source.size_hint().exact(), Some(0));
    assert!(!source.is_end_stream());
    let body = adapt(source, None, None);
    assert_eq!(body.size_hint().exact(), Some(0));
    let collected = body.collect().await.unwrap();
    assert_eq!(collected.trailers().unwrap()["x-checksum"], "verified");
}

#[tokio::test]
async fn active_native_size_limit_preserves_hints_and_refuses_excess_data() {
    for timeout in [None, Some(Duration::from_secs(1))] {
        let source = Full::new(Bytes::from_static(b"hello"));
        let body = adapt(source, Some(4), timeout);
        assert_eq!(body.size_hint().exact(), Some(5));
        let error = body.collect().await.unwrap_err();
        assert_eq!(
            error.downcast_ref::<BodyLimit>(),
            Some(&BodyLimit::TooLarge { limit: 4 })
        );
    }
}

#[tokio::test(start_paused = true)]
async fn active_native_timeout_still_refuses_a_stalled_body() {
    let timeout = Duration::from_secs(1);
    for limit in [None, Some(4)] {
        let source = StreamBody::new(futures_util::stream::pending::<
            Result<Frame<Bytes>, Infallible>,
        >());
        let error =
            adapt(source, limit, Some(timeout)).collect().await.unwrap_err();
        assert_eq!(
            error.downcast_ref::<BodyLimit>(),
            Some(&BodyLimit::Stalled { after: timeout })
        );
    }
}
