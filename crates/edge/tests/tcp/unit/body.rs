use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use http::{HeaderMap, HeaderValue};
use http_body_util::{BodyExt, StreamBody};
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
    let dropped = Arc::new(AtomicBool::new(false));
    let body = adapt(Ended(dropped.clone()));
    assert!(dropped.load(Ordering::SeqCst));
    assert!(body.is_end_stream());
    assert_eq!(body.size_hint().exact(), Some(0));
}

#[tokio::test]
async fn unlimited_native_body_stays_lazy_and_releases_its_source_on_cancel() {
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    let frames =
        futures_util::stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|frame| (frame, receiver))
        });
    let mut body = adapt(StreamBody::new(frames));
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
    let body = adapt(source);
    assert_eq!(body.size_hint().exact(), Some(0));
    let collected = body.collect().await.unwrap();
    assert_eq!(collected.trailers().unwrap()["x-checksum"], "verified");
}
