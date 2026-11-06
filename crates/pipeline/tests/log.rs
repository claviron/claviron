use super::*;

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Waker;

struct Observer {
    progress: AccessProgress,
    drops: Arc<AtomicUsize>,
}

impl AsMut<AccessProgress> for Observer {
    fn as_mut(&mut self) -> &mut AccessProgress {
        &mut self.progress
    }
}

impl Drop for Observer {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

fn observer(drops: &Arc<AtomicUsize>) -> Observer {
    Observer { progress: AccessProgress::default(), drops: drops.clone() }
}

#[test]
fn access_log_tracks_original_frames_and_final_grpc_trailers() {
    let data = Bytes::from_static(b"original response");
    let pointer = data.as_ptr();
    let mut trailers = HeaderMap::new();
    trailers.insert("grpc-status", http::HeaderValue::from_static("7"));
    let frames = VecDeque::from([
        Ok::<_, std::convert::Infallible>(Frame::data(data)),
        Ok(Frame::trailers(trailers)),
    ]);
    let drops = Arc::new(AtomicUsize::new(0));
    let mut guard = observer(&drops);
    let mut headers = HeaderMap::new();
    headers.insert("grpc-status", http::HeaderValue::from_static("0"));
    guard.progress.record_head(StatusCode::OK, &headers);
    let mut body = AccessBody {
        inner: claviron_http::boxed_body(http_body_util::StreamBody::new(
            futures_util::stream::iter(frames),
        )),
        guard,
    };
    let mut context = Context::from_waker(Waker::noop());
    let Poll::Ready(Some(Ok(frame))) =
        Pin::new(&mut body).poll_frame(&mut context)
    else {
        panic!("first frame must remain ready");
    };
    assert_eq!(frame.data_ref().unwrap().as_ptr(), pointer);
    assert_eq!(body.guard.progress.bytes, b"original response".len() as u64);
    assert_eq!(body.guard.progress.status, Some(StatusCode::OK));
    assert_eq!(body.guard.progress.grpc_status, Some(0));
    let Poll::Ready(Some(Ok(frame))) =
        Pin::new(&mut body).poll_frame(&mut context)
    else {
        panic!("trailers must remain ready");
    };
    assert_eq!(frame.trailers_ref().unwrap()["grpc-status"], "7");
    assert_eq!(body.guard.progress.grpc_status, Some(7));
    assert!(matches!(
        Pin::new(&mut body).poll_frame(&mut context),
        Poll::Ready(None)
    ));
    assert!(body.guard.progress.complete);
    drop(body);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

#[test]
fn access_log_cancellation_releases_stream_and_record_once_without_polling() {
    struct PendingBody(Arc<AtomicUsize>);
    impl Body for PendingBody {
        type Data = Bytes;
        type Error = std::convert::Infallible;
        fn poll_frame(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
            panic!(
                "logging must not poll a body while wrapping or dropping it"
            );
        }
    }
    impl Drop for PendingBody {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let body_drops = Arc::new(AtomicUsize::new(0));
    let record_drops = Arc::new(AtomicUsize::new(0));
    let guard = observer(&record_drops);
    assert!(guard.progress.status.is_none());
    assert!(guard.progress.grpc_status.is_none());
    let body = AccessBody::wrap(
        claviron_http::boxed_body(PendingBody(body_drops.clone())),
        guard,
    );
    assert_eq!(record_drops.load(Ordering::Relaxed), 0);
    drop(body);
    assert_eq!(body_drops.load(Ordering::Relaxed), 1);
    assert_eq!(record_drops.load(Ordering::Relaxed), 1);
}

#[test]
fn access_log_finishes_empty_bodies_without_retaining_a_stream_guard() {
    let drops = Arc::new(AtomicUsize::new(0));
    let body = AccessBody::wrap(claviron_http::empty_body(), observer(&drops));
    assert!(body.is_end_stream());
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    drop(body);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

#[test]
fn access_log_does_not_infer_grpc_success_or_body_completion_on_error() {
    let drops = Arc::new(AtomicUsize::new(0));
    let mut guard = observer(&drops);
    let mut headers = HeaderMap::new();
    headers.insert("grpc-status", http::HeaderValue::from_static("invalid"));
    guard.progress.record_head(StatusCode::BAD_GATEWAY, &headers);
    assert!(guard.progress.grpc_status.is_none());
    let mut body = AccessBody {
        inner: claviron_http::boxed_body(http_body_util::StreamBody::new(
            futures_util::stream::iter([Err::<Frame<Bytes>, _>(
                std::io::Error::other("body failed"),
            )]),
        )),
        guard,
    };
    assert!(matches!(
        Pin::new(&mut body).poll_frame(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Some(Err(_)))
    ));
    assert!(!body.guard.progress.complete);
    assert_eq!(body.guard.progress.status, Some(StatusCode::BAD_GATEWAY));
    assert_eq!(body.guard.progress.bytes, 0);
    drop(body);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}
