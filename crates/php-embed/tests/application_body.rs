use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

use claviron_http::body::BodyLimit;
use http::{HeaderMap, Request};
use http_body::Frame;

use super::*;

struct ProbeBody {
    frames: VecDeque<Result<Frame<Bytes>, BodyLimit>>,
    ended: bool,
    forbid_poll: bool,
    polls: Arc<AtomicUsize>,
}

impl Body for ProbeBody {
    type Data = Bytes;
    type Error = BodyLimit;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BodyLimit>>> {
        assert!(
            !self.forbid_poll,
            "ended GET/HEAD must never enter body polling"
        );
        self.polls.fetch_add(1, Ordering::Relaxed);
        Poll::Ready(self.frames.pop_front())
    }

    fn is_end_stream(&self) -> bool {
        self.ended
    }
}

fn collect(
    method: Method,
    length: Option<usize>,
    body: ProbeBody,
    limit: Option<usize>,
    stopping: CancellationToken,
) -> Result<Bytes, Rejection> {
    let mut request = Request::builder().method(method).uri("/example");
    if let Some(length) = length {
        request = request.header(CONTENT_LENGTH, length);
    }
    let (parts, ()) = request.body(()).unwrap().into_parts();
    let mut policy = RequestPolicy::unlimited();
    policy.max_body_size = limit;
    let mut future = Box::pin(read(
        &parts,
        claviron_http::boxed_body(body),
        &policy,
        &stopping,
    ));
    match future.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(result) => result,
        Poll::Pending => {
            panic!("the body fixture must complete without a runtime")
        }
    }
}

fn probe(
    ended: bool,
    frames: Vec<Result<Frame<Bytes>, BodyLimit>>,
) -> (ProbeBody, Arc<AtomicUsize>) {
    let polls = Arc::new(AtomicUsize::new(0));
    (
        ProbeBody {
            frames: frames.into(),
            ended,
            forbid_poll: false,
            polls: Arc::clone(&polls),
        },
        polls,
    )
}

#[test]
fn application_body_ended_get_and_head_never_poll() {
    for method in [Method::GET, Method::HEAD] {
        let (mut body, polls) = probe(true, vec![]);
        body.forbid_poll = true;
        assert!(
            collect(method, Some(0), body, Some(0), CancellationToken::new())
                .unwrap()
                .is_empty()
        );
        assert_eq!(polls.load(Ordering::Relaxed), 0);
    }
}

#[test]
fn application_body_unknown_empty_get_and_head_still_read() {
    for method in [Method::GET, Method::HEAD] {
        let (body, polls) = probe(false, vec![]);
        assert!(
            collect(method, None, body, Some(0), CancellationToken::new())
                .unwrap()
                .is_empty()
        );
        assert_eq!(polls.load(Ordering::Relaxed), 1);
    }
}

#[test]
fn application_body_content_length_zero_does_not_prove_empty() {
    for method in [Method::GET, Method::HEAD] {
        let (body, polls) =
            probe(false, vec![Ok(Frame::data(Bytes::from_static(b"x")))]);
        assert!(matches!(
            collect(method, Some(0), body, None, CancellationToken::new()),
            Err(Rejection::BadRequest(_))
        ));
        assert_eq!(polls.load(Ordering::Relaxed), 2);
    }
}

#[test]
fn application_body_payloads_preserve_bytes_for_all_methods() {
    for method in [
        Method::GET,
        Method::HEAD,
        Method::POST,
        Method::PUT,
        Method::PATCH,
        Method::DELETE,
    ] {
        let (body, polls) = probe(
            false,
            vec![
                Ok(Frame::data(Bytes::from_static(b"ab"))),
                Ok(Frame::data(Bytes::from_static(b"\0\xff"))),
            ],
        );
        assert_eq!(
            collect(method, Some(4), body, Some(4), CancellationToken::new())
                .unwrap(),
            Bytes::from_static(b"ab\0\xff")
        );
        assert_eq!(polls.load(Ordering::Relaxed), 3);
    }
}

#[test]
fn application_body_limits_cover_declared_and_chunked_payloads() {
    let (body, polls) =
        probe(false, vec![Ok(Frame::data(Bytes::from_static(b"abcd")))]);
    assert!(matches!(
        collect(Method::POST, Some(4), body, Some(3), CancellationToken::new()),
        Err(Rejection::TooLarge)
    ));
    assert_eq!(polls.load(Ordering::Relaxed), 0);
    let (body, polls) = probe(
        false,
        vec![
            Ok(Frame::data(Bytes::from_static(b"ab"))),
            Ok(Frame::data(Bytes::from_static(b"cd"))),
        ],
    );
    assert!(matches!(
        collect(Method::DELETE, None, body, Some(3), CancellationToken::new()),
        Err(Rejection::TooLarge)
    ));
    assert_eq!(polls.load(Ordering::Relaxed), 2);
}

#[test]
fn application_body_transport_limit_and_trailers_keep_rejections() {
    let (body, _) = probe(false, vec![Err(BodyLimit::TooLarge { limit: 3 })]);
    assert!(matches!(
        collect(Method::POST, None, body, Some(3), CancellationToken::new()),
        Err(Rejection::TooLarge)
    ));
    let (body, _) = probe(false, vec![Ok(Frame::trailers(HeaderMap::new()))]);
    assert!(matches!(
        collect(Method::POST, None, body, None, CancellationToken::new()),
        Err(Rejection::BadRequest(_))
    ));
}

#[test]
fn application_body_stop_keeps_nonempty_reads_cancellable() {
    let stopping = CancellationToken::new();
    stopping.cancel();
    let (mut body, polls) = probe(false, vec![]);
    body.forbid_poll = true;
    assert!(matches!(
        collect(Method::POST, None, body, None, stopping),
        Err(Rejection::Stopped)
    ));
    assert_eq!(polls.load(Ordering::Relaxed), 0);
}

#[test]
fn application_body_single_frame_keeps_binary_payload_allocation() {
    let payload = Bytes::from(vec![0, 255, 128, 1]);
    let pointer = payload.as_ptr();
    let (body, _) = probe(false, vec![Ok(Frame::data(payload.clone()))]);
    let received = collect(
        Method::POST,
        Some(payload.len()),
        body,
        Some(payload.len()),
        CancellationToken::new(),
    )
    .unwrap();
    assert_eq!(received, payload);
    assert_eq!(
        received.as_ptr(),
        pointer,
        "a single frame must not be copied or decoded"
    );
}
