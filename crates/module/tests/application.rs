use std::collections::VecDeque;
use std::ffi::c_void;
use std::num::NonZeroU16;
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body::{Body, Frame};
use http_body_util::BodyExt;

use super::*;
use crate::abi::{self, Slice};
use crate::request::{
    request_read_body, response_begin, response_finish, response_write,
};

/// A body whose last frame reads as its end, as a declared length's does.
struct Frames(VecDeque<Bytes>);

impl Body for Frames {
    type Data = Bytes;
    type Error = claviron_http::RouteBodyError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        Poll::Ready(self.0.pop_front().map(|piece| Ok(Frame::data(piece))))
    }

    fn is_end_stream(&self) -> bool {
        self.0.is_empty()
    }
}

/// `(length, end, what the write returned)`, in order.
static CALLS: Mutex<Vec<(u64, u32, u32)>> = Mutex::new(Vec::new());

unsafe extern "C" fn on_request(
    request: *mut abi::Request,
    _: *mut c_void,
) -> u32 {
    unsafe {
        response_begin(request, 200);
        request_read_body(request)
    }
}

/// The shape every echo takes: `end` says only that, and a write is not retried.
unsafe extern "C" fn on_body(
    request: *mut abi::Request,
    data: *const u8,
    length: u64,
    end: u32,
    _: *mut c_void,
) -> u32 {
    if end != 0 {
        CALLS.lock().unwrap().push((length, end, abi::OK));
        return unsafe { response_finish(request) };
    }
    let piece = unsafe { std::slice::from_raw_parts(data, length as usize) };
    let written = unsafe { response_write(request, Slice::of(piece)) };
    CALLS.lock().unwrap().push((length, end, written));
    written
}

#[tokio::test(flavor = "multi_thread")]
async fn a_body_echoed_from_its_callback_arrives_whole() {
    let table = abi::HttpHandler {
        struct_size: size_of::<abi::HttpHandler>() as u32,
        request: Some(on_request),
        body: Some(on_body),
        writable: None,
        closed: None,
    };
    let description = abi::Application {
        struct_size: size_of::<abi::Application>() as u32,
        context: std::ptr::null_mut(),
        start: None,
        stop: None,
        http: &table,
        websocket: std::ptr::null(),
    };
    let application = Arc::new(ModuleApplication::new(
        "echo".into(),
        description,
        table,
        None,
        ApplicationConfig {
            instances: NonZeroU16::new(1).unwrap(),
            max_waiting: 1,
            frameworks: Vec::new(),
            icon: None,
        },
    ));
    application.serve().unwrap();

    // More pieces than the reply holds, so each has to wait for the last.
    let pieces: VecDeque<Bytes> =
        (0..64u8).map(|piece| Bytes::from(vec![piece; 1024])).collect();
    let expected: Vec<u8> = pieces.iter().flatten().copied().collect();
    let request = Request::new(Frames(pieces).boxed_unsync());

    let (head, answered) = oneshot::channel();
    application.waiting.fetch_add(1, Ordering::AcqRel);
    application.open.send_modify(|open| *open += 1);
    let exchange = Exchange::new(request, head, None, Arc::clone(&application));
    let turn = Arc::clone(&exchange);
    assert!(application.submit(move || turn.run()));

    let reply = answered.await.unwrap();
    let echoed = reply.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&echoed[..], &expected[..]);

    let calls = CALLS.lock().unwrap().clone();
    assert_eq!(calls.len(), 65);
    assert!(calls[..64].iter().all(|call| *call == (1024, 0, abi::OK)));
    assert_eq!(calls[64], (0, 1, abi::OK));

    drop(exchange);
    tokio::task::spawn_blocking(move || application.shutdown_threads())
        .await
        .unwrap();
}
