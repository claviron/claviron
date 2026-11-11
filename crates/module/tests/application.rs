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
            max_queued: 1,
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
    application.open.send_modify(|open| *open += 1);
    let exchange = Exchange::new(request, head, None, Arc::clone(&application));
    assert_eq!(application.submit_exchange(&exchange), Some(false));

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

#[tokio::test]
async fn zero_queue_serves_an_idle_thread_and_refuses_while_busy() {
    struct Gate {
        entered: Mutex<Option<oneshot::Sender<()>>>,
        release: Mutex<std::sync::mpsc::Receiver<()>>,
    }

    unsafe extern "C" fn hold_request(
        request: *mut abi::Request,
        context: *mut c_void,
    ) -> u32 {
        // The test keeps the gate alive until it joins the worker thread.
        let gate = unsafe { &*context.cast::<Gate>() };
        if let Some(entered) = gate.entered.lock().unwrap().take() {
            let _ = entered.send(());
        }
        if gate.release.lock().unwrap().recv().is_err() {
            return abi::CLOSED;
        }
        unsafe {
            response_begin(request, 200);
            response_finish(request)
        }
    }

    let (entered, started) = oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let gate = Arc::into_raw(Arc::new(Gate {
        entered: Mutex::new(Some(entered)),
        release: Mutex::new(released),
    }));
    let table = abi::HttpHandler {
        struct_size: size_of::<abi::HttpHandler>() as u32,
        request: Some(hold_request),
        body: None,
        writable: None,
        closed: None,
    };
    let description = abi::Application {
        struct_size: size_of::<abi::Application>() as u32,
        context: gate.cast_mut().cast(),
        start: None,
        stop: None,
        http: &table,
        websocket: std::ptr::null(),
    };
    let application = Arc::new(ModuleApplication::new(
        "no-queue".into(),
        description,
        table,
        None,
        ApplicationConfig {
            instances: NonZeroU16::MIN,
            max_queued: 0,
            frameworks: Vec::new(),
            icon: None,
        },
    ));
    application.serve().unwrap();
    application.begin().await.unwrap();
    let backend = ModuleBackend {
        application: Arc::clone(&application),
        h1_upgrade: false,
    };
    let request = || Request::new(claviron_http::bytes_body(Bytes::new()));

    let DispatchCall::Pending(first) = backend.call(request(), None) else {
        panic!("the idle thread should accept work");
    };
    started.await.unwrap();
    let DispatchCall::Ready(ServiceResult::Handled(refused)) =
        backend.call(request(), None)
    else {
        panic!("the occupied thread should refuse a new request immediately");
    };
    assert_eq!(refused.status(), StatusCode::SERVICE_UNAVAILABLE);

    release.send(()).unwrap();
    let ServiceResult::Handled(reply) = first.await else {
        panic!("the first exchange should be served");
    };
    assert_eq!(reply.status(), StatusCode::OK);
    drop(reply);
    tokio::task::spawn_blocking(move || application.shutdown_threads())
        .await
        .unwrap();
    // No callbacks can use the context after the worker is joined.
    drop(unsafe { Arc::from_raw(gate) });
}
