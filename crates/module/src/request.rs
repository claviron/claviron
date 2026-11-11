//! One exchange as a module holds it: the request's head in place, and the reply
//! being built. Reading copies nothing; the reply's bytes become `Bytes` once.

use std::ffi::c_void;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use bytes::Bytes;
use claviron_http::ext::ConnCtx;
use claviron_http::policy::HttpPolicy;
use claviron_http::socket::Offer;
use claviron_http::{RouteBody, RouteResponse, boxed_body, err_status};
use claviron_transport::owner::RequestBody;
use claviron_transport::websocket::{Deliver, Message, accept};
use http::header::{HeaderName, HeaderValue};
use http::{HeaderMap, StatusCode};
use http_body::Frame;
use tokio::sync::{mpsc, oneshot, watch};

use crate::abi::{self, HttpHandler, Request, SessionHandler, Slice};
use crate::application::ModuleApplication;
use crate::registrar::contained;
use crate::service::SendPtr;

const READY: u8 = 3;
const QUEUED: u8 = 0;
const STARTED: u8 = 1;
const ABANDONED: u8 = 2;

#[derive(Default)]
struct Reply {
    head: Option<oneshot::Sender<RouteResponse>>,
    status: Option<StatusCode>,
    headers: HeaderMap,
    body: Option<mpsc::Sender<Bytes>>,
    finished: bool,
}

pub(crate) struct Exchange {
    parts: http::request::Parts,
    peer: String,
    local: String,
    scheme: String,
    version: &'static str,
    body: Mutex<Option<RouteBody>>,
    reply: Mutex<Reply>,
    /// Asked for a callback when output room returns.
    want_writable: AtomicBool,
    /// Ended with a failure after the head had gone.
    aborted: AtomicBool,
    closed: AtomicBool,
    closing: watch::Sender<bool>,
    progress: AtomicU8,
    data: AtomicPtr<c_void>,
    table: HttpHandler,
    context: SendPtr,
    /// A session attempt, with the table that answers it.
    session: Option<(Offer, SessionHandler)>,
    outbound: Mutex<Option<mpsc::Sender<Message>>>,
    application: Arc<ModuleApplication>,
    runtime: tokio::runtime::Handle,
}

// Safety: a table holds function pointers a module declares callable from the
// threads it is called on, and the context is that module's.
unsafe impl Send for Exchange {}
unsafe impl Sync for Exchange {}

impl Exchange {
    pub(crate) fn new(
        request: http::Request<RouteBody>,
        head: oneshot::Sender<RouteResponse>,
        offer: Option<Offer>,
        application: Arc<ModuleApplication>,
    ) -> Arc<Self> {
        let (parts, body) = request.into_parts();
        let connection = parts.extensions.get::<Arc<ConnCtx>>();
        let peer = connection
            .map(|connection| connection.remote.to_string())
            .unwrap_or_default();
        let local = connection
            .map(|connection| {
                std::net::SocketAddr::new(
                    connection.local_ip,
                    connection.local_port,
                )
                .to_string()
            })
            .unwrap_or_default();
        let scheme = connection
            .map(|connection| connection.scheme().to_string())
            .unwrap_or_default();
        let version = match parts.version {
            http::Version::HTTP_10 => "1.0",
            http::Version::HTTP_11 => "1.1",
            http::Version::HTTP_2 => "2",
            http::Version::HTTP_3 => "3",
            _ => "",
        };
        let (table, websocket, context) = application.table();
        Arc::new(Self {
            parts,
            peer,
            local,
            scheme,
            version,
            body: Mutex::new(Some(body)),
            reply: Mutex::new(Reply { head: Some(head), ..Reply::default() }),
            want_writable: AtomicBool::new(false),
            aborted: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            closing: watch::Sender::new(false),
            progress: AtomicU8::new(READY),
            data: AtomicPtr::new(std::ptr::null_mut()),
            table,
            context,
            session: offer.zip(websocket),
            outbound: Mutex::new(None),
            application,
            runtime: tokio::runtime::Handle::current(),
        })
    }

    fn handle(self: &Arc<Self>) -> *mut Request {
        Arc::as_ptr(self).cast_mut().cast()
    }

    /// Take a waiting place before the pool makes this exchange visible.
    pub(crate) fn enter_queue(&self) -> bool {
        if !self.application.enter_queue() {
            return false;
        }
        self.progress.store(QUEUED, Ordering::Release);
        true
    }

    fn leave_admission(
        &self,
        next: u8,
    ) -> bool {
        match self.progress.try_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |state| matches!(state, READY | QUEUED).then_some(next),
        ) {
            Ok(QUEUED) => {
                self.application.left_queue();
                true
            }
            Ok(_) => true,
            Err(_) => false,
        }
    }

    /// The request's turn on a thread. It runs unless the wait was given up.
    pub(crate) fn run(self: &Arc<Self>) {
        if !self.leave_admission(STARTED) {
            return;
        }
        let callback = match &self.session {
            Some((_, session)) => session.open,
            None => self.table.request,
        };
        let Some(request) = callback else { return };
        let (handle, context) = (self.handle(), self.context.0);
        let status = contained(|| unsafe { request(handle, context) });
        if status != abi::OK {
            self.fail(status);
        }
    }

    /// Give up a wait that has not started. `false` when it already has.
    pub(crate) fn abandon(&self) -> bool {
        self.leave_admission(ABANDONED)
    }

    /// A module's call ended this exchange with a failure: 500 if nothing has
    /// gone out, otherwise a reply cut short rather than one that looks whole.
    fn fail(
        self: &Arc<Self>,
        status: u32,
    ) {
        let mut reply = self.reply.lock().expect("reply");
        if let Some(head) = reply.head.take() {
            drop(reply);
            let _ = head.send(err_status(StatusCode::INTERNAL_SERVER_ERROR));
            self.close(status);
        } else {
            self.aborted.store(true, Ordering::Release);
            reply.body = None;
            drop(reply);
            // A session closes with an error status when its owner side goes.
            self.outbound.lock().expect("outbound").take();
        }
    }

    /// The end of this exchange, whichever way it came: `closed` runs once.
    pub(crate) fn close(
        self: &Arc<Self>,
        status: u32,
    ) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.closing.send_replace(true);
        // Never started, so the module has not heard of it.
        let heard = self.progress.load(Ordering::Acquire) == STARTED;
        let this = Arc::clone(self);
        let submitted = self.application.submit(move || {
            let closed = match &this.session {
                Some((_, session)) => session.closed,
                None => this.table.closed,
            };
            if let (true, Some(closed)) = (heard, closed) {
                let (handle, context) = (this.handle(), this.context.0);
                contained(|| {
                    unsafe { closed(handle, status, context) };
                    abi::OK
                });
            }
            this.application.exchange_closed();
        });
        if !submitted {
            self.application.exchange_closed();
        }
    }

    fn drained(self: &Arc<Self>) {
        if !self.want_writable.swap(false, Ordering::AcqRel) {
            return;
        }
        let callback = match &self.session {
            Some((_, session)) => session.writable,
            None => self.table.writable,
        };
        let Some(writable) = callback else { return };
        let this = Arc::clone(self);
        self.application.submit(move || {
            let (handle, context) = (this.handle(), this.context.0);
            let status = contained(|| unsafe { writable(handle, context) });
            if status != abi::OK {
                this.fail(status);
            }
        });
    }

    /// Send the head if it has not gone. It opens the body the reply writes to.
    fn send_head(
        self: &Arc<Self>,
        reply: &mut Reply,
    ) -> u32 {
        let Some(head) = reply.head.take() else { return abi::OK };
        let (sender, receiver) = mpsc::channel(1);
        reply.body = Some(sender);
        let mut response = http::Response::new(boxed_body(ReplyBody {
            receiver,
            exchange: Arc::clone(self),
            ended: false,
        }));
        *response.status_mut() = reply.status.unwrap_or(StatusCode::OK);
        *response.headers_mut() = std::mem::take(&mut reply.headers);
        if head.send(response).is_err() {
            return abi::CLOSED;
        }
        abi::OK
    }

    /// Propose the session with the headers set so far. The door commits it, and
    /// whatever follows runs between the socket and this exchange.
    fn accept_session(
        self: &Arc<Self>,
        limit: u64,
    ) -> u32 {
        let Some((offer, _)) = &self.session else {
            return abi::UNSUPPORTED;
        };
        let (Ok(limit), true) = (usize::try_from(limit), limit > 0) else {
            return abi::INVALID_ARGUMENT;
        };
        let mut reply = self.reply.lock().expect("reply");
        let Some(head) = reply.head.take() else {
            return abi::INVALID_ARGUMENT;
        };
        let headers = std::mem::take(&mut reply.headers)
            .iter()
            .map(|(name, value)| {
                (name.as_str().as_bytes().to_vec(), value.as_bytes().to_vec())
            })
            .collect();
        drop(reply);
        let (outbound, receiver) = mpsc::channel(1);
        *self.outbound.lock().expect("outbound") = Some(outbound);
        let mut attempt = http::Request::new(RequestBody::empty());
        attempt.extensions_mut().insert(offer.clone());
        let proposal = accept(
            attempt,
            headers,
            Inbound(Arc::clone(self)),
            receiver,
            limit,
        );
        if head.send(proposal).is_err() {
            return abi::CLOSED;
        }
        self.hear_retirement();
        abi::OK
    }

    /// An application stopping tells its open sessions, once, as a close the
    /// client did not send, in order with the session's other messages.
    fn hear_retirement(self: &Arc<Self>) {
        let this = Arc::clone(self);
        self.runtime.spawn(async move {
            let mut closing = this.closing.subscribe();
            let mut retiring = this.application.retiring();
            let retired = tokio::select! {
                _ = closing.wait_for(|closed| *closed) => false,
                changed = retiring.changed() => changed.is_ok(),
            };
            if retired {
                this.deliver(Message::Close {
                    code: 1001,
                    reason: "server shutting down".to_owned(),
                })
                .await;
            }
        });
    }

    /// Hand one message to the module and wait for its callback to return.
    async fn deliver(
        self: &Arc<Self>,
        message: Message,
    ) -> bool {
        let Some(callback) =
            self.session.as_ref().and_then(|(_, session)| session.message)
        else {
            return false;
        };
        if self.closed.load(Ordering::Acquire) {
            return false;
        }
        let (done, delivered) = oneshot::channel();
        let this = Arc::clone(self);
        let submitted = self.application.submit(move || {
            let (kind, code, data): (u32, u32, &[u8]) = match &message {
                Message::Text(text) => (abi::MESSAGE_TEXT, 0, text.as_bytes()),
                Message::Binary(bytes) => (abi::MESSAGE_BINARY, 0, bytes),
                Message::Close { code, reason } => {
                    (abi::MESSAGE_CLOSE, (*code).into(), reason.as_bytes())
                }
            };
            let (handle, context) = (this.handle(), this.context.0);
            let status = contained(|| unsafe {
                callback(
                    handle,
                    kind,
                    code,
                    data.as_ptr(),
                    data.len() as u64,
                    context,
                )
            });
            if status != abi::OK {
                this.fail(status);
            }
            let _ = done.send(status == abi::OK);
        });
        submitted && delivered.await.unwrap_or(false)
    }

    fn send_message(
        self: &Arc<Self>,
        message: Message,
    ) -> u32 {
        if self.closed.load(Ordering::Acquire) {
            return abi::CLOSED;
        }
        let outbound = self.outbound.lock().expect("outbound");
        let Some(sender) = outbound.as_ref() else {
            return abi::CLOSED;
        };
        match sender.try_send(message) {
            Ok(()) => abi::OK,
            Err(mpsc::error::TrySendError::Closed(_)) => abi::CLOSED,
            Err(mpsc::error::TrySendError::Full(_)) => {
                let sender = sender.clone();
                drop(outbound);
                // Say when room returns: what took the last message is the pump,
                // and it knows nothing of this.
                self.want_writable.store(true, Ordering::Release);
                let this = Arc::clone(self);
                self.runtime.spawn(async move {
                    if sender.reserve().await.is_ok() {
                        this.drained();
                    }
                });
                abi::BUSY
            }
        }
    }

    fn read_body(self: &Arc<Self>) -> u32 {
        if self.session.is_some() {
            return abi::UNSUPPORTED;
        }
        let Some(callback) = self.table.body else {
            return abi::UNSUPPORTED;
        };
        let Some(mut body) = self.body.lock().expect("body").take() else {
            return abi::INVALID_ARGUMENT;
        };
        let this = Arc::clone(self);
        self.runtime.spawn(async move {
            let mut closing = this.closing.subscribe();
            loop {
                // Input advances once the last output has drained: a handler
                // answering from its body callback has room to write.
                let output =
                    this.reply.lock().expect("reply").body.clone();
                if let Some(output) = output {
                    tokio::select! {
                        biased;
                        _ = closing.wait_for(|closed| *closed) => return,
                        _ = output.reserve() => {}
                    }
                }
                let frame = tokio::select! {
                    biased;
                    _ = closing.wait_for(|closed| *closed) => return,
                    frame = std::future::poll_fn(|context| {
                        http_body::Body::poll_frame(Pin::new(&mut body), context)
                    }) => frame,
                };
                let (piece, end) = match frame {
                    Some(Ok(frame)) => match frame.into_data() {
                        Ok(piece) => (piece, false),
                        // Trailers carry nothing a module reads here.
                        Err(_) => continue,
                    },
                    None => (Bytes::new(), true),
                    Some(Err(error)) => {
                        tracing::debug!(%error, "module request body failed");
                        this.fail(abi::CLOSED);
                        return;
                    }
                };
                let (done, delivered) = oneshot::channel();
                let call = Arc::clone(&this);
                let submitted = this.application.submit(move || {
                    let (handle, context) = (call.handle(), call.context.0);
                    let status = contained(|| unsafe {
                        callback(
                            handle,
                            piece.as_ptr(),
                            piece.len() as u64,
                            u32::from(end),
                            context,
                        )
                    });
                    if status != abi::OK {
                        call.fail(status);
                    }
                    let _ = done.send(());
                });
                if !submitted || delivered.await.is_err() || end {
                    return;
                }
            }
        });
        abi::OK
    }
}

/// The client's side of a session, as the socket's pump drives it.
struct Inbound(Arc<Exchange>);

impl Deliver for Inbound {
    fn deliver(
        &self,
        message: Message,
    ) -> impl std::future::Future<Output = bool> + Send {
        let exchange = Arc::clone(&self.0);
        async move { exchange.deliver(message).await }
    }

    fn end(&self) {
        self.0.close(abi::OK);
    }
}

/// The reply's body: what the module writes, as it arrives. Its end, or its drop
/// when the client has gone, is the end of the exchange.
struct ReplyBody {
    receiver: mpsc::Receiver<Bytes>,
    exchange: Arc<Exchange>,
    ended: bool,
}

#[derive(Debug)]
struct Aborted;

impl std::fmt::Display for Aborted {
    fn fmt(
        &self,
        formatter: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        formatter.write_str("the module ended this reply with a failure")
    }
}

impl std::error::Error for Aborted {}

impl http_body::Body for ReplyBody {
    type Data = Bytes;
    type Error = Aborted;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Aborted>>> {
        match self.receiver.poll_recv(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(piece)) => {
                self.exchange.drained();
                Poll::Ready(Some(Ok(Frame::data(piece))))
            }
            Poll::Ready(None) => {
                self.ended = true;
                if self.exchange.aborted.load(Ordering::Acquire) {
                    self.exchange.close(abi::CLOSED);
                    return Poll::Ready(Some(Err(Aborted)));
                }
                self.exchange.close(abi::OK);
                Poll::Ready(None)
            }
        }
    }
}

impl Drop for ReplyBody {
    fn drop(&mut self) {
        if !self.ended {
            self.exchange.close(abi::CLOSED);
        }
    }
}

/// Run `call` against a module's handle.
///
/// # Safety
/// A handle is valid from its request callback until its closed callback returns.
unsafe fn with(
    request: *const Request,
    call: impl FnOnce(&Arc<Exchange>) -> u32,
) -> u32 {
    if request.is_null() {
        return abi::INVALID_ARGUMENT;
    }
    contained(|| {
        let raw = request.cast::<Exchange>();
        // Safety: the pool holds an Arc for as long as the handle is valid.
        unsafe { Arc::increment_strong_count(raw) };
        let exchange = unsafe { Arc::from_raw(raw) };
        call(&exchange)
    })
}

pub(crate) unsafe extern "C" fn request_field(
    request: *const Request,
    field: u32,
    index: u64,
    out: *mut Slice,
) -> u32 {
    if out.is_null() {
        return abi::INVALID_ARGUMENT;
    }
    unsafe {
        with(request, |exchange| {
            let parts = &exchange.parts;
            let nth = || parts.headers.iter().nth(index as usize);
            let bytes: Option<&[u8]> = match field {
                abi::FIELD_METHOD => Some(parts.method.as_str().as_bytes()),
                abi::FIELD_SCHEME => Some(exchange.scheme.as_bytes()),
                abi::FIELD_PATH => Some(parts.uri.path().as_bytes()),
                abi::FIELD_QUERY => {
                    Some(parts.uri.query().unwrap_or_default().as_bytes())
                }
                abi::FIELD_PEER => Some(exchange.peer.as_bytes()),
                abi::FIELD_LOCAL => Some(exchange.local.as_bytes()),
                abi::FIELD_VERSION => Some(exchange.version.as_bytes()),
                abi::FIELD_HEADER_NAME => {
                    nth().map(|(name, _)| name.as_str().as_bytes())
                }
                abi::FIELD_HEADER_VALUE => {
                    nth().map(|(_, value)| value.as_bytes())
                }
                _ => None,
            };
            let Some(bytes) = bytes else { return abi::INVALID_ARGUMENT };
            // Safety: checked above, and a module's output is writable.
            *out = Slice::of(bytes);
            abi::OK
        })
    }
}

pub(crate) unsafe extern "C" fn request_header_count(
    request: *const Request
) -> u64 {
    let mut count = 0;
    unsafe {
        with(request, |exchange| {
            count = exchange.parts.headers.len() as u64;
            abi::OK
        })
    };
    count
}

pub(crate) unsafe extern "C" fn request_policy(
    request: *const Request,
    out: *mut abi::RequestPolicy,
) -> u32 {
    if out.is_null() {
        return abi::INVALID_ARGUMENT;
    }
    unsafe {
        with(request, |exchange| {
            let off = |limit: Option<usize>| {
                limit.map_or(u64::MAX, |bytes| bytes as u64)
            };
            let policy = exchange.parts.extensions.get::<HttpPolicy>();
            *out = abi::RequestPolicy {
                struct_size: size_of::<abi::RequestPolicy>() as u32,
                max_request_body: off(
                    policy.and_then(|p| p.requests.max_body_size)
                ),
                max_response_body: off(
                    policy.and_then(|p| p.responses.max_body_size)
                ),
            };
            abi::OK
        })
    }
}

pub(crate) unsafe extern "C" fn request_set_data(
    request: *mut Request,
    data: *mut c_void,
) -> u32 {
    unsafe {
        with(request, |exchange| {
            exchange.data.store(data, Ordering::Release);
            abi::OK
        })
    }
}

pub(crate) unsafe extern "C" fn request_data(
    request: *const Request
) -> *mut c_void {
    let mut data = std::ptr::null_mut();
    unsafe {
        with(request, |exchange| {
            data = exchange.data.load(Ordering::Acquire);
            abi::OK
        })
    };
    data
}

pub(crate) unsafe extern "C" fn request_read_body(
    request: *mut Request
) -> u32 {
    unsafe { with(request, Exchange::read_body) }
}

pub(crate) unsafe extern "C" fn response_begin(
    request: *mut Request,
    status: u32,
) -> u32 {
    unsafe {
        with(request, |exchange| {
            let Some(status) = u16::try_from(status)
                .ok()
                .and_then(|status| StatusCode::from_u16(status).ok())
                .filter(|status| !status.is_informational())
            else {
                return abi::INVALID_ARGUMENT;
            };
            let mut reply = exchange.reply.lock().expect("reply");
            if reply.head.is_none() || reply.status.is_some() {
                return abi::INVALID_ARGUMENT;
            }
            reply.status = Some(status);
            abi::OK
        })
    }
}

pub(crate) unsafe extern "C" fn response_header(
    request: *mut Request,
    name: Slice,
    value: Slice,
) -> u32 {
    unsafe {
        with(request, |exchange| {
            let (Ok(name), Ok(value)) = (
                HeaderName::from_bytes(name.bytes()),
                HeaderValue::from_bytes(value.bytes()),
            ) else {
                return abi::INVALID_ARGUMENT;
            };
            let mut reply = exchange.reply.lock().expect("reply");
            if reply.head.is_none() {
                return abi::INVALID_ARGUMENT;
            }
            reply.headers.append(name, value);
            abi::OK
        })
    }
}

pub(crate) unsafe extern "C" fn response_write(
    request: *mut Request,
    data: Slice,
) -> u32 {
    unsafe {
        with(request, |exchange| {
            if exchange.closed.load(Ordering::Acquire) {
                return abi::CLOSED;
            }
            let mut reply = exchange.reply.lock().expect("reply");
            if reply.finished {
                return abi::INVALID_ARGUMENT;
            }
            let opened = exchange.send_head(&mut reply);
            if opened != abi::OK {
                return opened;
            }
            let Some(body) = reply.body.as_ref() else {
                return abi::CLOSED;
            };
            if let Ok(permit) = body.try_reserve() {
                permit.send(Bytes::copy_from_slice(data.bytes()));
                return abi::OK;
            }
            if body.is_closed() {
                return abi::CLOSED;
            }
            // Asking first, then looking again: room that returned in between
            // has no later event to say so.
            exchange.want_writable.store(true, Ordering::Release);
            if let Ok(permit) = body.try_reserve() {
                exchange.want_writable.store(false, Ordering::Release);
                permit.send(Bytes::copy_from_slice(data.bytes()));
                return abi::OK;
            }
            abi::BUSY
        })
    }
}

pub(crate) unsafe extern "C" fn response_finish(request: *mut Request) -> u32 {
    unsafe {
        with(request, |exchange| {
            if exchange.closed.load(Ordering::Acquire) {
                return abi::CLOSED;
            }
            let mut reply = exchange.reply.lock().expect("reply");
            if reply.finished {
                return abi::INVALID_ARGUMENT;
            }
            let opened = exchange.send_head(&mut reply);
            reply.finished = true;
            // The body ends once what is queued has gone.
            reply.body = None;
            opened
        })
    }
}

pub(crate) unsafe extern "C" fn session_accept(
    request: *mut Request,
    max_message_bytes: u64,
) -> u32 {
    unsafe {
        with(request, |exchange| exchange.accept_session(max_message_bytes))
    }
}

pub(crate) unsafe extern "C" fn session_send(
    request: *mut Request,
    kind: u32,
    code: u32,
    data: Slice,
) -> u32 {
    unsafe {
        with(request, |exchange| {
            let bytes = data.bytes();
            let message = match kind {
                abi::MESSAGE_TEXT => match std::str::from_utf8(bytes) {
                    Ok(text) => Message::Text(text.to_owned()),
                    Err(_) => return abi::INVALID_ARGUMENT,
                },
                abi::MESSAGE_BINARY => Message::Binary(bytes.to_vec()),
                abi::MESSAGE_CLOSE => {
                    let Ok(code) = u16::try_from(code) else {
                        return abi::INVALID_ARGUMENT;
                    };
                    let reason = String::from_utf8_lossy(bytes).into_owned();
                    if !Message::sendable_close(code, &reason) {
                        return abi::INVALID_ARGUMENT;
                    }
                    Message::Close { code, reason }
                }
                _ => return abi::INVALID_ARGUMENT,
            };
            exchange.send_message(message)
        })
    }
}
