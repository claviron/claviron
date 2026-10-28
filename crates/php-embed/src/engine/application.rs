//! Persistent PHP workers. No PHP request startup/shutdown between messages;
//! adapters receive explicit HTTP data and own framework request-scope cleanup.

use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use bytes::Bytes;
use claviron_http::{RouteBody, RouteResponse, bytes_body};
use http::{Method, Request, Response, StatusCode};
use http_body_util::BodyExt;
use tokio::sync::{Notify, Semaphore, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::program::{PhpProgram, run};
use crate::event::{
    EventSubscription, PhpEventBridge, ReceivedEvent, RuntimeInput,
};
use crate::{PhpApplication, SapiGatewayError};

mod dispatch;
use dispatch::{Control, Mailbox, Waiting};

type Reply =
    oneshot::Sender<Result<Response<ApplicationBody>, SapiGatewayError>>;

/// An application's reply body: whole, from `claviron_application_respond`,
/// or piece by piece after `claviron_application_stream`, ended by
/// `claviron_application_end` (or the next `receive`). An error ends a stream
/// the worker could not finish, so it does not look whole.
pub(crate) enum ApplicationBody {
    Whole(Bytes),
    Stream(mpsc::Receiver<Result<Bytes, String>>),
}

/// Pieces of a streamed reply in flight to the client. Small, as SAPI's and
/// JavaScript's: a client that stops reading is felt at once as a write that
/// waits.
const REPLY_BODY_CAPACITY: usize = 2;

/// A streamed reply being written: where its pieces go and what bounds them.
struct Outgoing {
    sender: mpsc::Sender<Result<Bytes, String>>,
    written: usize,
    max_body_size: Option<usize>,
    write_timeout: Option<Duration>,
    runtime: tokio::runtime::Handle,
    stopping: CancellationToken,
}
pub(crate) struct Job {
    pub request: Request<Bytes>,
    pub reply: Reply,
    /// The route's reply ceiling for this request (`responses.max_body_size`).
    pub max_body_size: Option<usize>,
    /// The route's `responses.write_timeout`: how long a streamed write may
    /// wait for a client that is not reading.
    pub write_timeout: Option<Duration>,
    /// The runtime the PHP thread waits on for a full reply channel; the
    /// thread is not one of its own.
    pub runtime: tokio::runtime::Handle,
    /// The pool stopping, which ends such a wait.
    pub stopping: CancellationToken,
}

pub(crate) struct Inbox {
    rx: mpsc::Receiver<Job>,
    pub current: Option<Job>,
    /// The streamed reply still being written, if the current request chose
    /// to stream.
    outgoing: Option<Outgoing>,
    responded: bool,
    event_handled: bool,
    control: Arc<Control>,
    /// Budget for one request or event, already resolved against the host
    /// default.
    pub execution_timeout: Option<Duration>,
}

thread_local! {
    pub(crate) static INBOX: RefCell<Option<Inbox>> = const { RefCell::new(None) };
}

impl Inbox {
    pub fn receive(
        &mut self,
        mut events: Option<(&PhpEventBridge, &mut EventSubscription)>,
    ) -> Result<Option<ApplicationInput<'_>>, String> {
        // Coming back for work ends a stream the handler did not end itself.
        self.outgoing = None;
        if self.current.is_some() {
            return Err(
                "respond to the current application request before receiving another".into()
            );
        }
        if self.responded {
            self.control.complete(true);
            self.responded = false;
        }
        if self.event_handled {
            self.control.complete(false);
            self.event_handled = false;
        }
        self.control.publish_ready();
        loop {
            let input = match events.as_mut() {
                Some((bridge, subscription)) => {
                    match bridge.receive_with(
                        &mut self.rx,
                        subscription,
                        &self.control.cancel,
                    )? {
                        Some(input) => Some(input),
                        // Stop closes lifecycle-owned senders. A dispatcher that
                        // already claimed this context may still be publishing
                        // its accepted job; drain that mailbox before returning.
                        None => {
                            self.rx.blocking_recv().map(RuntimeInput::Value)
                        }
                    }
                }
                None => self.rx.blocking_recv().map(RuntimeInput::Value),
            };
            let Some(input) = input else { return Ok(None) };
            let job = match input {
                RuntimeInput::Value(job) => job,
                RuntimeInput::Event(event) => {
                    if self.control.claim() {
                        self.event_handled = true;
                        return Ok(Some(ApplicationInput::Event(event)));
                    }
                    // HTTP admission won the race immediately after select.
                    // Preserve the event and receive the already-reserved job.
                    if let Some((_, subscription)) = events.as_mut() {
                        (*subscription).defer(event);
                    }
                    let Some(job) = self.rx.blocking_recv() else {
                        return Ok(None);
                    };
                    job
                }
            };
            if job.reply.is_closed() {
                self.control.release_claim();
                continue;
            }
            self.current = Some(job);
            return Ok(self
                .current
                .as_ref()
                .map(|job| ApplicationInput::Request(&job.request)));
        }
    }

    /// The current request's reply ceiling, from its route.
    pub fn max_body_size(&self) -> Option<usize> {
        self.current.as_ref().and_then(|job| job.max_body_size)
    }

    pub fn respond(
        &mut self,
        response: Response<Bytes>,
    ) -> Result<(), String> {
        let job = self.current.take().ok_or("no active application request")?;
        let _ = job.reply.send(Ok(response.map(ApplicationBody::Whole)));
        // Stay reserved until PHP has finished its after-response/reset work
        // and explicitly asks for the next request.
        self.responded = true;
        Ok(())
    }

    /// Send the head of a reply whose body follows piece by piece.
    pub fn respond_head(
        &mut self,
        head: http::response::Parts,
    ) -> Result<(), String> {
        let job = self.current.take().ok_or("no active application request")?;
        let (sender, receiver) = mpsc::channel(REPLY_BODY_CAPACITY);
        let _ = job.reply.send(Ok(Response::from_parts(
            head,
            ApplicationBody::Stream(receiver),
        )));
        self.outgoing = Some(Outgoing {
            sender,
            written: 0,
            max_body_size: job.max_body_size,
            write_timeout: job.write_timeout,
            runtime: job.runtime,
            stopping: job.stopping,
        });
        self.responded = true;
        Ok(())
    }

    /// One piece of the streamed reply. Waits while the client is not
    /// reading, up to `write_timeout`; `Ok(false)` means the client is gone,
    /// the wait ran out or the pool is stopping, and the handler should stop.
    pub fn write(
        &mut self,
        chunk: &[u8],
    ) -> Result<bool, String> {
        let outgoing =
            self.outgoing.as_mut().ok_or("no streamed reply is open")?;
        outgoing.written = outgoing.written.saturating_add(chunk.len());
        if outgoing.max_body_size.is_some_and(|limit| outgoing.written > limit)
        {
            let _ = outgoing.sender.try_send(Err(
                "application reply exceeds responses.max_body_size".into(),
            ));
            self.outgoing = None;
            return Err(
                "application reply exceeds the route's responses.max_body_size"
                    .into(),
            );
        }
        let piece =
            match outgoing.sender.try_send(Ok(Bytes::copy_from_slice(chunk))) {
                Ok(()) => return Ok(true),
                Err(mpsc::error::TrySendError::Closed(_)) => return Ok(false),
                Err(mpsc::error::TrySendError::Full(piece)) => piece,
            };
        let Outgoing { sender, write_timeout, runtime, stopping, .. } =
            outgoing;
        let deadline = *write_timeout;
        Ok(runtime.block_on(async {
            tokio::select! {
                biased;
                _ = stopping.cancelled() => false,
                sent = sender.send(piece) => sent.is_ok(),
                _ = async {
                    match deadline {
                        Some(deadline) => tokio::time::sleep(deadline).await,
                        None => std::future::pending().await
                    }
                } => false,
            }
        }))
    }

    /// End the streamed reply.
    pub fn end(&mut self) {
        self.outgoing = None;
    }
}

pub(crate) enum ApplicationInput<'a> {
    Request(&'a Request<Bytes>),
    Event(ReceivedEvent),
}

impl Drop for Inbox {
    fn drop(&mut self) {
        // A stream the worker never ended is cut, not completed.
        if let Some(outgoing) = self.outgoing.take() {
            let _ = outgoing
                .sender
                .try_send(Err("application worker ended mid-reply".into()));
        }
        if let Some(job) = self.current.take() {
            // Two different endings arrive here. The application can exit on its
            // own — a fatal, an `exit()` — which is a bad gateway: something the
            // client asked for went wrong upstream. Or the slot can have been
            // retired underneath it, which is what a stop does, including a
            // drain that ran out of grace and interrupted the execution. That
            // one is 503: the server chose it, nothing is wrong with the
            // request, and a client or balancer may sensibly try elsewhere.
            // Reporting both as "application exited" sent whoever read the log
            // looking for a PHP error that never happened.
            let _ = job.reply.send(Err(if self.control.cancel.is_cancelled() {
                SapiGatewayError::ServiceUnavailable(
                    "application stopped while this request was in flight; it was not replayed"
                        .into()
                )
            } else {
                SapiGatewayError::BadGateway(
                    "application exited without a response; request was not replayed".into()
                )
            }));
        }
        self.control.exited();
    }
}

struct Slot {
    id: u8,
    tx: Option<mpsc::Sender<Job>>,
    control: Arc<Control>,
    handle: Option<std::thread::JoinHandle<Result<(), String>>>,
    born: Instant,
    retiring: bool,
    failures: u32,
    retry_at: Instant,
}

struct State {
    slots: Vec<Slot>,
    maintain_at: Instant,
}

/// One application in one serving process. All handles, including retired
/// threads, remain owned here until joined. Admission never uses Orbit locks.
pub(crate) struct ApplicationWorkers {
    definition: PhpApplication,
    state: Mutex<State>,
    dispatch: ArcSwap<Vec<Mailbox<Job>>>,
    accepting: AtomicBool,
    next: AtomicUsize,
    waiting: AtomicUsize,
    growth_requested: AtomicBool,
    maintenance: Arc<Notify>,
    wake: Arc<Notify>,
    admission: Semaphore,
    stopping: CancellationToken,
    /// Identity of this pool's executions. A drain that expires interrupts
    /// these and no others — not another application's, not a script request,
    /// not a task.
    owner: super::interrupt::ExecutionOwner,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct PhpApplicationSnapshot {
    pub accepting: bool,
    pub instances: usize,
    pub ready: usize,
    pub busy: usize,
    pub retiring: usize,
    pub backing_off: usize,
    pub exhausted: usize,
}

impl ApplicationWorkers {
    pub(crate) fn new(
        mut definition: PhpApplication,
        maintenance: Arc<Notify>,
    ) -> Result<Arc<Self>, String> {
        definition.validate()?;
        definition.script_path = std::fs::canonicalize(&definition.script_path)
            .map_err(|e| e.to_string())?;
        if !definition.script_path.is_file() {
            return Err("application script is not a file".into());
        }
        let admission = Semaphore::new(definition.pool.max_waiting);
        Ok(Arc::new(Self {
            definition,
            state: Mutex::new(State {
                slots: Vec::new(),
                maintain_at: Instant::now(),
            }),
            dispatch: ArcSwap::from_pointee(Vec::new()),
            accepting: AtomicBool::new(true),
            next: AtomicUsize::new(0),
            waiting: AtomicUsize::new(0),
            growth_requested: AtomicBool::new(false),
            maintenance,
            wake: Arc::new(Notify::new()),
            admission,
            stopping: CancellationToken::new(),
            owner: super::interrupt::ExecutionOwner::next(),
        }))
    }

    pub(crate) fn snapshot(&self) -> PhpApplicationSnapshot {
        let state = self.state.lock().unwrap();
        PhpApplicationSnapshot {
            accepting: self.accepting.load(Ordering::Acquire),
            instances: state
                .slots
                .iter()
                .filter(|s| s.handle.is_some())
                .count(),
            ready: state
                .slots
                .iter()
                .filter(|s| s.control.ready() && !s.retiring)
                .count(),
            busy: state.slots.iter().filter(|s| s.control.busy()).count(),
            retiring: state.slots.iter().filter(|s| s.retiring).count(),
            backing_off: state
                .slots
                .iter()
                .filter(|s| {
                    s.handle.is_none() && !s.retiring && !self.exhausted(s)
                })
                .count(),
            exhausted: state
                .slots
                .iter()
                .filter(|s| {
                    s.handle.is_none() && !s.retiring && self.exhausted(s)
                })
                .count(),
        }
    }

    fn fresh_slot(
        &self,
        id: u8,
    ) -> Slot {
        Slot {
            id,
            tx: None,
            control: Arc::new(Control::new(
                Arc::clone(&self.wake),
                Arc::clone(&self.maintenance),
            )),
            handle: None,
            born: Instant::now(),
            retiring: false,
            failures: 0,
            retry_at: Instant::now(),
        }
    }

    fn exhausted(
        &self,
        slot: &Slot,
    ) -> bool {
        self.definition.pool.retry.exhausted(slot.failures)
    }

    fn spawn(
        &self,
        slot: &mut Slot,
    ) {
        let (tx, rx) = mpsc::channel(1);
        let program = PhpProgram::new(
            self.definition.name.clone(),
            self.definition.script_path.clone(),
            self.definition.args.clone(),
            // Applications are reached by path, not started from a shell, so
            // they carry no working directory of their own yet.
            None,
            self.definition.environment.clone(),
            // Applications arm and disarm per request by hand in
            // `functions::application`; the program itself is unbounded.
            None,
        );
        slot.control = self.fresh_slot(slot.id).control;
        let control = Arc::clone(&slot.control);
        let owner = self.owner;
        let execution_timeout = crate::engine::interrupt::resolve(
            self.definition.pool.execution_timeout,
        );
        let id = slot.id;
        let handle = std::thread::Builder::new()
            .name(format!(
                "claviron-php-application-{}-{id}",
                self.definition.name
            ))
            .spawn(move || {
                struct InboxGuard;
                impl Drop for InboxGuard {
                    fn drop(&mut self) {
                        INBOX.with(|cell| {
                            cell.borrow_mut().take();
                        });
                    }
                }
                INBOX.with(|cell| {
                    *cell.borrow_mut() = Some(Inbox {
                        rx,
                        current: None,
                        outgoing: None,
                        responded: false,
                        event_handled: false,
                        control: Arc::clone(&control),
                        execution_timeout,
                    })
                });
                let _inbox = InboxGuard;
                let (started, _) = oneshot::channel();
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    run(
                        program,
                        super::ProgramModel::Application,
                        id,
                        control.cancel.clone(),
                        started,
                        owner,
                    )
                }))
                .unwrap_or_else(|_| {
                    Err("application PHP thread panicked".into())
                })
            });
        match handle {
            Ok(handle) => {
                slot.tx = Some(tx);
                slot.handle = Some(handle);
                slot.born = Instant::now();
            }
            Err(error) => {
                self.failed(slot, &error.to_string());
            }
        }
    }

    fn failed(
        &self,
        slot: &mut Slot,
        error: &str,
    ) {
        // Any completed request proves a useful generation; boot/crash loops
        // without progress retain exponential backoff across thread replacement.
        if slot.control.served.load(Ordering::Relaxed) > 0 {
            slot.failures = 0;
        }
        slot.failures = slot.failures.saturating_add(1);
        let delay = self.definition.pool.retry.delay_for(slot.failures);
        slot.retry_at = Instant::now() + delay;
        if self.exhausted(slot) {
            tracing::error!(
                application = %self.definition.name,
                instance = slot.id,
                attempts = slot.failures,
                %error,
                "application worker failed; retry budget exhausted"
            );
        } else {
            tracing::warn!(
                application = %self.definition.name,
                instance = slot.id,
                attempts = slot.failures,
                %error,
                ?delay,
                "application worker stopped; retry scheduled after join"
            );
        }
    }

    fn retire(slot: &mut Slot) {
        slot.retiring = true;
        slot.control.retire();
        slot.tx.take();
    }

    /// Lifecycle bookkeeping only. Request dispatch never takes this lock.
    pub(crate) fn maintain(&self) {
        let grow = self.growth_requested.swap(false, Ordering::AcqRel);
        let mut state = self.state.lock().unwrap();
        let scheduled = !self.accepting.load(Ordering::Acquire)
            || Instant::now() >= state.maintain_at;
        if !scheduled && !grow {
            return;
        }
        let mut changed = false;
        if scheduled {
            state.maintain_at =
                Instant::now() + self.definition.pool.maintenance_interval;
            for slot in &mut state.slots {
                if slot.handle.as_ref().is_some_and(|h| h.is_finished()) {
                    let result =
                        slot.handle.take().unwrap().join().unwrap_or_else(
                            |_| Err("PHP thread teardown panicked".into()),
                        );
                    slot.tx.take();
                    changed = true;
                    if !slot.retiring {
                        self.failed(
                            slot,
                            result
                                .as_ref()
                                .err()
                                .map(String::as_str)
                                .unwrap_or("application script returned"),
                        );
                    }
                }
            }
            state.slots.retain(|s| !(s.retiring && s.handle.is_none()));
        }
        if !self.accepting.load(Ordering::Acquire) {
            return;
        }
        let capacity = &self.definition.pool.capacity;
        if scheduled {
            let mut live = state.slots.iter().filter(|s| !s.retiring).count();
            let now = Instant::now();
            for slot in &mut state.slots {
                if slot.retiring {
                    continue;
                }
                if slot.handle.is_none() {
                    if now >= slot.retry_at && !self.exhausted(slot) {
                        self.spawn(slot);
                        changed = true;
                    }
                    continue;
                }
                let trim = live
                    > usize::from(capacity.max_idle.max(capacity.min_idle))
                    && slot.control.idle_for() >= capacity.idle_timeout;
                let rotate = capacity
                    .max_life
                    .is_some_and(|age| slot.born.elapsed() >= age)
                    || capacity.max_requests.is_some_and(|n| {
                        slot.control.served.load(Ordering::Relaxed) >= n
                    });
                if (trim || rotate)
                    && slot
                        .control
                        .retire_idle((!rotate).then_some(capacity.idle_timeout))
                {
                    Self::retire(slot);
                    live -= 1;
                    changed = true;
                }
            }
            while state.slots.len() < usize::from(capacity.min_idle) {
                self.add_slot(&mut state);
                changed = true;
            }
        }
        if grow {
            // Starting contexts and idle contexts can satisfy these waiters.
            // Coalesced demand must not start another thread for each wake.
            let available = state
                .slots
                .iter()
                .filter(|s| {
                    !s.retiring
                        && s.handle.is_some()
                        && s.control.starting_or_idle()
                })
                .count();
            let needed =
                self.waiting.load(Ordering::Acquire).saturating_sub(available);
            let room = usize::from(capacity.max_size)
                .saturating_sub(state.slots.len());
            for _ in 0..needed.min(room) {
                self.add_slot(&mut state);
                changed = true;
            }
        }
        if changed && self.accepting.load(Ordering::Acquire) {
            let mailboxes = state
                .slots
                .iter()
                .filter(|s| !s.retiring)
                .filter_map(|s| {
                    s.tx.as_ref()
                        .map(|tx| Mailbox::new(tx, Arc::clone(&s.control)))
                })
                .collect();
            self.dispatch.store(Arc::new(mailboxes));
            self.wake.notify_waiters();
        }
    }

    fn add_slot(
        &self,
        state: &mut State,
    ) {
        let id = (0..self.definition.pool.capacity.max_size)
            .find(|id| state.slots.iter().all(|s| s.id != *id))
            .expect("capacity checked before adding application slot");
        let mut slot = self.fresh_slot(id);
        self.spawn(&mut slot);
        state.slots.push(slot);
    }

    /// This pool's execution identity, for its owner's drain.
    pub(crate) fn owner(&self) -> super::interrupt::ExecutionOwner {
        self.owner
    }

    pub(crate) fn stop(&self) {
        if !self.accepting.swap(false, Ordering::AcqRel) {
            return;
        }
        self.stopping.cancel();
        self.admission.close();
        self.dispatch.store(Arc::new(Vec::new()));
        let mut state = self.state.lock().unwrap();
        for slot in &mut state.slots {
            Self::retire(slot);
        }
        self.wake.notify_waiters();
    }

    pub(crate) fn drained(&self) -> bool {
        self.maintain();
        self.state.lock().unwrap().slots.is_empty()
            && self.admission.available_permits()
                == self.definition.pool.max_waiting
    }

    /// Snapshot guards and transient strong senders end before any await.
    fn try_dispatch(
        &self,
        job: &mut Option<Job>,
    ) -> bool {
        let slots = self.dispatch.load();
        dispatch::send_available(&slots, &self.next, job)
    }

    pub(crate) fn accepting(&self) -> bool {
        self.accepting.load(Ordering::Acquire)
    }

    pub(crate) async fn execute(
        &self,
        req: Request<RouteBody>,
    ) -> Result<RouteResponse, SapiGatewayError> {
        let _permit = self.admission.try_acquire().map_err(|_| {
            SapiGatewayError::ServiceUnavailable(
                "application admission is full or closed".into(),
            )
        })?;
        let (mut parts, mut body) = req.into_parts();
        // What the route decides for this request — the wait for an instance
        // and the reply's ceiling — carried from the adapter; running limits
        // are this application's own.
        let policy = parts
            .extensions
            .get::<claviron_http::policy::HttpPolicy>()
            .copied()
            .ok_or_else(|| {
                SapiGatewayError::Internal(
                    "HttpPolicy is required for PHP application execution"
                        .into(),
                )
            })?;
        if parts.method == Method::CONNECT
            || parts.headers.contains_key(http::header::UPGRADE)
        {
            return Err(SapiGatewayError::NotImplemented);
        }
        claviron_http::forwarding::reject_request_smuggling(
            parts.version,
            &parts.headers,
        )
        .map_err(|e| SapiGatewayError::BadRequest(e.to_string()))?;
        let head = parts.method == Method::HEAD;
        // Validate the effective route override in the existing input path;
        // the transport already owns its body guard. None is explicit off.
        if claviron_http::body::declared_length_within(
            &parts.headers,
            policy.requests.max_body_size,
        )
        .is_err()
        {
            let mut response = Response::new(claviron_http::empty_body());
            *response.status_mut() = StatusCode::PAYLOAD_TOO_LARGE;
            return Ok(response);
        }
        let mut buffered = bytes::BytesMut::new();
        loop {
            let frame = tokio::select! {
                biased;
                _ = self.stopping.cancelled() => return Err(SapiGatewayError::ServiceUnavailable("application is stopped".into())),
                frame = body.frame() => frame,
            };
            let Some(frame) = frame else {
                break;
            };
            let frame = match frame {
                Ok(frame) => frame,
                Err(error)
                    if matches!(
                        error.downcast_ref::<claviron_http::body::BodyLimit>(),
                        Some(claviron_http::body::BodyLimit::TooLarge { .. })
                    ) =>
                {
                    let mut response =
                        Response::new(claviron_http::empty_body());
                    *response.status_mut() = StatusCode::PAYLOAD_TOO_LARGE;
                    return Ok(response);
                }
                Err(error) => {
                    return Err(SapiGatewayError::BadRequest(format!(
                        "application body rejected: {error}"
                    )));
                }
            };
            if let Ok(data) = frame.into_data() {
                if let Some(limit) = policy.requests.max_body_size
                    && buffered.len().saturating_add(data.len()) > limit
                {
                    let mut response =
                        Response::new(claviron_http::empty_body());
                    *response.status_mut() = StatusCode::PAYLOAD_TOO_LARGE;
                    return Ok(response);
                }
                buffered.extend_from_slice(&data);
            } else {
                return Err(SapiGatewayError::BadRequest(
                    "application request trailers are not supported".into(),
                ));
            }
        }
        let bytes = buffered.freeze();
        if parts
            .headers
            .get(http::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse::<u64>().ok())
            .is_some_and(|declared| declared != bytes.len() as u64)
        {
            return Err(SapiGatewayError::BadRequest(
                "application body length differs from Content-Length".into(),
            ));
        }
        claviron_http::forwarding::strip_hop_by_hop_headers(&mut parts.headers);
        let request = Request::from_parts(parts, bytes);
        let (reply, result) = oneshot::channel();
        let mut job = Some(Job {
            request,
            reply,
            max_body_size: policy.responses.max_body_size,
            write_timeout: policy.responses.write_timeout,
            runtime: tokio::runtime::Handle::current(),
            stopping: self.stopping.clone(),
        });
        // Unset waits for an instance; stopping ends the wait either way.
        let deadline = policy
            .requests
            .queue_timeout
            .map(|timeout| tokio::time::Instant::now() + timeout);
        let mut waiting = None;
        loop {
            let notified = self.wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !self.accepting.load(Ordering::Acquire) {
                return Err(SapiGatewayError::ServiceUnavailable(
                    "application is stopped".into(),
                ));
            }
            if self.try_dispatch(&mut job) {
                break;
            }
            waiting.get_or_insert_with(|| Waiting::enter(&self.waiting));
            if self.dispatch.load().len()
                < usize::from(self.definition.pool.capacity.max_size)
                && !self.growth_requested.swap(true, Ordering::AcqRel)
            {
                self.maintenance.notify_one();
            }
            let expired = tokio::select! {
                _ = notified => false,
                _ = self.stopping.cancelled() => false,
                _ = async {
                    match deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending().await
                    }
                } => true,
            };
            if expired {
                return Err(SapiGatewayError::ServiceUnavailable(
                    "application has no ready idle worker".into(),
                ));
            }
        }
        drop(waiting);
        let response = result.await.map_err(|_| {
            SapiGatewayError::BadGateway(
                "application worker disappeared; request was not replayed"
                    .into(),
            )
        })??;
        let (mut parts, body) = response.into_parts();
        claviron_http::forwarding::strip_hop_by_hop_headers(&mut parts.headers);
        let mut body = match body {
            ApplicationBody::Whole(body) => body,
            ApplicationBody::Stream(receiver) => {
                // Pieces as the script writes them, held instead only as the
                // reply's head and its policy say — the rule JavaScript and
                // the edge follow.
                let stream = if head {
                    claviron_http::empty_body()
                } else {
                    claviron_http::boxed_body(StreamedReply(receiver))
                };
                let body = claviron_http::body::deliver_reply(
                    &mut parts.headers,
                    stream,
                    &policy.responses,
                )
                .await;
                return Ok(Response::from_parts(parts, body));
            }
        };
        parts.headers.remove(http::header::CONTENT_LENGTH);
        if parts.status == StatusCode::NO_CONTENT
            || parts.status == StatusCode::NOT_MODIFIED
            || parts.status == StatusCode::RESET_CONTENT
        {
            body = Bytes::new();
        } else {
            parts
                .headers
                .insert(http::header::CONTENT_LENGTH, body.len().into());
            if head {
                body = Bytes::new();
            }
        }
        Ok(Response::from_parts(parts, bytes_body(body)))
    }
}

impl Drop for ApplicationWorkers {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
#[path = "../../tests/application.rs"]
mod tests;

/// A streamed reply as the script writes it. An error from the worker ends
/// the body as failed, so a cut reply never looks whole.
struct StreamedReply(mpsc::Receiver<Result<Bytes, String>>);

#[derive(Debug)]
struct ReplyCut(String);

impl std::fmt::Display for ReplyCut {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ReplyCut {}

impl http_body::Body for StreamedReply {
    type Data = Bytes;
    type Error = ReplyCut;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Bytes>, ReplyCut>>>
    {
        self.0.poll_recv(context).map(|piece| {
            piece.map(|piece| {
                piece.map(http_body::Frame::data).map_err(ReplyCut)
            })
        })
    }
}
