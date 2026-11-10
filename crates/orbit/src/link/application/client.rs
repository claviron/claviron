//! The calling side: reach an application by name and wait for its answer.
//!
//! One of these per edge worker. It takes a lane like any other participant —
//! replies come back to it — keeps a correlation table of the exchanges it is
//! waiting on, and hands each reply to the request that is waiting for it.
//!
//! ## Why a table and not a lane per request
//!
//! A lane is a mailbox, and this worker has one. Every reply it is owed
//! arrives in it, in whatever order the applications finish, so the frame
//! carries the number that says which request it belongs to. The edge minted
//! that number and nobody else interprets it.
//!
//! ## What is retained, and what is not
//!
//! Stream slots are retained. A slot claimed and given back per exchange pays two
//! compare-and-swaps, a reset, and a ring nobody has touched since it was
//! last used — cold in cache, absent from the TLB — and that is a fixed cost
//! an exchange carries before a byte moves. So a caller keeps the slots it
//! built and re-arms them instead.
//!
//! A kept slot is not a connection and is not tied to the process at the other
//! end. The application opens its side for one exchange and lets it go, and
//! re-arming hands that side to whichever process the next exchange is
//! addressed to. The caller retains these slots in its internal hot-slot cache.
//!
//! Nothing else is. No health probe, no warm floor, no request replay: there is no
//! acknowledgement to retry against, and a request that was written was
//! delivered while one that was not is a 503 before anything ran.
//!
//! No timeout: a reply head that cannot be published is signalled by RESET on
//! the exchange's existing response stream. Confirmed worker death uses that
//! stream's recovery too; elapsed time is not a substitute for either signal.
//!
//! Nobody has to report a death for that to happen. An application may have no
//! supervisor, and a dead one writes nothing, so each application this client
//! reaches is watched through the kernel's exit notification. The notification
//! only says when to look: the lane's lock remains the proof.

use std::collections::{HashMap, VecDeque};
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use bytes::Bytes;
use claviron_runtime::{FleetName, ProcessEventBus};
use orbitive::link::Received;
use orbitive::stream::{Endpoint, ReadHalf, ResetSignal, Ticket, WriteHalf};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::oneshot;

use super::creator::{CreatorAdmission, CreatorTurn};

use super::{
    Access, ApplicationBodies, ApplicationConnected, ApplicationDisconnected,
    ApplicationLinkSetup, ApplicationSegment, ApplicationStateNotice,
    ControlCommand, Error, Exchange, ExchangeBody, ExchangeReply, FrameKind,
    LinkJoined, Peers, Result, fit_inline, frame_kind,
};

/// The runtime tag an edge worker's lane carries. It serves no name — its lane
/// only takes replies — but a lane says what holds it, so `--lanes` reads the
/// same for every participant.
pub const EDGE_RUNTIME: &str = "edge";

/// An exchange that has been sent and not yet answered.
///
/// The three pieces are separate because they happen at once. The request body
/// is still being written while the reply is being waited for, and the
/// response body may start arriving before the request body has finished —
/// that is what answers a large upload with 413 without reading it, and what
/// lets a streamed response reach a client as it is produced.
pub struct Pending {
    pub correlation: u64,
    /// Where the request body goes. Returns its stream to the hot-slot cache
    /// when the caller drops it.
    pub request_body: LentWrite,
    /// Where the response body comes from, when the reply says it streams.
    pub response_body: LentRead,
    /// What of a whole request body is still to be written to
    /// [`Self::request_body`], then ended: present only when the body did not
    /// fit the frame and more of it remained than the stream's window took
    /// before the frame went.
    pub unsent: Option<Bytes>,
    reply: ReplyWait,
}

impl Pending {
    /// Take the three apart, because the caller drives them at the same time.
    ///
    /// The request body is written while the reply is being waited for, and
    /// the response body may start arriving before the request body is
    /// finished. Holding them together would make that a borrow problem
    /// rather than the ordinary case it is.
    pub fn split(self) -> (LentWrite, LentRead, ReplyWait, Option<Bytes>) {
        (self.request_body, self.response_body, self.reply, self.unsent)
    }
}

/// The head of a reply, once the application has one.
pub struct ReplyWait {
    reply: oneshot::Receiver<ExchangeReply>,
    reset: ResetSignal,
    waiting: Arc<Waiting>,
    correlation: u64,
    completed: bool,
}

impl ReplyWait {
    /// Wait for the status and headers.
    ///
    /// No deadline: a failed publication resets the existing response stream,
    /// waking this wait even when no reply head could enter the shared inbox.
    pub async fn head(mut self) -> Result<ExchangeReply> {
        let result = poll_fn(|cx| {
            // A successfully delivered head wins over a later body reset.
            if let Poll::Ready(reply) = Pin::new(&mut self.reply).poll(cx) {
                return Poll::Ready(reply.map_err(|_| {
                    Error::Malformed(
                        "the reply channel closed before an answer arrived",
                    )
                }));
            }
            match self.reset.poll_reset(cx) {
                Poll::Ready(Ok(())) => Poll::Ready(Err(Error::Stream(
                    orbitive::stream::Error::Reset,
                ))),
                Poll::Ready(Err(error)) => {
                    Poll::Ready(Err(Error::Stream(error)))
                }
                Poll::Pending => Poll::Pending,
            }
        })
        .await;
        if result.is_err() {
            self.waiting.take(self.correlation);
        }
        self.completed = true;
        result
    }
}

impl Drop for ReplyWait {
    fn drop(&mut self) {
        if !self.completed {
            self.waiting.take(self.correlation);
        }
    }
}

/// The streams this worker built and keeps between exchanges.
///
/// One list for the worker, not one per peer. The slot is this worker's —
/// it came from its own table — and the application's side is opened for one
/// exchange and let go at the end of it, so a kept slot can carry the next
/// exchange to any process serving any name. Pools kept per peer did not
/// survive four processes under one name: each grew to its own peer's peak,
/// the peaks did not fall together, and three peers sat on twenty to thirty
/// idle slots each while the fourth, in a burst, could not build one.
///
/// Unbounded here because it is bounded already: nothing is kept that was not
/// built, and building is bounded by the table. After a burst the slots stay,
/// which is the point — they are this worker's seats and nobody else's.
#[derive(Default)]
struct Hot {
    slots: Mutex<VecDeque<(Endpoint, Ticket)>>,
    returned: tokio::sync::Notify,
}

impl Hot {
    /// Inspect without re-arming. Only the creator turn owner calls this.
    fn available(
        &self,
        bodies: &ApplicationBodies,
    ) -> bool {
        let mut slots =
            self.slots.lock().expect("the hot-slot mutex is never poisoned");
        let mut at = 0;
        while at < slots.len() {
            match bodies.streams().can_rearm(slots[at].1.id) {
                Ok(true) => return true,
                Ok(false) => at += 1,
                Err(_) => {
                    // A never-opened or stale endpoint cannot be reused. Drop
                    // it as take() does, making its physical slot available.
                    slots.remove(at);
                }
            }
        }
        false
    }

    /// A kept slot re-armed for the next exchange, if one is ready.
    ///
    /// Coming home is not the same as being free. This side stops reading a
    /// reply the moment the response is complete — a `content-length` says how
    /// long it is, so nothing waits for the stream to close — while the
    /// application still holds its side for a few more instructions. Whether
    /// it has let go is a state in the table, and re-arming is what reads it:
    /// a slot still held is passed over and offered again next time, and one
    /// that cannot be re-armed at all is dropped, which empties it.
    ///
    /// Oldest first, since the slot that came home longest ago is the one most
    /// likely to have been let go.
    fn take(
        &self,
        bodies: &ApplicationBodies,
    ) -> Option<(Endpoint, Ticket)> {
        let mut slots =
            self.slots.lock().expect("the hot-slot mutex is never poisoned");
        let mut at = 0;
        while at < slots.len() {
            match bodies.rearm(&mut slots[at].0) {
                Ok(()) => return slots.remove(at),
                Err(Error::Stream(orbitive::stream::Error::WouldBlock)) => {
                    at += 1
                }
                Err(_) => {
                    slots.remove(at);
                }
            }
        }
        None
    }
}

/// One stream lent out for an exchange, and what it needs to find its way
/// home.
///
/// The two halves go to different places — the request body is written while
/// the response body is streamed to the client — so neither can return the
/// slot alone. Each puts itself here as it is dropped, and the second one to
/// arrive rejoins them and puts the slot back.
struct Loan {
    hot: Arc<Hot>,
    ticket: Ticket,
    halves: Mutex<(Option<ReadHalf>, Option<WriteHalf>)>,
}

impl Loan {
    fn give_back(&self) {
        let rejoined = {
            let mut halves =
                self.halves.lock().expect("the loan mutex is never poisoned");
            match (halves.0.take(), halves.1.take()) {
                (Some(read), Some(write)) => Endpoint::rejoin(read, write).ok(),
                // The other half is still out; it will find this one here.
                (read, write) => {
                    halves.0 = read;
                    halves.1 = write;
                    None
                }
            }
        };
        let Some(endpoint) = rejoined else { return };

        self.hot
            .slots
            .lock()
            .expect("the hot-slot mutex is never poisoned")
            .push_back((endpoint, self.ticket));
        self.hot.returned.notify_waiters();
    }
}

/// The writing half of a lent stream: an `AsyncWrite` that returns the slot
/// when the caller is done with it.
pub struct LentWrite {
    half: Option<WriteHalf>,
    loan: Arc<Loan>,
}

/// The reading half of a lent stream.
pub struct LentRead {
    half: Option<ReadHalf>,
    loan: Arc<Loan>,
}

impl LentWrite {
    /// Abandon this direction: the reader gets an error instead of an end.
    /// How a body cut short — too large, stalled, its client gone — is told
    /// from one that is whole; ending it with a shutdown would hand the
    /// application half a body as if it were all of it.
    pub fn reset(&mut self) {
        if let Some(half) = self.half.as_mut() {
            half.reset();
        }
    }
}

impl Drop for LentWrite {
    fn drop(&mut self) {
        if let Some(half) = self.half.take() {
            self.loan
                .halves
                .lock()
                .expect("the loan mutex is never poisoned")
                .1 = Some(half);
        }
        self.loan.give_back();
    }
}

impl Drop for LentRead {
    fn drop(&mut self) {
        if let Some(half) = self.half.take() {
            // Cancel response work immediately, even while the upload still
            // owns its half. Pooling the endpoint must not delay this signal.
            half.stop_reading();
            self.loan
                .halves
                .lock()
                .expect("the loan mutex is never poisoned")
                .0 = Some(half);
        }
        self.loan.give_back();
    }
}

impl AsyncWrite for LentWrite {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.half.as_mut() {
            Some(half) => Pin::new(half).poll_write(cx, buf),
            None => Poll::Ready(Ok(0)),
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.half.as_mut() {
            Some(half) => Pin::new(half).poll_flush(cx),
            None => Poll::Ready(Ok(())),
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.half.as_mut() {
            Some(half) => Pin::new(half).poll_shutdown(cx),
            None => Poll::Ready(Ok(())),
        }
    }
}

impl AsyncRead for LentRead {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.half.as_mut() {
            Some(half) => Pin::new(half).poll_read(cx, buf),
            None => Poll::Ready(Ok(())),
        }
    }
}

/// This worker's place in one destination fleet, as a caller.
pub struct ApplicationClient {
    fleet: FleetName,
    segment: Arc<ApplicationSegment>,
    bodies: Arc<ApplicationBodies>,
    lane: usize,
    /// The lane's lock, let go in [`ApplicationClient::leave`] after the lane.
    hold: Mutex<Option<orbitive::shm::LaneHold>>,
    correlation: AtomicU64,
    /// Advances the two random choices made for unbounded receivers.
    rotation: AtomicU64,
    directory: Mutex<Directory>,
    creator: CreatorAdmission,
    hot: Arc<Hot>,
    waiting: Arc<Waiting>,
    reader: Mutex<Option<tokio::task::JoinHandle<()>>>,
    last_seen: Arc<Mutex<HashMap<String, String>>>,
    application_states: Arc<Mutex<HashMap<usize, ApplicationBatch>>>,
}

#[derive(Default)]
struct ApplicationBatch {
    completed: Option<CompletedApplicationBatch>,
    pending: Option<PendingApplicationBatch>,
}

/// A complete snapshot from one exact runner lane incarnation.
struct CompletedApplicationBatch {
    incarnation: u64,
    pid: Option<u32>,
    batch: u64,
    rows: Vec<Vec<u8>>,
}

struct PendingApplicationBatch {
    incarnation: u64,
    pid: Option<u32>,
    batch: u64,
    rows: Vec<Option<Vec<u8>>>,
}

/// Which lanes serve a name, kept until the inbox says otherwise.
///
/// Resolving a name means walking every lane and comparing bytes, and the
/// answer changes only when a process joins or leaves. The inbox counts those
/// events, so this holds the count it last resolved under and throws the
/// whole map away when the count moves — an application that restarts costs
/// one rescan, not one per request.
///
/// Depth is deliberately not cached. It is the load signal, it changes with
/// every frame, and reading it is the point of choosing at all.
#[derive(Default)]
struct Directory {
    generation: u64,
    lanes: HashMap<Box<str>, Arc<[usize]>>,
}

impl Directory {
    fn resolve(
        this: &Mutex<Self>,
        inbox: &super::Inbox,
        application: &str,
    ) -> Arc<[usize]> {
        let generation = inbox.directory();
        let mut directory =
            this.lock().expect("the lane directory mutex is never poisoned");
        if directory.generation != generation {
            directory.lanes.clear();
            directory.generation = generation;
        }
        if let Some(lanes) = directory.lanes.get(application) {
            return lanes.clone();
        }
        let lanes: Arc<[usize]> = inbox.serving(application).into();
        directory.lanes.insert(application.into(), lanes.clone());
        lanes
    }
}

/// The exchanges this worker is waiting on.
///
/// A plain map behind a mutex: it is touched twice per exchange, for a few
/// nanoseconds each time, and a reply's first act is to take its sender out of
/// it. Nothing iterates it on the request path.
#[derive(Default)]
struct Waiting {
    capacity_changed: tokio::sync::Notify,
    membership_changed: tokio::sync::Notify,
    closed: AtomicBool,
    senders: Mutex<HashMap<u64, (LaneOwner, oneshot::Sender<ExchangeReply>)>>,
}

/// Who a request was written to. A lane is reused, and a standalone process
/// restarts with the same incarnation, so it takes all three to say which
/// process will never answer.
#[derive(Clone, Copy, PartialEq, Eq)]
struct LaneOwner {
    lane: usize,
    pid: Option<u32>,
    incarnation: u64,
}

impl Waiting {
    fn insert(
        &self,
        correlation: u64,
        owner: LaneOwner,
        sender: oneshot::Sender<ExchangeReply>,
    ) {
        self.senders
            .lock()
            .expect("waiting exchanges")
            .insert(correlation, (owner, sender));
    }

    fn take(
        &self,
        correlation: u64,
    ) -> Option<oneshot::Sender<ExchangeReply>> {
        self.senders
            .lock()
            .expect("waiting exchanges")
            .remove(&correlation)
            .map(|(_, sender)| sender)
    }

    /// End what this process will never answer. A request it had not read yet
    /// holds no stream side of its own, so ending its streams cannot reach it.
    fn abandon(
        &self,
        gone: LaneOwner,
    ) {
        self.senders
            .lock()
            .expect("waiting exchanges")
            .retain(|_, (owner, _)| *owner != gone);
    }
}

impl ApplicationClient {
    /// Bind an application name once, before requests begin to use it.
    pub fn target(
        self: &Arc<Self>,
        application: impl Into<Arc<str>>,
    ) -> super::ApplicationTarget {
        super::ApplicationTarget::new(Arc::clone(self), application.into())
    }

    pub(super) fn stream_buffer_bytes(&self) -> usize {
        self.segment.spec().streams.buffer_bytes
    }

    pub(super) fn inbox(&self) -> &super::Inbox {
        self.segment.inbox()
    }

    /// Current application lanes in this joined fleet, read from Link.
    pub fn applications(&self) -> Vec<super::Peer> {
        let mut peers = Peers::new(self.fleet, |role| role != EDGE_RUNTIME);
        peers.update(self.segment.inbox()).0
    }

    /// Identities this edge worker actually observed before a Link departure.
    /// They remain identifiable, but have no currently connected lane.
    pub fn known_applications(&self) -> Vec<(String, String)> {
        self.last_seen
            .lock()
            .expect("last seen Link peers")
            .iter()
            .map(|(name, runtime)| (name.clone(), runtime.clone()))
            .collect()
    }

    /// Last complete application snapshot from this exact live lane incarnation.
    pub fn application_state(
        &self,
        peer: &super::Peer,
    ) -> Option<Vec<Vec<u8>>> {
        let state = self
            .application_states
            .lock()
            .expect("application state lock")
            .get(&peer.lane)
            .and_then(|batch| batch.completed.as_ref())
            .filter(|completed| {
                completed.incarnation == peer.incarnation
                    && completed.pid == peer.pid
            })
            .map(|completed| completed.rows.clone());
        if state.is_none() {
            request_application_state(&self.segment, peer, self.lane);
        }
        state
    }

    /// Forward one command through the same inbox used by HTTP requests.
    pub fn command(
        &self,
        command: &super::ControlCommand,
    ) -> Result<()> {
        self.segment.control(&command.application, command)
    }

    /// Join `fleet_name` as a caller. `send` selects an application inside it.
    ///
    /// `name` is what this worker's lane advertises. It is not an application
    /// name and nothing routes to it — the edge is the one participant that
    /// only ever receives replies — but it is what an operator reading the
    /// table sees, so it should say which process this is.
    pub fn join(
        fleet_name: &str,
        name: &str,
        incarnation: u64,
    ) -> Result<Arc<Self>> {
        Self::join_observed(fleet_name, name, incarnation, None, None)
    }

    /// As [`Self::join`], and report application processes coming and going
    /// in this fleet on `events`, the process's local bus: read from the
    /// lane directory each time it moves, never published to the fleet.
    /// With `link`, application events also cross between this edge's fleet
    /// and the destination fleet over this lane.
    pub fn join_observed(
        fleet_name: &str,
        name: &str,
        incarnation: u64,
        events: Option<ProcessEventBus>,
        link: Option<ApplicationLinkSetup>,
    ) -> Result<Arc<Self>> {
        Self::join_observed_with_access(
            fleet_name,
            name,
            incarnation,
            events,
            link,
            &Access::OwnerOnly,
        )
    }

    pub fn join_with_access(
        fleet_name: &str,
        name: &str,
        incarnation: u64,
        access: &Access,
    ) -> Result<Arc<Self>> {
        Self::join_observed_with_access(
            fleet_name,
            name,
            incarnation,
            None,
            None,
            access,
        )
    }

    pub fn join_observed_with_access(
        fleet_name: &str,
        name: &str,
        incarnation: u64,
        events: Option<ProcessEventBus>,
        link: Option<ApplicationLinkSetup>,
        access: &Access,
    ) -> Result<Arc<Self>> {
        let segment = Arc::new(ApplicationSegment::for_fleet_with_access(
            fleet_name, access,
        )?);
        Self::join_segment(segment, name, incarnation, events, link)
    }

    pub fn join_segment(
        segment: Arc<ApplicationSegment>,
        name: &str,
        incarnation: u64,
        events: Option<ProcessEventBus>,
        link: Option<ApplicationLinkSetup>,
    ) -> Result<Arc<Self>> {
        let fleet_name = segment.fleet_name.clone();
        let joined = segment.join(name, EDGE_RUNTIME, incarnation)?;
        let lane = joined.lane;
        let bodies = Arc::new(ApplicationBodies::for_segment(
            &segment,
            lane,
            incarnation,
        )?);
        bodies.reclaimed(&joined.reclaimed);

        let client = Arc::new(Self {
            fleet: FleetName::new(&fleet_name),
            segment,
            bodies,
            lane,
            hold: Mutex::new(Some(joined.hold)),
            correlation: AtomicU64::new(1),
            // Seeded with this caller's own lane, not with zero, so callers
            // that start together do not draw the same receivers in lockstep.
            rotation: AtomicU64::new(lane as u64),
            directory: Mutex::default(),
            creator: CreatorAdmission::default(),
            hot: Arc::default(),
            waiting: Arc::new(Waiting::default()),
            reader: Mutex::new(None),
            last_seen: Arc::new(Mutex::new(HashMap::new())),
            application_states: Arc::new(Mutex::new(HashMap::new())),
        });
        let reader = client.spawn_reader(name, events, link)?;
        *client.reader.lock().expect("reader handle") = Some(reader);
        Ok(client)
    }

    /// The lane this worker reads, and the address a reply is sent to.
    pub fn lane(&self) -> usize {
        self.lane
    }

    /// Whether anything serves this application right now.
    ///
    /// Read, not subscribed to: a worker that joined a moment ago is already
    /// here and one that left is already gone.
    pub fn serves(
        &self,
        application: &str,
    ) -> bool {
        let inbox = self.segment.inbox();
        !inbox
            .ready(&Directory::resolve(&self.directory, inbox, application))
            .is_empty()
    }

    /// Send one exchange.
    ///
    /// The target is chosen by reading queue depths — not a guess, and not a
    /// number anybody reported. Everything that can go wrong before the
    /// application has the request goes wrong here, so the caller answers 503
    /// rather than waiting for a timeout that would mean something else.
    ///
    /// It returns as soon as the frame is in the target's mailbox. The bodies
    /// and the reply are the caller's to drive from there.
    ///
    /// The request body travels as the exchange says. Empty or inline, its
    /// direction is ended here, before the frame that announces it: nobody has
    /// opened the far side yet, so ending it rings no one, and the application
    /// never parks for a body it already has. An inline body too large for the
    /// frame goes into the stream instead, as much as its window takes before
    /// the frame is written — the far side finds it waiting — and the rest is
    /// [`Pending::unsent`].
    pub fn send(
        &self,
        application: &str,
        mut request: Exchange,
    ) -> Result<Pending> {
        let lanes =
            Directory::resolve(&self.directory, self.inbox(), application);
        self.send_to(application, &lanes, &mut request)
    }

    pub(super) fn send_to(
        &self,
        application: &str,
        lanes: &[usize],
        request: &mut Exchange,
    ) -> Result<Pending> {
        self.check_open(application)?;
        if self.creator.contended() {
            return Err(Error::Stream(orbitive::stream::Error::WouldBlock));
        }
        self.send_acquiring(application, lanes, request, ())
    }

    pub(super) fn send_with_creator_turn(
        &self,
        application: &str,
        lanes: &[usize],
        request: &mut Exchange,
        turn: CreatorTurn<'_>,
    ) -> Result<Pending> {
        self.check_open(application)?;
        self.send_acquiring(application, lanes, request, turn)
    }

    pub(super) fn check_open(
        &self,
        application: &str,
    ) -> Result<()> {
        if self.waiting.closed.load(Ordering::Acquire) {
            return Err(Error::NoConsumer(application.to_owned()));
        }
        Ok(())
    }

    pub(super) async fn creator_turn(&self) -> CreatorTurn<'_> {
        self.creator.acquire().await
    }

    pub(super) fn creator_available(&self) -> bool {
        self.hot.available(&self.bodies) || self.bodies.streams().can_create()
    }

    /// Whether a lane can take work at all, and its depth if so.
    fn candidate(
        &self,
        lane: usize,
    ) -> Option<u64> {
        // A bounded target may have every slot reserved and every published
        // frame still unread. Keep it in the candidate set: inbox fullness
        // must not turn execution saturation into "no consumer".
        let admission = self.inbox().admits(lane).ok()?;
        let bounded = self.inbox().work_capacity(lane).ok()?.is_some();
        if admission == super::Admission::Accepted
            || (bounded && admission == super::Admission::Full)
        {
            self.inbox().depth(lane).ok()
        } else {
            None
        }
    }

    /// Try one receiver; `None` when it refused or is gone.
    fn try_reserve(
        &self,
        target: usize,
        full: &mut bool,
    ) -> Result<Option<Chosen>> {
        match self.segment.reserve_work(target, self.lane) {
            Ok(admission) => Ok(Some((target, admission))),
            Err(Error::CapacityFull) => {
                *full = true;
                Ok(None)
            }
            Err(Error::ReceiverGone) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Which receiver takes this exchange.
    ///
    /// ## Keep receivers busy rather than spread
    ///
    /// A receiver that gets a thin share of the work drains its lane, parks,
    /// and has to be rung for the next exchange: a bell, a reactor wake and an
    /// unpark on both sides, every time. Spreading consecutive exchanges evenly
    /// over every receiver maximises exactly that. So a bounded receiver is
    /// filled up to its declared admission before the next one gets work — the
    /// upstream balancer's `first`, HAProxy's `balance first` — and each sender
    /// starts from its own receiver, chosen by its lane, so senders fill
    /// different receivers instead of piling onto one. Measured on a 32 × 32
    /// two-socket host, this cut serving CPU per exchange by 15–24% and moved
    /// the accepted load up by a third against an even spread.
    ///
    /// The receiver's bound is what makes that safe: concentration stops where
    /// the receiver said it would. A receiver that declares no bound gives no
    /// stopping point, so unbounded receivers get two random choices instead,
    /// the shallower first — constant cost, without reading every lane.
    ///
    /// Either way, when neither yields a reservation, every lane is tried once
    /// in the same order, so a full fleet still reads as full and an empty one
    /// as having no consumer.
    fn choose(
        &self,
        lanes: &[usize],
    ) -> Result<(Option<Chosen>, bool)> {
        if lanes.is_empty() {
            return Ok((None, false));
        }
        let home = self.lane % lanes.len();
        let bounded =
            self.inbox().work_capacity(lanes[home]).ok().flatten().is_some();
        if !bounded && let (Some(chosen), full) = self.choose_two(lanes)? {
            return Ok((Some(chosen), full));
        }
        let mut full = false;
        for offset in 0..lanes.len() {
            let target = lanes[(home + offset) % lanes.len()];
            if self.candidate(target).is_none() {
                continue;
            }
            if let Some(chosen) = self.try_reserve(target, &mut full)? {
                return Ok((Some(chosen), full));
            }
        }
        Ok((None, full))
    }

    /// Two receivers picked at random, the shallower tried first.
    fn choose_two(
        &self,
        lanes: &[usize],
    ) -> Result<(Option<Chosen>, bool)> {
        let mut full = false;
        if lanes.len() < 2 {
            return Ok((None, full));
        }
        let turn = self.rotation.fetch_add(1, Ordering::Relaxed);
        let random = turn
            .wrapping_add(self.lane as u64)
            .wrapping_mul(0x9e37_79b9_7f4a_7c15);
        let first = (random % lanes.len() as u64) as usize;
        let mut second = ((random >> 32) % (lanes.len() as u64 - 1)) as usize;
        if second >= first {
            second += 1;
        }
        let (first, second) = (lanes[first], lanes[second]);
        let order = match (self.candidate(first), self.candidate(second)) {
            (Some(a), Some(b)) if b < a => [Some(second), Some(first)],
            (Some(_), Some(_)) => [Some(first), Some(second)],
            (Some(_), None) => [Some(first), None],
            (None, Some(_)) => [Some(second), None],
            (None, None) => [None, None],
        };
        for target in order.into_iter().flatten() {
            if let Some(chosen) = self.try_reserve(target, &mut full)? {
                return Ok((Some(chosen), full));
            }
        }
        Ok((None, full))
    }

    fn send_acquiring<G>(
        &self,
        application: &str,
        lanes: &[usize],
        request: &mut Exchange,
        acquisition: G,
    ) -> Result<Pending> {
        let (selected, full) = self.choose(lanes)?;
        let Some((target, admission)) = selected else {
            return Err(if full {
                Error::CapacityFull
            } else {
                Error::NoConsumer(application.to_owned())
            });
        };

        // A kept stream, or a new one when none is ready. Re-arming happens
        // here rather than after the write, because the frame below is what
        // tells the far side a new exchange has begun and it must not carry a
        // slot that still holds the last one's state.
        let (mut endpoint, ticket) = self.take_hot()?;
        // Only acquisition is serialized. Encoding, publication and the whole
        // exchange lifetime run after the next queued creator can proceed.
        drop(acquisition);

        let correlation = self.correlation.fetch_add(1, Ordering::Relaxed);
        request.correlation = correlation;
        request.origin = self.lane as u16;
        request.stream = Some(ticket);

        let mut head = request.encode_head()?;
        let (body, spill) = fit_inline(
            &mut head,
            std::mem::take(&mut request.body),
            self.segment.inbox().geometry().payload_capacity,
        );
        let mut unsent = None;
        match (&body, spill) {
            (ExchangeBody::Stream, Some(bytes)) => {
                let written = match endpoint.try_write(&bytes) {
                    Ok(written) => written,
                    Err(orbitive::stream::Error::WouldBlock) => 0,
                    Err(error) => return Err(Error::Stream(error)),
                };
                if written == bytes.len() {
                    let _ = endpoint.finish();
                } else {
                    unsent = Some(bytes.slice(written..));
                }
            }
            (ExchangeBody::Stream, None) => {}
            (ExchangeBody::Empty | ExchangeBody::Inline(_), _) => {
                let _ = endpoint.finish();
            }
        }
        let (sender, receiver) = oneshot::channel();
        // Registered before the write, because the application may answer
        // before this function returns.
        let owner = LaneOwner {
            lane: target,
            pid: self.segment.inbox().owner(target).unwrap_or_default(),
            incarnation: self
                .segment
                .inbox()
                .incarnation(target)
                .unwrap_or_default(),
        };
        self.waiting.insert(correlation, owner, sender);

        let published = match admission {
            Some(admission) => admission.publish(&[&head, body.inline()]),
            None => self
                .segment
                .inbox()
                .write_vectored(target, &[&head, body.inline()]),
        };
        if let Err(error) = published {
            self.waiting.take(correlation);
            self.segment.reclaim_if_gone(target);
            tracing::debug!(
                application,
                target,
                consumers =
                    self.segment.inbox().consumers(target).unwrap_or_default(),
                depth = self.segment.inbox().depth(target).unwrap_or_default(),
                "exchange inbox write was refused: {error}"
            );
            return Err(super::map_link_error(error));
        }

        let (read, write) = endpoint.split();
        let reset = read.reset_signal();
        let loan = Arc::new(Loan {
            hot: Arc::clone(&self.hot),
            ticket,
            halves: Mutex::new((None, None)),
        });
        Ok(Pending {
            correlation,
            request_body: LentWrite {
                half: Some(write),
                loan: Arc::clone(&loan),
            },
            response_body: LentRead { half: Some(read), loan },
            unsent,
            reply: ReplyWait {
                reply: receiver,
                reset,
                waiting: Arc::clone(&self.waiting),
                correlation,
                completed: false,
            },
        })
    }

    pub(super) fn capacity_changed(&self) -> &tokio::sync::Notify {
        &self.waiting.capacity_changed
    }

    pub(super) fn streams_returned(&self) -> &tokio::sync::Notify {
        &self.hot.returned
    }

    pub(super) fn membership_changed(&self) -> &tokio::sync::Notify {
        &self.waiting.membership_changed
    }

    pub(super) fn stream_capacity_changed(
        &self
    ) -> orbitive::stream::CapacityChanged {
        self.bodies.streams().capacity_changed()
    }

    pub(super) fn watch_capacity(
        &self,
        lanes: &[usize],
    ) -> Result<()> {
        for &lane in lanes {
            self.inbox()
                .watch_capacity(lane, self.lane)
                .map_err(super::map_link_error)?;
        }
        Ok(())
    }

    /// A kept stream re-armed for this exchange, or a new one.
    fn take_hot(&self) -> Result<(Endpoint, Ticket)> {
        let kept = self.hot.take(&self.bodies);
        match kept {
            Some(slot) => Ok(slot),
            None => Ok(self.bodies.create()?),
        }
    }

    /// Stop reading and give up the lane.
    ///
    /// The lane goes first, so nothing new is addressed to a mailbox this
    /// worker is about to stop emptying. Then the reader is *woken* rather
    /// than waited out.
    pub fn leave(&self) {
        self.waiting.closed.store(true, Ordering::Release);
        self.waiting.capacity_changed.notify_waiters();
        self.waiting.membership_changed.notify_waiters();
        let _ = self.segment.inbox().release_lane(self.lane);
        drop(self.hold.lock().expect("lane hold").take());
        if let Some(reader) = self.reader.lock().expect("reader handle").take()
        {
            reader.abort();
        }
    }

    fn spawn_reader(
        self: &Arc<Self>,
        edge: &str,
        events: Option<ProcessEventBus>,
        link: Option<ApplicationLinkSetup>,
    ) -> Result<tokio::task::JoinHandle<()>> {
        let edge = edge.to_owned();
        let mut receiver = self.segment.receiver(self.lane)?;
        let waiting = Arc::clone(&self.waiting);
        let lane = self.lane;
        let segment = Arc::clone(&self.segment);
        let last_seen = Arc::clone(&self.last_seen);
        let application_states = Arc::clone(&self.application_states);
        // Every lane that is not an edge's: the processes this edge reaches.
        let mut applications =
            Peers::new(self.fleet, |role| role != EDGE_RUNTIME);
        // Read after the receiver is bound: a change from here on wakes it,
        // and the applications already present are reported as found.
        let found = observe_applications(
            &mut applications,
            &segment,
            lane,
            &edge,
            events.as_ref(),
            &last_seen,
            &application_states,
        );
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| {
            Error::Malformed("the reply receiver needs a Tokio runtime")
        })?;
        // The far side of an edge's lane is every lane that is not an edge's.
        let link = link.and_then(|setup| {
            setup.for_lane(&segment, lane, |role| role != EDGE_RUNTIME)
        });
        if let Some(events) = &events {
            events.emit(LinkJoined {
                fleet: self.fleet,
                lane,
                role: EDGE_RUNTIME.to_owned(),
            });
        }
        Ok(runtime.spawn(async move {
            // Dropped with this task, so no watch outlives the client.
            let mut watches = tokio::task::JoinSet::new();
            for peer in found {
                watches.spawn(watch_application(
                    Arc::clone(&segment),
                    Arc::clone(&waiting),
                    peer,
                ));
            }
            let mut frame = Vec::new();
            loop {
                match receiver.next(&mut frame).await {
                    Ok(Received::Frame) => match frame_kind(&frame) {
                        Ok(FrameKind::ApplicationState) => accept_application_state(&application_states, &segment, &frame),
                        // Handed over whole, so an inline body is a slice of
                        // it rather than a copy.
                        _ => deliver(&waiting, Bytes::from(std::mem::take(&mut frame))),
                    },
                    Ok(Received::Capacity(_)) => waiting.capacity_changed.notify_waiters(),
                    Ok(Received::Work(ticket)) => {
                        // This lane advertises no work capacity. Reject an
                        // invalid application frame without retaining a permit.
                        drop(segment.accept_work(lane, ticket));
                        tracing::warn!(lane, "work admission was addressed to a reply lane");
                    }
                    Ok(Received::Event) => match &link {
                        Some(link) => link.accept(&frame),
                        None => tracing::debug!(
                            "an application event arrived in reply lane {lane}, which bridges none"
                        ),
                    },
                    Ok(Received::Directory(_)) => {
                        waiting.capacity_changed.notify_waiters();
                        waiting.membership_changed.notify_waiters();
                        while watches.try_join_next().is_some() {}
                        for peer in observe_applications(
                            &mut applications,
                            &segment,
                            lane,
                            &edge,
                            events.as_ref(),
                            &last_seen,
                            &application_states,
                        ) {
                            watches.spawn(watch_application(
                                Arc::clone(&segment),
                                Arc::clone(&waiting),
                                peer,
                            ));
                        }
                    }
                    Err(error) => {
                        tracing::debug!(
                            "reply lane {lane} could not be read: {error}"
                        );
                        waiting.closed.store(true, Ordering::Release);
                        waiting.capacity_changed.notify_waiters();
                        waiting.membership_changed.notify_waiters();
                        return;
                    }
                }
            }
        }))
    }
}

/// Wait for this application's process to exit, then take its lane back: its
/// streams end, what it never read ends with them, and new requests are refused
/// rather than queued for nobody. A lane that was already taken back is left
/// alone, because recovery names this exact process.
async fn watch_application(
    segment: Arc<ApplicationSegment>,
    waiting: Arc<Waiting>,
    peer: super::Peer,
) {
    match segment.recover_on_exit(&peer).await {
        Ok(true) => {}
        Ok(false) => return,
        Err(error) => {
            tracing::error!(lane = peer.lane, %error, "could not recover departed Link owner");
            return;
        }
    }
    waiting.abandon(LaneOwner {
        lane: peer.lane,
        pid: peer.pid,
        incarnation: peer.incarnation,
    });
}

/// Diff the directory and say what changed: in the log, and as a local event.
/// A departure before an arrival, because a process that restarted left
/// before it came back. Every connect and disconnect is reported, reconnects
/// too. Returns who arrived.
fn observe_applications(
    applications: &mut Peers,
    segment: &ApplicationSegment,
    lane: usize,
    edge: &str,
    events: Option<&ProcessEventBus>,
    last_seen: &Mutex<HashMap<String, String>>,
    states: &Mutex<HashMap<usize, ApplicationBatch>>,
) -> Vec<super::Peer> {
    let (came, went) = applications.update(segment.inbox());
    for application in went {
        states
            .lock()
            .expect("application state lock")
            .remove(&application.lane);
        tracing::warn!(
            fleet_name = %application.fleet,
            "🛰 application '{}' ({}) left '{edge}' (lane {}, pid {})",
            application.name,
            application.role,
            application.lane,
            pid(application.pid)
        );
        if let Some(events) = events {
            let last = !applications.serves(&application.name);
            events.emit(ApplicationDisconnected {
                application,
                edge: edge.to_owned(),
                last,
            });
        }
    }
    for application in &came {
        let application = application.clone();
        request_application_state(segment, &application, lane);
        last_seen
            .lock()
            .expect("last seen Link peers")
            .insert(application.name.clone(), application.role.clone());
        tracing::info!(
            fleet_name = %application.fleet,
            "🛰 application '{}' ({}) reachable from '{edge}' (lane {}, pid {})",
            application.name,
            application.role,
            application.lane,
            pid(application.pid)
        );
        if let Some(events) = events {
            events.emit(ApplicationConnected {
                application,
                edge: edge.to_owned(),
            });
        }
    }
    came
}

/// Ask one application for its state, answered to this edge's lane alone:
/// a broadcast answer to every status request made each of N edges hear from
/// each of N applications N times over at once.
fn request_application_state(
    segment: &ApplicationSegment,
    peer: &super::Peer,
    lane: usize,
) {
    let request = ControlCommand {
        application: peer.name.clone(),
        name: "status".to_owned(),
        payload: Vec::new(),
        target: None,
        origin: u16::try_from(lane).ok(),
    };
    match request.encode() {
        Ok(frame) => {
            if let Err(error) = segment.inbox().write(peer.lane, &frame) {
                segment.reclaim_if_gone(peer.lane);
                tracing::debug!(
                    lane = peer.lane,
                    "Link application status request failed: {error}"
                );
            }
        }
        Err(error) => tracing::debug!(
            "Link application status request could not be encoded: {error}"
        ),
    }
}

fn pid(pid: Option<u32>) -> String {
    pid.map_or_else(|| "unknown".to_owned(), |pid| pid.to_string())
}

fn accept_application_state(
    states: &Mutex<HashMap<usize, ApplicationBatch>>,
    segment: &ApplicationSegment,
    frame: &[u8],
) {
    let notice = match ApplicationStateNotice::decode(frame) {
        Ok(notice) => notice,
        Err(error) => {
            tracing::debug!("malformed application state notice: {error}");
            return;
        }
    };
    let lane = usize::from(notice.lane);
    let inbox = segment.inbox();
    if inbox.service(lane).ok().flatten().as_deref()
        != Some(notice.application.as_str())
        || inbox.incarnation(lane).ok() != Some(notice.incarnation)
        || !matches!(inbox.role(lane).ok().flatten(), Some(role) if role != EDGE_RUNTIME)
        || inbox.consumers(lane).ok().is_none_or(|count| count == 0)
    {
        return;
    }
    if notice.row_count == 0 || notice.row_count > 512 {
        return;
    }
    let pid = inbox.owner(lane).ok().flatten();
    let mut states = states.lock().expect("application state lock");
    states.entry(lane).or_default().accept(notice, pid);
}

impl ApplicationBatch {
    /// Merge a decoded row after the caller has validated its live lane owner
    /// and bounded row count. Partial snapshots are never exposed as complete.
    fn accept(
        &mut self,
        notice: ApplicationStateNotice,
        pid: Option<u32>,
    ) {
        if self.completed.as_ref().is_some_and(|completed| {
            completed.incarnation == notice.incarnation
                && completed.pid == pid
                && completed.batch >= notice.batch
        }) {
            return;
        }
        let replace = self.pending.as_ref().is_none_or(|pending| {
            pending.incarnation != notice.incarnation
                || pending.pid != pid
                || notice.batch > pending.batch
        });
        if replace {
            // A newer observation is in flight. Do not present the prior
            // lifecycle as current while only part of this batch has arrived.
            self.completed = None;
            self.pending = Some(PendingApplicationBatch {
                incarnation: notice.incarnation,
                pid,
                batch: notice.batch,
                rows: vec![None; usize::from(notice.row_count)],
            });
        }
        let Some(pending) = self.pending.as_mut() else { return };
        if pending.incarnation != notice.incarnation
            || pending.pid != pid
            || pending.batch != notice.batch
            || pending.rows.len() != usize::from(notice.row_count)
        {
            return;
        }
        pending.rows[usize::from(notice.row_index)] = Some(notice.payload);
        if pending.rows.iter().all(Option::is_some) {
            let rows = pending.rows.iter().flatten().cloned().collect();
            self.completed = Some(CompletedApplicationBatch {
                incarnation: pending.incarnation,
                pid: pending.pid,
                batch: pending.batch,
                rows,
            });
            self.pending = None;
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/link/application/client.rs"]
mod tests;

/// Hand one frame to whoever is waiting for it.
fn deliver(
    waiting: &Waiting,
    frame: Bytes,
) {
    match frame_kind(&frame) {
        Ok(FrameKind::Reply) => {}
        Ok(FrameKind::Request) => {
            // An edge's lane takes replies. A request here was written to the
            // wrong lane, which is the writer's bug and not something to
            // answer.
            tracing::debug!("a request arrived in a reply lane");
            return;
        }
        Ok(FrameKind::Control) => {
            tracing::debug!("a control command arrived in a reply lane");
            return;
        }
        Ok(FrameKind::ShutdownInitiated) => {
            tracing::debug!("a shutdown notice arrived in a reply lane");
            return;
        }
        Ok(FrameKind::ApplicationState) => {
            tracing::debug!(
                "application state was routed to the reply decoder"
            );
            return;
        }
        Err(error) => {
            tracing::debug!(
                "an unreadable frame arrived in a reply lane: {error}"
            );
            return;
        }
    }

    let reply = match ExchangeReply::decode(frame) {
        Ok(reply) => reply,
        Err(error) => {
            tracing::debug!("a malformed reply arrived: {error}");
            return;
        }
    };
    match waiting.take(reply.correlation) {
        // The caller gave up, or this is a second reply to one exchange.
        // Dropped rather than raised: there is nobody left to tell.
        None => {
            tracing::debug!("reply {} has no caller waiting", reply.correlation)
        }
        Some(sender) => {
            let _ = sender.send(reply);
        }
    }
}

/// A receiver chosen for one exchange, and its admission when it is bounded.
type Chosen = (usize, Option<super::WorkReservation>);
