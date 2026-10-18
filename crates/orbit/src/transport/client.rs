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
//! addressed to. See [`Hot`].
//!
//! Nothing else is. No health probe, no warm floor, no retry: there is no
//! acknowledgement to retry against, and a request that was written was
//! delivered while one that was not is a 503 before anything ran.
//!
//! No timeout either, yet. A reply that never comes is a wait that never ends,
//! and the answer to that is to read the worker's death rather than to invent
//! a duration for it.

use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use claviron_runtime::{FleetName, ProcessEventBus};
use orbitive::link::Received;
use orbitive::stream::{Endpoint, ReadHalf, Ticket, WriteHalf};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::oneshot;

use super::{
    ApplicationConnected, ApplicationDisconnected, ApplicationLinkSetup,
    ApplicationStateNotice, ControlCommand, Error, Exchange, ExchangeReply,
    FrameKind, LinkJoined, Peers, Result, TransportBodies, TransportSegment,
    frame_kind,
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
    /// Where the response body comes from.
    pub response_body: LentRead,
    reply: oneshot::Receiver<ExchangeReply>,
}

impl Pending {
    /// Take the three apart, because the caller drives them at the same time.
    ///
    /// The request body is written while the reply is being waited for, and
    /// the response body may start arriving before the request body is
    /// finished. Holding them together would make that a borrow problem
    /// rather than the ordinary case it is.
    pub fn split(self) -> (LentWrite, LentRead, ReplyWait) {
        (self.request_body, self.response_body, ReplyWait { reply: self.reply })
    }
}

/// The head of a reply, once the application has one.
pub struct ReplyWait {
    reply: oneshot::Receiver<ExchangeReply>,
}

impl ReplyWait {
    /// Wait for the status and headers.
    ///
    /// There is no deadline here. A reply that never comes means the worker
    /// holding the request is gone, and reading that is the answer rather than
    /// a number chosen to stand in for it.
    pub async fn head(self) -> Result<ExchangeReply> {
        self.reply.await.map_err(|_| {
            Error::Malformed(
                "the reply channel closed before an answer arrived",
            )
        })
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
    slots: VecDeque<(Endpoint, Ticket)>,
}

impl Hot {
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
        &mut self,
        bodies: &TransportBodies,
    ) -> Option<(Endpoint, Ticket)> {
        let mut at = 0;
        while at < self.slots.len() {
            match bodies.rearm(&mut self.slots[at].0) {
                Ok(()) => return self.slots.remove(at),
                Err(Error::Stream(orbitive::stream::Error::WouldBlock)) => {
                    at += 1
                }
                Err(_) => {
                    self.slots.remove(at);
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
    hot: Arc<Mutex<Hot>>,
    ticket: Ticket,
    halves: Mutex<(Option<ReadHalf>, Option<WriteHalf>)>,
}

impl Loan {
    fn give_back(&self) {
        let rejoined = {
            let mut halves =
                self.halves.lock().expect("the loan mutex is never poisoned");
            match (halves.0.take(), halves.1.take()) {
                // The exchange is over for this side, and the application is
                // told so, as dropping the half would have told it. Ordinarily
                // every byte has been read and this says nothing new; when the
                // client went away mid-response it is what stops the
                // application waiting on a ring nobody will empty.
                (Some(read), Some(write)) => {
                    read.stop_reading();
                    Endpoint::rejoin(read, write).ok()
                }
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
            .lock()
            .expect("the hot-slot mutex is never poisoned")
            .slots
            .push_back((endpoint, self.ticket));
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
pub struct TransportClient {
    fleet: FleetName,
    segment: Arc<TransportSegment>,
    bodies: Arc<TransportBodies>,
    lane: usize,
    /// The lane's lock, let go in [`TransportClient::leave`] after the lane.
    hold: Mutex<Option<orbitive::shm::LaneHold>>,
    correlation: AtomicU64,
    /// Which of the idlest targets to take next. See [`TransportClient::send`].
    rotation: AtomicU64,
    directory: Mutex<Directory>,
    hot: Arc<Mutex<Hot>>,
    waiting: Arc<Waiting>,
    reader: Mutex<Option<tokio::task::JoinHandle<()>>>,
    last_seen: Arc<Mutex<HashMap<String, String>>>,
    application_states: Arc<Mutex<HashMap<usize, ApplicationBatch>>>,
}

#[derive(Default)]
struct ApplicationBatch {
    completed: Option<(u64, Option<u32>, u64, Vec<Vec<u8>>)>,
    pending: Option<PendingApplicationBatch>,
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
    lanes: std::collections::HashMap<Box<str>, Arc<[usize]>>,
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
    senders: Mutex<HashMap<u64, oneshot::Sender<ExchangeReply>>>,
}

impl Waiting {
    fn insert(
        &self,
        correlation: u64,
        sender: oneshot::Sender<ExchangeReply>,
    ) {
        self.senders
            .lock()
            .expect("waiting exchanges")
            .insert(correlation, sender);
    }

    fn take(
        &self,
        correlation: u64,
    ) -> Option<oneshot::Sender<ExchangeReply>> {
        self.senders.lock().expect("waiting exchanges").remove(&correlation)
    }
}

impl TransportClient {
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
            .filter(|(incarnation, pid, _, _)| {
                *incarnation == peer.incarnation && *pid == peer.pid
            })
            .map(|(_, _, _, rows)| rows.clone());
        if state.is_none() {
            request_application_state(&self.segment, peer);
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
        let segment = Arc::new(TransportSegment::for_fleet(fleet_name)?);
        let joined = segment.join(name, EDGE_RUNTIME, incarnation)?;
        let lane = joined.lane;
        let bodies = Arc::new(TransportBodies::for_fleet(
            fleet_name,
            lane,
            incarnation,
        )?);
        bodies.reclaimed(&joined.reclaimed);

        let client = Arc::new(Self {
            fleet: FleetName::new(fleet_name),
            segment,
            bodies,
            lane,
            hold: Mutex::new(Some(joined.hold)),
            correlation: AtomicU64::new(1),
            // Seeded with this caller's own lane, not with zero.
            //
            // Every edge worker reads the same targets in the same order and
            // steps through them by one. Starting them all at the same place
            // makes independent rotation move in lockstep: at any instant all
            // of them are writing to the same application process while its
            // siblings sit idle, and they move on together. The counter was
            // meant to spread the tie and it was spreading it in unison.
            //
            // The lane index is already unique per process in the fleet, so
            // it costs nothing to start from it.
            rotation: AtomicU64::new(lane as u64),
            directory: Mutex::default(),
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
    /// `body_follows` is false for a request known to have no body. Its
    /// direction is then ended here, before the frame that announces it: the
    /// application's first look at the body finds the end already there and
    /// never parks for it, and since nobody has opened the far side yet,
    /// ending it rings no one. Ended after the frame instead — by the caller,
    /// from a task of its own — the application was usually parked by then,
    /// and every bodiless request paid a wake for its missing body.
    pub fn send(
        &self,
        application: &str,
        mut request: Exchange,
        body_follows: bool,
    ) -> Result<Pending> {
        let inbox = self.segment.inbox();
        let targets = inbox.ready(&Directory::resolve(
            &self.directory,
            inbox,
            application,
        ));
        let Some((_, idlest)) = targets.first().copied() else {
            return Err(Error::NoConsumer(application.to_owned()));
        };

        // Depth decides, and when depth cannot decide, rotation does.
        //
        // A lane's depth is what has arrived and not yet been picked up, so on
        // a fleet that is keeping up it is zero everywhere: a worker takes the
        // frame immediately and spends its time in the application, not in the
        // mailbox. Depth is therefore the overload signal rather than the
        // everyday one, and every ordinary dispatch is a tie.
        //
        // Reading gives nothing in a tie, so this is a choice rather than a
        // reading, and it is made where choices belong — in the caller. Taking
        // the first target would be a choice too, and the wrong one: it is
        // always the same process, so a second one serving the same
        // application would sit idle until the first fell behind.
        //
        // `targets` is sorted by depth, so the tied ones are a prefix of it.
        let tied =
            targets.iter().take_while(|(_, depth)| *depth == idlest).count();
        let turn = self.rotation.fetch_add(1, Ordering::Relaxed) as usize;
        let target = targets[turn % tied].0;

        // A kept stream, or a new one when none is ready. Re-arming happens
        // here rather than after the write, because the frame below is what
        // tells the far side a new exchange has begun and it must not carry a
        // slot that still holds the last one's state.
        let (mut endpoint, ticket) = self.take_hot()?;
        if !body_follows {
            let _ = endpoint.finish();
        }

        let correlation = self.correlation.fetch_add(1, Ordering::Relaxed);
        request.correlation = correlation;
        request.origin = self.lane as u16;
        request.body = Some(ticket);

        let frame = request.encode()?;
        let (sender, receiver) = oneshot::channel();
        // Registered before the write, because the application may answer
        // before this function returns.
        self.waiting.insert(correlation, sender);

        if let Err(error) = self.segment.inbox().write(target, &frame) {
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
            reply: receiver,
        })
    }

    /// A kept stream re-armed for this exchange, or a new one.
    fn take_hot(&self) -> Result<(Endpoint, Ticket)> {
        let kept = self
            .hot
            .lock()
            .expect("the hot-slot mutex is never poisoned")
            .take(&self.bodies);
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
        observe_applications(
            &mut applications,
            &segment,
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
            let mut frame = Vec::new();
            loop {
                match receiver.next(&mut frame).await {
                    Ok(Received::Frame) => match frame_kind(&frame) {
                        Ok(FrameKind::ApplicationState) => accept_application_state(&application_states, &segment, &frame),
                        _ => deliver(&waiting, &frame),
                    },
                    Ok(Received::Event) => match &link {
                        Some(link) => link.accept(&frame),
                        None => tracing::debug!(
                            "an application event arrived in reply lane {lane}, which bridges none"
                        ),
                    },
                    Ok(Received::Directory(_)) => observe_applications(
                        &mut applications,
                        &segment,
                        &edge,
                        events.as_ref(),
                        &last_seen,
                        &application_states,
                    ),
                    Err(error) => {
                        tracing::debug!(
                            "reply lane {lane} could not be read: {error}"
                        );
                        return;
                    }
                }
            }
        }))
    }
}

/// Diff the directory and say what changed: in the log, and as a local event.
/// A departure before an arrival, because a process that restarted left
/// before it came back. Every connect and disconnect is reported, reconnects
/// too.
fn observe_applications(
    applications: &mut Peers,
    segment: &TransportSegment,
    edge: &str,
    events: Option<&ProcessEventBus>,
    last_seen: &Mutex<HashMap<String, String>>,
    states: &Mutex<HashMap<usize, ApplicationBatch>>,
) {
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
    for application in came {
        request_application_state(segment, &application);
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
}

fn request_application_state(
    segment: &TransportSegment,
    peer: &super::Peer,
) {
    let request = ControlCommand {
        application: peer.name.clone(),
        name: "status".to_owned(),
        payload: Vec::new(),
        target: None,
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
    segment: &TransportSegment,
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
    let entry = states.entry(lane).or_default();
    if entry.completed.as_ref().is_some_and(
        |(incarnation, previous_pid, batch, _)| {
            *incarnation == notice.incarnation
                && *previous_pid == pid
                && *batch >= notice.batch
        },
    ) {
        return;
    }
    let replace = entry.pending.as_ref().is_none_or(|pending| {
        pending.incarnation != notice.incarnation
            || pending.pid != pid
            || notice.batch > pending.batch
    });
    if replace {
        // A newer observation is in flight. Do not present the prior
        // lifecycle as current while only part of this batch has arrived.
        entry.completed = None;
        entry.pending = Some(PendingApplicationBatch {
            incarnation: notice.incarnation,
            pid,
            batch: notice.batch,
            rows: vec![None; usize::from(notice.row_count)],
        });
    }
    let Some(pending) = entry.pending.as_mut() else { return };
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
        entry.completed =
            Some((pending.incarnation, pending.pid, pending.batch, rows));
        entry.pending = None;
    }
}

/// Hand one frame to whoever is waiting for it.
fn deliver(
    waiting: &Waiting,
    frame: &[u8],
) {
    match frame_kind(frame) {
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
