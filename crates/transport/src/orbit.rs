//! The ingress with no socket.
//!
//! Nothing here binds a port. The process joins its configured fleet, says
//! which application it serves and what answers it, and waits on its lane; a
//! Claviron edge asked for that name writes an exchange into it. The [`Owner`]
//! that answers is the one the sockets call, so an application never learns
//! which door a request came through.
//!
//! ## Nobody is told about anybody
//!
//! There is no registration call, no supervisor and no pool. A process starts
//! on its own and introduces itself by taking a lane and writing its name in
//! it. The edge reads the lanes serving a name and picks one; several
//! processes may serve a name without knowing about each other.
//!
//! ## Reactor-native inbox wake
//!
//! A lane is read by the runtime task itself. The frame remains in shared
//! memory; a named descriptor carries only a coalescing wake into Tokio's
//! reactor. There is no polling interval, deadline, reader thread or hidden
//! process-local queue.

use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use claviron_app::{
    APPLICATION_EVENT_TOPIC, ApplicationEvent, EventArrayKey, EventValue,
};
use claviron_http::ext::{RealIpExt, RequestArrival};
use claviron_http::socket::{
    Carrier, Deferred, Door, Offer, OfferInfo, SocketIo, SocketKind, refusal,
    strip_protocol_headers,
};
use claviron_http::trust::Forwarded;
pub use claviron_orbit::link::application::Endpoint;
use claviron_orbit::link::application::{
    ApplicationBodies, ApplicationLinkSetup, ApplicationSegment,
    ControlCommand, ControlTarget, EDGE_RUNTIME, EdgeConnected,
    EdgeDisconnected, Error as LinkError, Exchange, ExchangeBody,
    ExchangeReply, FleetGrowth, FrameKind, HttpVersion, IdleRetirement, Inbox,
    LinkJoined, LinkPressure, LinkWorkerIdle, Offered, Peers, Received,
    RetirementSource, ShutdownNotice, Watched, WorkExecution, WorkQueue,
    fit_inline, frame_kind,
};
pub use claviron_orbit::stream::{ReadHalf, WriteHalf};
use claviron_orbit::{ApplicationEventRegistry, FleetEventCodec};
use claviron_runtime::{
    AppState, Event, FleetIncarnation, FleetName, RunContext, registry,
};
use http::{Request, Uri};
use http_body_util::BodyExt;
use tokio::io::AsyncWriteExt;
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::owner::{AppRequest, Owner, RequestBody};

/// Read-only retirement signal for this ingress generation's message owners.
/// Waiting permits a cooperative protocol close, not forced guest termination.
#[derive(Clone)]
pub struct SessionShutdown {
    token: CancellationToken,
}

impl SessionShutdown {
    pub async fn cancelled(&self) {
        self.token.cancelled().await;
    }
}

/// A message session's I/O, in [`SocketIo::Native`], once the edge has been told
/// it is accepted: an H1/H2 WebSocket or one reliable WebTransport bidi stream,
/// as records on the exchange's slot.
pub struct LinkSession {
    pub incoming: ReadHalf,
    pub outgoing: WriteHalf,
    /// This ingress generation's retirement.
    pub shutdown: SessionShutdown,
}

/// Link's session acknowledgment: `101` with the owner's accepted headers, or
/// an error status. Edge translates acceptance to the native H1/H2/H3 response.
struct Handshake {
    segment: Arc<ApplicationSegment>,
    origin: usize,
    correlation: u64,
}

impl Handshake {
    /// Publish the Link acknowledgment and report whether it was delivered. The
    /// slot must be reset when this fails, so the edge's pending admission wakes
    /// even when its inbox could not take the reply.
    fn try_answer(
        self,
        status: u16,
        headers: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> bool {
        let correlation = self.correlation;
        // Accepted, the session's records travel on the stream; refused, the
        // stream ends with nothing on it.
        let reply = ExchangeReply {
            correlation,
            status,
            headers,
            body: ExchangeBody::Stream,
        };
        match reply.encode() {
            Ok(frame) => {
                if let Err(error) =
                    self.segment.inbox().write(self.origin, &frame)
                {
                    tracing::debug!(
                        correlation,
                        "🛰 ✗ WebSocket answer was not delivered: {error}"
                    );
                    return false;
                }
                true
            }
            Err(error) => {
                tracing::debug!(correlation, "🛰 ✗ WebSocket answer: {error}");
                false
            }
        }
    }

    fn refuse(
        self,
        status: u16,
        endpoint: Option<Endpoint>,
    ) {
        if !self.try_answer(status, Vec::new())
            && let Some(mut endpoint) = endpoint
        {
            endpoint.reset();
        }
    }
}

/// How a session attempt ended, for the exchange that holds its slot.
enum Ended {
    Accepted(Deferred),
    /// Whether the edge heard the refusal: an answer ends the direction it waits
    /// on, and without one only a reset wakes it.
    Refused {
        answered: bool,
    },
}

/// Completes an attempt with the Link acknowledgment. The slot stays with the
/// exchange, which runs the session as its own work.
struct LinkDoor {
    handshake: Handshake,
    ended: oneshot::Sender<Ended>,
}

impl Door for LinkDoor {
    fn commit(
        self: Box<Self>,
        mut proposal: claviron_http::RouteResponse,
        deferred: Deferred,
    ) -> claviron_http::RouteResponse {
        strip_protocol_headers(proposal.headers_mut());
        let ended = if self.handshake.try_answer(101, header_pairs(&proposal)) {
            Ended::Accepted(deferred)
        } else {
            Ended::Refused { answered: false }
        };
        let _ = self.ended.send(ended);
        proposal
    }

    fn refuse(
        self: Box<Self>,
        response: claviron_http::RouteResponse,
    ) -> claviron_http::RouteResponse {
        let response = refusal(response);
        let answered = self
            .handshake
            .try_answer(response.status().as_u16(), header_pairs(&response));
        let _ = self.ended.send(Ended::Refused { answered });
        response
    }
}

fn header_pairs(
    response: &claviron_http::RouteResponse
) -> Vec<(Vec<u8>, Vec<u8>)> {
    response
        .headers()
        .iter()
        .map(|(name, value)| {
            (name.as_str().as_bytes().to_vec(), value.as_bytes().to_vec())
        })
        .collect()
}

/// This process's place in its configured transport fleet.
pub struct OrbitDoor {
    application: String,
    runtime: &'static str,
    capacity: Arc<claviron_orbit::config::LinkCapacity>,
}

/// A Link command delivered to the application. This door does not execute
/// arbitrary commands; application listeners decide their meaning.
#[derive(Clone, Debug)]
pub struct LinkCommandReceived {
    pub fleet: FleetName,
    /// The advertised Link destination name.
    pub application: String,
    pub name: String,
    pub payload: Vec<u8>,
    pub target: Option<ControlTarget>,
}

impl Event for LinkCommandReceived {}

/// The edge master announced ShutdownInitiated over Link. This is independent
/// of later lane disconnection.
#[derive(Clone, Debug)]
pub struct EdgeShutdownInitiated {
    pub edge: String,
    pub fleet: FleetName,
    pub application: String,
}

impl Event for EdgeShutdownInitiated {}

impl EdgeShutdownInitiated {
    fn application_event(&self) -> registry::Result<ApplicationEvent> {
        ApplicationEvent::new(
            "claviron.edge.shutdown.initiated.v1",
            EventValue::Array(
                [
                    ("edge", self.edge.as_str()),
                    ("fleet", self.fleet.as_str()),
                    ("application", self.application.as_str()),
                ]
                .into_iter()
                .map(|(key, value)| {
                    (
                        EventArrayKey::String(key.to_owned()),
                        EventValue::String(value.as_bytes().to_vec()),
                    )
                })
                .collect(),
            ),
        )
        .map_err(|error| registry::Error::msg(error.to_string()))
    }
}

/// Report the real local application snapshot through this process's Link
/// lane: to every edge, or to the one edge lane in `to` that asked.
pub fn publish_application_snapshot(
    state: &AppState,
    segment: &ApplicationSegment,
    link_name: &str,
    to: Option<usize>,
) -> registry::Result<()> {
    let Some(rows) = claviron_app::fleet::link_snapshot_rows(state)
        .map_err(registry::Error::msg)?
    else {
        return Ok(());
    };
    segment
        .publish_application_state(link_name, &rows, to)
        .map_err(|error| registry::Error::msg(error.to_string()))?;
    Ok(())
}

/// Like a departure, the notice belongs to this process's applications, not
/// the fleet ring. Queue it before emitting the optional shutdown policy event.
fn edge_shutdown(
    state: &AppState,
    notice: EdgeShutdownInitiated,
) {
    let delivered = notice.application_event().and_then(|event| {
        match state.registry_ref().resolve::<ApplicationEventRegistry>() {
            Some(registry) => registry
                .deliver(
                    state.events().bus(),
                    APPLICATION_EVENT_TOPIC,
                    &event.encode_orbit(),
                )
                .map_err(registry::Error::msg),
            None => {
                state.events().emit(event);
                Ok(())
            }
        }
    });
    if let Err(error) = delivered {
        tracing::warn!(%error, "edge shutdown notice could not be delivered to applications");
    }
    state.events().emit(notice);
}

impl OrbitDoor {
    /// `runtime` is what the lane says answers the name (`php`, `asgi`); a
    /// process of another runtime given the same name is refused at its join
    /// rather than splitting this one's requests. `max_in_flight` bounds the
    /// reservations and accepted exchanges through body/session completion;
    /// `None` is no bound.
    pub fn new(
        application: impl Into<String>,
        runtime: &'static str,
        max_in_flight: Option<usize>,
    ) -> Self {
        Self {
            application: application.into(),
            runtime,
            // A bound given here admits only what it runs: past it, senders
            // are refused. A configured door takes `orbit.link` through
            // `capacity`.
            capacity: Arc::new(claviron_orbit::config::LinkCapacity::new(
                max_in_flight,
                None,
            )),
        }
    }

    /// Share the configuration owner's live admission control.
    pub fn capacity(
        mut self,
        capacity: Arc<claviron_orbit::config::LinkCapacity>,
    ) -> Self {
        self.capacity = capacity;
        self
    }

    /// Update a positive receiver bound. Existing body/session work is retained.
    /// A previously unbounded live receiver must rejoin before becoming bounded.
    pub fn set_max_in_flight(
        &self,
        limit: usize,
    ) -> registry::Result<()> {
        self.capacity.set_max_in_flight(limit)
    }
}

impl OrbitDoor {
    /// A master serves no traffic: its workers own the serving lanes. It
    /// watches the directory without a lane unless explicit idle retirement
    /// needs a management lane for capacity wakes.
    pub async fn serve<O: Owner>(
        &self,
        owner: Arc<O>,
        incarnation: Option<Arc<FleetIncarnation>>,
        state: AppState,
        context: RunContext,
    ) -> registry::Result<()> {
        if state.is_master() {
            return self.watch(&state, context).await;
        }
        // The master's count of this worker slot's lives. It stamps the lane
        // and the streams, so a death report that names the process before
        // this one cannot end this one's work.
        let incarnation =
            incarnation.map_or(FleetIncarnation::FIRST, |held| *held).raw();

        let (segment, joined) = self.capacity.join(
            state.fleet_name(),
            &self.application,
            self.runtime,
            incarnation,
        )?;
        let lane = joined.lane;
        let bodies = Arc::new(
            ApplicationBodies::for_segment(&segment, lane, incarnation)
                .map_err(|error| registry::Error::msg(error.to_string()))?,
        );
        if let Err(error) = publish_application_snapshot(
            &state,
            &segment,
            &self.application,
            None,
        ) {
            tracing::warn!(%error, "initial Link application snapshot was not published");
        }
        for gone in &joined.reclaimed {
            tracing::info!(
                "🛰 took back lane {} of '{}' from pid {}, which is gone",
                gone.lane,
                self.application,
                gone.pid
            );
        }
        bodies.reclaimed(&joined.reclaimed);
        tracing::info!(
            fleet_name = state.fleet_name(),
            "🛰 serving '{}' on lane {lane}",
            self.application
        );

        let mut receiver = segment
            .application_receiver(lane)
            .map_err(|error| registry::Error::msg(error.to_string()))?;
        // Read after the receiver is bound: a change from here on wakes it,
        // and the edges already present are reported as they are found.
        let mut edges = edge_peers(&state);
        observe_edges(&state, &mut edges, segment.inbox(), &self.application);
        // Application events cross to and from the edges over this lane.
        let link = ApplicationLinkSetup::resolve(&state).and_then(|setup| {
            setup.for_lane(&segment, lane, |role| role == EDGE_RUNTIME)
        });
        state.events().emit(LinkJoined {
            fleet: FleetName::new(state.fleet_name()),
            lane,
            role: self.runtime.to_owned(),
        });
        let mut exchanges = JoinSet::new();
        let shutdown = SessionShutdown { token: context.cancellation_token() };
        let mut frame = Vec::new();
        // Up to the published execution runs; the rest of the window waits
        // here, and the end of one exchange starts the next without a wake.
        let mut work = WorkQueue::new();
        loop {
            tokio::select! {
                biased;
                () = context.cancelled() => break,
                Some(finished) = exchanges.join_next(), if !exchanges.is_empty() => {
                    report(finished);
                    if let Some((request, admission)) = work.finished(&segment, lane) {
                        exchanges.spawn(serve(
                            request,
                            lane,
                            Arc::clone(&owner),
                            Arc::clone(&segment),
                            Arc::clone(&bodies),
                            shutdown.clone(),
                            admission,
                        ));
                    }
                }
                () = self.capacity.execution_changed(), if work.waiting() != 0 => {
                    while let Some((request, admission)) = work.start_waiting(&segment, lane) {
                        exchanges.spawn(serve(
                            request,
                            lane,
                            Arc::clone(&owner),
                            Arc::clone(&segment),
                            Arc::clone(&bodies),
                            shutdown.clone(),
                            admission,
                        ));
                    }
                }
                arrived = receiver.next(&mut frame) => match arrived {
                    Ok(Received::Directory(_)) => { observe_edges(&state, &mut edges, segment.inbox(), &self.application); },
                    Ok(Received::Capacity(_)) => {},
                    Ok(Received::Event) => match &link {
                        Some(link) => link.accept(&frame),
                        None => tracing::debug!("lane {lane} received an application event it bridges nowhere"),
                    },
                    Ok(received @ (Received::Frame | Received::Work(_)))
                        if matches!(frame_kind(&frame), Ok(FrameKind::Request)) =>
                    {
                        match work.offer(&segment, lane, received, &mut frame) {
                            Ok(Offered::Start((request, admission))) => {
                                exchanges.spawn(serve(
                                    request,
                                    lane,
                                    Arc::clone(&owner),
                                    Arc::clone(&segment),
                                    Arc::clone(&bodies),
                                    shutdown.clone(),
                                    admission,
                                ));
                            }
                            Ok(Offered::Held) => {}
                            Err(error) => {
                                tracing::debug!(lane, %error, "Link exchange refused before execution");
                                refuse_exchange(&frame, &segment, &bodies);
                            }
                        }
                    }
                    Ok(received @ (Received::Frame | Received::Work(_))) => {
                        let admission = match segment.accept_frame(lane, received, &frame) {
                            Ok(admission) => admission,
                            Err(error) => {
                                tracing::debug!(lane, %error, "Link exchange refused before execution");
                                refuse_exchange(&frame, &segment, &bodies);
                                continue;
                            }
                        };
                        // Requests went to the queue above; what reaches here
                        // carries no work, so its admission (none) is dropped.
                        drop(admission);
                        match frame_kind(&frame) {
                        Ok(FrameKind::Request) => {}
                        Ok(FrameKind::Control) => match ControlCommand::decode(&frame) {
                            Ok(command) => {
                                if command.application != self.application {
                                    tracing::warn!("lane {lane} control command named another application");
                                    continue;
                                }
                                if command.name == "status" && command.target.is_none() {
                                    // Answered to the edge that asked; an older
                                    // edge that names no origin still hears it
                                    // with every other edge.
                                    if let Err(error) = publish_application_snapshot(
                                        &state, &segment, &self.application,
                                        command.origin.map(usize::from),
                                    ) {
                                        tracing::warn!(%error, "Link application status request failed");
                                    }
                                    continue;
                                }
                                state.events().emit(LinkCommandReceived {
                                    fleet: FleetName::new(state.fleet_name()),
                                    application: self.application.clone(),
                                    name: command.name.clone(),
                                    payload: command.payload,
                                    target: command.target,
                                });
                            }
                            Err(error) => tracing::warn!("lane {lane} received malformed control command: {error}"),
                        },
                        Ok(FrameKind::Reply) => tracing::debug!("lane {lane} received a reply, which only an edge's lane takes"),
                        Ok(FrameKind::ShutdownInitiated) => match ShutdownNotice::decode(&frame) {
                            Ok(notice) => edge_shutdown(&state, EdgeShutdownInitiated {
                                edge: notice.edge,
                                fleet: FleetName::new(state.fleet_name()),
                                application: self.application.clone(),
                            }),
                            Err(error) => tracing::warn!("lane {lane} received malformed shutdown notice: {error}"),
                        },
                        Ok(FrameKind::ApplicationState) => tracing::debug!("lane {lane} received an application state notice meant for an edge"),
                        Err(error) => tracing::debug!("lane {lane} received an unreadable frame: {error}"),
                    }},
                    Err(error) => {
                        tracing::debug!("lane {lane} could not be read: {error}");
                        break;
                    }
                }
            }
        }

        // Stop being a target first, so nothing new is written into a lane
        // this process is about to stop reading, and the edge's next choice
        // lands on one that is still here.
        let _ = segment.inbox().release_lane(lane);
        // Exchanges already taken are finished, queued ones included: their
        // callers are still waiting for an answer they can have.
        while let Some(finished) = exchanges.join_next().await {
            report(finished);
            if let Some((request, admission)) = work.finished(&segment, lane) {
                exchanges.spawn(serve(
                    request,
                    lane,
                    Arc::clone(&owner),
                    Arc::clone(&segment),
                    Arc::clone(&bodies),
                    shutdown.clone(),
                    admission,
                ));
            }
        }
        // Withdrawal stops routing, not ownership. Nobody may reuse the lane
        // identity while accepted streams are still running on it.
        drop(joined);
        Ok(())
    }
}

#[cfg(test)]
#[path = "../tests/orbit.rs"]
mod tests;

impl OrbitDoor {
    /// The master's part: no lane, a directory watcher instead, so edges
    /// coming and going are heard in the process that serves nothing.
    async fn watch(
        &self,
        state: &AppState,
        context: RunContext,
    ) -> registry::Result<()> {
        let segment = ApplicationSegment::for_fleet_with_spec(
            state.fleet_name(),
            self.capacity.geometry()?.spec(),
            &claviron_orbit::link::application::Access::OwnerOnly,
        )
        .map_err(|error| registry::Error::msg(error.to_string()))?;
        let segment = Arc::new(segment);
        let mut watcher = segment
            .watch(self.runtime)
            .map_err(|error| registry::Error::msg(error.to_string()))?;
        // The owner publishes its growth policy in the fleet it owns; static
        // (no `workers` block) publishes nothing and senders report nothing.
        let policy = match self.capacity.workers()? {
            Some(workers) => workers.for_owner(
                u16::from(state.worker_capacity()),
                self.capacity.geometry()?.fleet_capacity,
                u16::from(state.worker_count()),
            )?,
            None => None,
        };
        let retirement = self.capacity.workers()?.and_then(|workers| {
            workers
                .retire_idle()
                .timeout()
                .map(|timeout| (workers.initial_count(), timeout))
        });
        let source = state.registry_ref().resolve::<RetirementSource>();
        if retirement.is_some() && source.is_none() {
            return Err(registry::Error::msg(
                "idle retirement requires the worker owner's candidate source",
            ));
        }
        let mut idle = retirement
            .map(|_| {
                IdleRetirement::new(
                    Arc::clone(&segment),
                    FleetIncarnation::FIRST.raw(),
                )
            })
            .transpose()
            .map_err(|error| registry::Error::msg(error.to_string()))?;
        let mut retiring = None;
        let mut growth = FleetGrowth::publish(&segment, policy);
        // A worker this master added has arrived once the receivers serving
        // the application differ from when it was asked for.
        let mut asked_at = None;
        // The master's own subscribers declare interest, and what it
        // publishes crosses too; it receives through its workers' lanes.
        let _link = ApplicationLinkSetup::resolve(state).and_then(|setup| {
            setup.for_watcher(&watcher, |role| role == EDGE_RUNTIME)
        });
        let mut edges = edge_peers(state);
        observe_edges(state, &mut edges, segment.inbox(), &self.application);
        loop {
            let owned = if let Some(source) = &source {
                source.0.candidate_pid().await
            } else {
                None
            };
            if retiring.is_some() && retiring != owned {
                retiring = None;
            }
            let candidate = if growth.growing() || retiring.is_some() {
                None
            } else {
                owned
            };
            let owner_changed = async {
                match &source {
                    Some(source) => source.0.changed(owned).await,
                    None => std::future::pending().await,
                }
            };
            let idle_wait = async {
                match (&mut idle, retirement, candidate) {
                    (Some(idle), Some((_, timeout)), Some(pid)) => {
                        idle.wait(pid, timeout).await
                    }
                    _ => std::future::pending().await,
                }
            };
            tokio::select! {
                biased;
                () = context.cancelled() => break,
                () = owner_changed => {},
                expired = idle_wait => {
                    match expired.map_err(|error| registry::Error::msg(error.to_string()))? {
                        Some(pid) => {
                            retiring = Some(pid);
                            state.events().emit(LinkWorkerIdle { pid, minimum: retirement.expect("enabled retirement").0 });
                        }
                        None => { /* directory change will select the next owned candidate */ }
                    }
                }
                moved = watcher.next_change() => match moved {
                    Ok(Watched::Directory(_)) => {
                        observe_edges(state, &mut edges, segment.inbox(), &self.application);
                        if retiring.is_some_and(|pid| !(0..segment.inbox().lanes()).any(|lane| segment.inbox().owner(lane).ok().flatten() == Some(pid))) {
                            retiring = None;
                            growth.arrived(&mut watcher);
                        }
                        if asked_at.is_some_and(|serving| {
                            segment.inbox().serving(&self.application).len() != serving
                        }) {
                            growth.arrived(&mut watcher);
                            asked_at = None;
                        }
                    }
                    Ok(Watched::Pressure) => {
                        if retiring.is_some() { watcher.take_pressure(); continue; }
                        if let Some(policy) = growth.pressure(&mut watcher) {
                            let serving = segment.inbox().serving(&self.application).len();
                            asked_at = Some(serving);
                            state.events().emit(LinkPressure {
                                fleet: state.fleet_name().to_owned(),
                                application: self.application.clone(),
                                policy,
                                serving,
                            });
                        }
                    }
                    Err(error) => {
                        tracing::debug!("the lane directory could not be watched: {error}");
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}

fn edge_peers(state: &AppState) -> Peers {
    Peers::new(FleetName::new(state.fleet_name()), |role| role == EDGE_RUNTIME)
}

/// Diff the directory and say what changed: in the log, and as a local event.
/// A departure before an arrival, because an edge that restarted left before
/// it came back. Every connect and disconnect is reported, reconnects too.
fn observe_edges(
    state: &AppState,
    edges: &mut Peers,
    inbox: &Inbox,
    application: &str,
) {
    let (came, went) = edges.update(inbox);
    for edge in went {
        tracing::warn!(
            fleet_name = %edge.fleet,
            "🛰 edge '{}' left '{application}' (lane {}, pid {})",
            edge.name,
            edge.lane,
            pid(edge.pid)
        );
        let last = !edges.serves(&edge.name);
        state.events().emit(EdgeDisconnected {
            edge,
            application: application.to_owned(),
            last,
        });
    }
    for edge in &came {
        tracing::info!(
            fleet_name = %edge.fleet,
            "🛰 edge '{}' reached '{application}' (lane {}, pid {})",
            edge.name,
            edge.lane,
            pid(edge.pid)
        );
        state.events().emit(EdgeConnected {
            edge: edge.clone(),
            application: application.to_owned(),
        });
    }
}

fn refuse_exchange(
    frame: &[u8],
    segment: &ApplicationSegment,
    bodies: &ApplicationBodies,
) {
    let Ok(exchange) = Exchange::decode(Bytes::copy_from_slice(frame)) else {
        return;
    };
    let mut endpoint =
        exchange.stream.and_then(|ticket| bodies.accept(ticket).ok());
    let reply = ExchangeReply {
        correlation: exchange.correlation,
        status: 503,
        headers: Vec::new(),
        body: ExchangeBody::Empty,
    };
    if let Some(endpoint) = endpoint.as_mut() {
        let _ = endpoint.finish();
    }
    if let Ok(frame) = reply.encode() {
        if segment.inbox().write(exchange.origin as usize, &frame).is_err()
            && let Some(endpoint) = endpoint.as_mut()
        {
            endpoint.reset();
        }
    } else if let Some(endpoint) = endpoint.as_mut() {
        endpoint.reset();
    }
}

fn pid(pid: Option<u32>) -> String {
    pid.map_or_else(|| "unknown".to_owned(), |pid| pid.to_string())
}

fn report(finished: Result<(), tokio::task::JoinError>) {
    if let Err(error) = finished {
        tracing::debug!("exchange task failed: {error}");
    }
}

/// One exchange, from the frame that arrived to the reply that leaves.
async fn serve<O: Owner>(
    frame: Vec<u8>,
    lane: usize,
    owner: Arc<O>,
    segment: Arc<ApplicationSegment>,
    bodies: Arc<ApplicationBodies>,
    shutdown: SessionShutdown,
    admission: Option<(usize, WorkExecution)>,
) {
    // Scope owns the admission until the response/session future has ended.
    let _admission = admission;
    match frame_kind(&frame) {
        Ok(FrameKind::Request) => {}
        Ok(FrameKind::Reply) => {
            // An application lane receives requests; a reply here was written
            // to the wrong lane, a bug in the writer rather than something to
            // answer.
            tracing::debug!(
                "lane {lane} received a reply, which only an edge's lane takes"
            );
            return;
        }
        Ok(FrameKind::Control) => {
            tracing::debug!(
                "lane {lane} received a control command in the exchange handler"
            );
            return;
        }
        Ok(FrameKind::ShutdownInitiated) => {
            tracing::debug!(
                "lane {lane} received a shutdown notice in the exchange handler"
            );
            return;
        }
        Ok(FrameKind::ApplicationState) => {
            tracing::debug!(
                "lane {lane} received application state in the exchange handler"
            );
            return;
        }
        Err(error) => {
            tracing::debug!(
                "lane {lane} received an unreadable frame: {error}"
            );
            return;
        }
    }
    let mut exchange = match Exchange::decode(Bytes::from(frame)) {
        Ok(exchange) => exchange,
        Err(error) => {
            tracing::debug!(
                "lane {lane} received a malformed exchange: {error}"
            );
            return;
        }
    };
    let origin = exchange.origin as usize;
    if _admission
        .as_ref()
        .is_some_and(|(reserved_origin, _)| *reserved_origin != origin)
    {
        tracing::warn!(
            lane,
            origin,
            "exchange origin differs from its work admission"
        );
        return;
    }
    let correlation = exchange.correlation;
    // Measured, not budgeted: read so an operator can see what a request cost.
    let arrived = Instant::now();
    tracing::debug!(
        correlation,
        origin,
        "🛰 → {} {} on lane {lane}",
        exchange.method,
        exchange.path
    );

    // The slot the edge created carries whichever body streams: the request
    // arrives on one direction and the reply leaves on the other. Opened even
    // when neither does, because its far side going away is how this side
    // learns the edge stopped waiting.
    let body = match exchange.stream {
        Some(ticket) => match bodies.accept(ticket) {
            Ok(endpoint) => Some(endpoint),
            Err(error) => {
                tracing::debug!(
                    "exchange {correlation} has no readable body: {error}"
                );
                return;
            }
        },
        None => None,
    };

    if exchange.upgrade {
        let handshake =
            Handshake { segment: Arc::clone(&segment), origin, correlation };
        match body {
            Some(endpoint) => {
                session(
                    &exchange,
                    handshake,
                    endpoint,
                    owner.as_ref(),
                    shutdown,
                )
                .await;
            }
            // The edge sent no slot for one.
            None => handshake.refuse(400, None),
        }
        return;
    }

    // A streamed request body is handed on as it arrives rather than read to
    // its end first; a body the edge cut short ends in an error, not an end.
    // An inline one is already whole, and its direction is already ended.
    let (incoming, mut reply_body) = match body {
        Some(endpoint) => {
            let (incoming, outgoing) = endpoint.split();
            (Some(incoming), Some(outgoing))
        }
        None => (None, None),
    };
    let request_body = match (std::mem::take(&mut exchange.body), incoming) {
        (ExchangeBody::Stream, Some(incoming)) => {
            let piece = declared_length(&exchange.headers)
                .map_or(segment.spec().streams.buffer_bytes, |length| {
                    length.clamp(1, segment.spec().streams.buffer_bytes)
                });
            RequestBody::new(SlotBody::new(incoming, piece))
        }
        (ExchangeBody::Stream, None) => {
            tracing::debug!(
                "exchange {correlation} streams a body it gave no stream for"
            );
            return;
        }
        (ExchangeBody::Inline(bytes), _) => {
            RequestBody::new(http_body_util::Full::new(bytes))
        }
        (ExchangeBody::Empty, _) => RequestBody::empty(),
    };
    let request = match build_request(&exchange, request_body) {
        Ok(request) => request,
        Err(reason) => {
            tracing::debug!(
                "exchange {correlation} could not be rebuilt: {reason}"
            );
            return;
        }
    };

    let Some(response) = before_head(
        reply_body.as_mut(),
        owner.call(
            request.map(RequestBody::into_inner),
            None,
            RequestArrival::Established,
        ),
    )
    .await
    else {
        tracing::debug!(correlation, "🛰 reply reader left before the head");
        return;
    };
    let (parts, response_body) = response.into_parts();
    // A reply that is whole says its exact size. Taken here, it is in hand
    // before the head goes; anything else streams after the head.
    let whole = http_body::Body::size_hint(&response_body)
        .exact()
        .is_some_and(|size| size <= segment.spec().streams.buffer_bytes as u64);
    let mut streaming = None;
    let bytes = if whole {
        let Some(collected) =
            before_head(reply_body.as_mut(), response_body.collect()).await
        else {
            tracing::debug!(correlation, "🛰 reply reader left while buffering");
            return;
        };
        match collected {
            Ok(body) => body.to_bytes(),
            Err(error) => {
                tracing::debug!(correlation, "🛰 ✗ reply body failed: {error}");
                return;
            }
        }
    } else {
        streaming = Some(response_body);
        Bytes::new()
    };
    let mut reply = ExchangeReply {
        correlation,
        status: parts.status.as_u16(),
        headers: parts
            .headers
            .iter()
            .map(|(name, value)| {
                (name.as_str().as_bytes().to_vec(), value.as_bytes().to_vec())
            })
            .collect(),
        body: match (whole, bytes.is_empty()) {
            (true, true) => ExchangeBody::Empty,
            (true, false) => ExchangeBody::Inline(bytes.clone()),
            (false, _) => ExchangeBody::Stream,
        },
    };
    let mut head = match reply.encode_head() {
        Ok(head) => head,
        Err(error) => {
            if let Some(outgoing) = reply_body.as_mut() {
                outgoing.reset();
            }
            tracing::debug!(
                correlation,
                "🛰 ✗ reply does not fit a frame: {error}"
            );
            return;
        }
    };
    let (body, spill) = fit_inline(
        &mut head,
        std::mem::take(&mut reply.body),
        segment.inbox().geometry().payload_capacity,
    );
    match (&body, spill, reply_body.as_mut()) {
        // Whole and in the frame: the stream's reply direction carries
        // nothing, and the frame says so, so it ends without waking anyone.
        (ExchangeBody::Empty | ExchangeBody::Inline(_), _, Some(outgoing)) => {
            outgoing.finish_quietly();
        }
        // Whole but larger than the frame. It goes first when it fits the
        // stream's window: ended before the head, it is waiting when the head
        // wakes the edge, which then never parks on an empty slot.
        (ExchangeBody::Stream, Some(bytes), Some(outgoing)) => {
            if let Err(error) = outgoing.write_all(&bytes).await {
                tracing::debug!(
                    correlation,
                    "🛰 ✗ reply body was cut short: {error}"
                );
                return;
            }
            if let Err(error) = outgoing.shutdown().await {
                tracing::debug!(
                    correlation,
                    "🛰 ✗ reply body did not close: {error}"
                );
            }
        }
        (ExchangeBody::Stream, Some(_), None) => {
            tracing::debug!(
                correlation,
                "🛰 ✗ reply body outgrows the frame and the edge gave no stream"
            );
            return;
        }
        _ => {}
    }
    let status = reply.status;
    if let Err(error) = publish_reply_head(
        &segment,
        origin,
        &[&head, body.inline()],
        reply_body.as_mut(),
    )
    .await
    {
        tracing::debug!(correlation, "🛰 ✗ reply was not delivered: {error}");
        return;
    }
    // The head is away: the moment the edge can start answering its client.
    let answered = arrived.elapsed();

    let mut sent = bytes.len();
    if let (false, Some(outgoing), Some(response_body)) =
        (whole, reply_body.as_mut(), streaming)
    {
        let Some(streamed) =
            stream_reply(outgoing, response_body, correlation).await
        else {
            tracing::debug!(
                correlation,
                "🛰 reply stream ended before completion"
            );
            return;
        };
        sent += streamed;
    }
    tracing::debug!(
        correlation,
        "🛰 ← {status} in {:?} (head {answered:?}, {sent} B{})",
        arrived.elapsed(),
        if whole { ", buffered" } else { "" }
    );
}

/// Own the streaming body until FIN or the response reader leaves. Waiting
/// for process death alone misses cancellation by a caller that stays alive.
async fn stream_reply<B>(
    outgoing: &mut claviron_orbit::stream::WriteHalf,
    mut body: B,
    correlation: u64,
) -> Option<usize>
where
    B: http_body::Body<Data = Bytes> + Unpin,
    B::Error: std::fmt::Display,
{
    let mut sent = 0;
    loop {
        let frame = tokio::select! {
            biased;
            _ = outgoing.peer_closed() => return None,
            frame = body.frame() => frame,
        };
        let Some(frame) = frame else { break };
        let frame = match frame {
            Ok(frame) => frame,
            Err(error) => {
                tracing::debug!(correlation, "🛰 ✗ reply body failed: {error}");
                outgoing.reset();
                return None;
            }
        };
        let Ok(part) = frame.into_data() else { continue };
        sent += part.len();
        if let Err(error) = outgoing.write_all(&part).await {
            tracing::debug!(
                correlation,
                "🛰 ✗ reply body was cut short: {error}"
            );
            return None;
        }
    }
    if let Err(error) = outgoing.shutdown().await {
        tracing::debug!(correlation, "🛰 ✗ reply body did not close: {error}");
    }
    Some(sent)
}

/// Before publishing a head, no response read can otherwise observe caller
/// cancellation. Drop only this exchange's future when its reader goes away.
async fn before_head<T>(
    outgoing: Option<&mut claviron_orbit::stream::WriteHalf>,
    work: impl Future<Output = T>,
) -> Option<T> {
    match outgoing {
        Some(outgoing) => tokio::select! {
            biased;
            _ = outgoing.peer_closed() => None,
            result = work => Some(result),
        },
        None => Some(work.await),
    }
}

/// A session attempt: an offer through the same owner as any request. What
/// stands in front of the application can answer or refuse it, and whoever claims
/// it decides. The session then runs here, as this exchange's work.
async fn session<O: Owner>(
    exchange: &Exchange,
    handshake: Handshake,
    endpoint: Endpoint,
    owner: &O,
    shutdown: SessionShutdown,
) {
    let mut request = match build_request(exchange, RequestBody::empty()) {
        Ok(request) => request,
        Err(_) => return handshake.refuse(400, Some(endpoint)),
    };
    // The version an edge marks a WebTransport stream with.
    let kind = match exchange.version {
        HttpVersion::Http3 => SocketKind::WebTransport,
        _ => SocketKind::WebSocket,
    };
    let (ended, mut outcome) = oneshot::channel();
    let offer = Offer::new(
        OfferInfo { kind, carrier: Carrier::Orbit },
        LinkDoor { handshake, ended },
    );
    request.extensions_mut().insert(offer.clone());
    let (incoming, mut outgoing) = endpoint.split();
    let reset = incoming.reset_signal();
    let answered = tokio::select! {
        biased;
        // The edge gave up on it; there is nobody left to answer.
        _ = poll_fn(|cx| reset.poll_reset(cx)) => return,
        () = shutdown.cancelled() => claviron_http::err_status(
            http::StatusCode::SERVICE_UNAVAILABLE,
        ),
        answered = owner.call(
            request.map(RequestBody::into_inner),
            None,
            RequestArrival::Established,
        ) => answered,
    };
    offer.finish(answered);
    match outcome.try_recv() {
        Ok(Ended::Accepted(deferred)) => {
            let _activity = claviron_runtime::activity::ACTIVITY.open_session();
            deferred(SocketIo::Native(Box::new(LinkSession {
                incoming,
                outgoing,
                shutdown,
            })))
            .await;
        }
        Ok(Ended::Refused { answered: true }) => {
            let _ = outgoing.shutdown().await;
        }
        _ => outgoing.reset(),
    }
}

async fn publish_reply_head(
    segment: &ApplicationSegment,
    origin: usize,
    frame: &[&[u8]],
    mut outgoing: Option<&mut claviron_orbit::stream::WriteHalf>,
) -> Result<(), String> {
    let error = loop {
        match segment.publish_reply(origin, frame) {
            Ok(()) => return Ok(()),
            // The reply exists; Edges have not yet read this worker's earlier
            // ones. Wait for one to, holding this exchange and its admission,
            // so a backlog turns into fewer admitted requests rather than a
            // lost reply. Only the asking Edge leaving ends the wait.
            Err(LinkError::ProducerBusy) => {
                match before_head(
                    outgoing.as_deref_mut(),
                    segment.record_released(),
                )
                .await
                {
                    Some(Ok(())) => {}
                    Some(Err(error)) => break error.to_string(),
                    None => {
                        break "the edge left while the reply waited".into();
                    }
                }
            }
            Err(error) => break error.to_string(),
        }
    };
    // The response direction is already owned by this exchange. RESET wakes
    // its caller even after a buffered response published FIN.
    if let Some(outgoing) = outgoing {
        outgoing.reset();
    }
    Err(error)
}

/// Rebuild the request the edge took apart.
///
/// `raw_path` rather than `path`: the transport carries both, and building
/// the URI from the raw one means the decoded one comes back out of it
/// unchanged instead of being decoded twice.
fn build_request(
    exchange: &Exchange,
    body: RequestBody,
) -> Result<AppRequest, String> {
    let mut target = exchange.raw_path.clone();
    if !exchange.query.is_empty() {
        target.push(b'?');
        target.extend_from_slice(&exchange.query);
    }
    let uri =
        Uri::try_from(target.as_slice()).map_err(|error| error.to_string())?;
    let mut request = Request::builder()
        .method(exchange.method.as_bytes())
        .uri(uri)
        .version(exchange.version.into());
    for (name, value) in &exchange.headers {
        request = request.header(name.as_slice(), value.as_slice());
    }
    let mut request = request.body(body).map_err(|error| error.to_string())?;

    // The edge accepted the connection and reported it in the exchange: a
    // trusted hop's word, which is what `Forwarded` carries. There is no
    // connection of this process's own to describe.
    if let Some(client) = exchange.client {
        request.extensions_mut().insert(RealIpExt(client.ip()));
    }
    request.extensions_mut().insert(Forwarded {
        client: exchange.client,
        server: exchange.server,
        scheme: exchange.scheme.parse().ok(),
        host: None,
    });
    Ok(request)
}

/// The length a request declared, which sizes the reads of its body.
fn declared_length(headers: &[(Vec<u8>, Vec<u8>)]) -> Option<usize> {
    headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(b"content-length"))
        .and_then(|(_, value)| {
            std::str::from_utf8(value).ok()?.trim().parse().ok()
        })
}

/// A request body read out of the exchange's slot, a part as large as the
/// window holds at once: as long as the body says it is, up to one window, so
/// a small body is one read. The buffer is reused: a part splits off what was
/// read and the rest of the allocation takes the next.
struct SlotBody<R> {
    half: R,
    buffer: bytes::BytesMut,
    piece: usize,
}

impl<R> SlotBody<R> {
    fn new(
        half: R,
        piece: usize,
    ) -> Self {
        Self { half, buffer: bytes::BytesMut::new(), piece }
    }
}

impl<R> http_body::Body for SlotBody<R>
where
    R: tokio::io::AsyncRead + Unpin,
{
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>>
    {
        use std::task::{Poll, ready};
        let this = &mut *self;
        if this.buffer.capacity() < this.piece {
            this.buffer.reserve(this.piece);
        }
        let read = ready!(poll_read_into(
            &mut this.half,
            &mut this.buffer,
            this.piece,
            context
        ));
        Poll::Ready(match read {
            Ok(0) => None,
            Ok(_) => {
                Some(Ok(http_body::Frame::data(this.buffer.split().freeze())))
            }
            Err(error) => Some(Err(error)),
        })
    }
}

/// Read into the spare capacity of `buffer`, at most `limit` bytes.
fn poll_read_into<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut R,
    buffer: &mut bytes::BytesMut,
    limit: usize,
    context: &mut std::task::Context<'_>,
) -> std::task::Poll<std::io::Result<usize>> {
    use bytes::BufMut;
    let spare = buffer.chunk_mut();
    let len = spare.len().min(limit);
    // SAFETY: `ReadBuf` treats the slice as uninitialised and records only
    // what the reader fills; `advance_mut` then exposes exactly that much.
    let slice = unsafe {
        std::slice::from_raw_parts_mut(spare.as_mut_ptr().cast(), len)
    };
    let mut read = tokio::io::ReadBuf::uninit(slice);
    std::task::ready!(Pin::new(reader).poll_read(context, &mut read))?;
    let filled = read.filled().len();
    unsafe { buffer.advance_mut(filled) };
    std::task::Poll::Ready(Ok(filled))
}
