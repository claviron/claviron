//! The ingress with no socket.
//!
//! Nothing here binds a port. The process joins its configured fleet, says
//! which application it serves and what answers it, and waits on its lane; a
//! Claviron edge asked for that name writes an exchange into it. The service
//! that answers is the one the socket ingress calls, so an application never
//! learns which door a request came through.
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

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use claviron_app::{
    APPLICATION_EVENT_TOPIC, ApplicationEvent, EventArrayKey, EventValue,
};
use claviron_http::ext::RealIpExt;
use claviron_http::trust::Forwarded;
use claviron_orbit::placement::{Scope, Scoped};
pub use claviron_orbit::transport::Endpoint;
use claviron_orbit::transport::{
    ApplicationLinkSetup, ControlCommand, ControlTarget, EDGE_RUNTIME,
    EdgeConnected, EdgeDisconnected, Exchange, ExchangeReply, FrameKind, Inbox,
    LinkJoined, Peers, Received, ShutdownNotice, TRANSPORT_BUFFER_BYTES,
    Ticket, TransportBodies, TransportSegment, frame_kind,
};
use claviron_orbit::{ApplicationEventRegistry, FleetEventCodec};
use claviron_runtime::{
    AppState, Component, Event, FleetIncarnation, FleetName, RunContext,
    Runnable, ServiceStartPolicy, registry,
};
use http::{Request, Uri};
use http_body_util::BodyExt;
use tokio::io::AsyncWriteExt;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::routing::{AppRequest, Application, Owner, RequestBody};

/// A message session the edge carries for the owner: an H1/H2 WebSocket or
/// one reliable WebTransport bidi stream. The original request, the message
/// slot and its one acceptance answer reach the same owner callback.
pub type OrbitSession = Arc<
    dyn Fn(
            AppRequest,
            Endpoint,
            Handshake,
        ) -> Pin<Box<dyn Future<Output = ()> + Send>>
        + Send
        + Sync,
>;

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

/// Link's session acknowledgment: `101` with the owner's accepted headers, or
/// an error status. Edge translates acceptance to the native H1/H2/H3 response.
pub struct Handshake {
    segment: Arc<TransportSegment>,
    origin: usize,
    correlation: u64,
    body: Option<Ticket>,
}

impl Handshake {
    pub fn answer(
        self,
        status: u16,
        headers: Vec<(Vec<u8>, Vec<u8>)>,
    ) {
        self.try_answer(status, headers);
    }

    /// Publish the Link acknowledgment and report whether it was delivered.
    /// An owner must reset its endpoint when this fails, so the edge's pending
    /// admission wakes even when its inbox could not take the reply.
    pub fn try_answer(
        self,
        status: u16,
        headers: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> bool {
        let correlation = self.correlation;
        let reply =
            ExchangeReply { correlation, status, headers, body: self.body };
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

/// This process's place in its configured transport fleet, as a runnable Claviron
/// owns.
pub struct OrbitIngress {
    application: String,
    runtime: &'static str,
    max_in_flight: Option<usize>,
}

/// A Link command delivered to the application. The runner does not execute
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

impl OrbitIngress {
    /// `runtime` is what the lane says answers the name (`php`, `asgi`); a
    /// process of another runtime given the same name is refused at its join
    /// rather than splitting this one's requests. `max_in_flight` bounds the
    /// exchanges taken and not yet answered; `None` is no bound.
    pub fn new(
        application: impl Into<String>,
        runtime: &'static str,
        max_in_flight: Option<usize>,
    ) -> Self {
        Self { application: application.into(), runtime, max_in_flight }
    }
}

impl Component<AppState> for OrbitIngress {
    type Deps =
        (Arc<Application>, Option<Arc<Owner>>, Option<Arc<FleetIncarnation>>);

    fn name(&self) -> &'static str {
        "OrbitIngress"
    }

    fn as_scoped(&self) -> Option<&dyn Scoped> {
        Some(self)
    }

    fn as_runnable(
        self: Arc<Self>
    ) -> Option<Arc<dyn Runnable<AppState, Self::Deps>>> {
        Some(self)
    }
}

/// Workers serve on their lanes; the master watches the directory. Without
/// this the runtime would place it on workers only, its default.
impl Scoped for OrbitIngress {
    fn scope(&self) -> Scope {
        Scope::Any
    }
}

#[async_trait]
impl Runnable<AppState, <Self as Component<AppState>>::Deps> for OrbitIngress {
    /// A master serves no traffic and takes no lane: its workers each take
    /// their own, so an edge sees as many targets as there are workers. It
    /// still watches the directory, so it hears edges come and go.
    fn start_policy(
        &self,
        _state: &AppState,
    ) -> ServiceStartPolicy {
        ServiceStartPolicy::Automatic
    }

    async fn run(
        self: Arc<Self>,
        (application, owner, incarnation): <Self as Component<AppState>>::Deps,
        state: AppState,
        context: RunContext,
    ) -> registry::Result<()> {
        if state.is_master() {
            return self.watch(&state, context).await;
        }
        let sessions = owner.and_then(|owner| owner.orbit_session());
        // The master's count of this worker slot's lives. It stamps the lane
        // and the streams, so a death report that names the process before
        // this one cannot end this one's work.
        let incarnation =
            incarnation.map_or(FleetIncarnation::FIRST, |held| *held).raw();

        let segment = Arc::new(
            TransportSegment::for_fleet(state.fleet_name())
                .map_err(|error| registry::Error::msg(error.to_string()))?,
        );
        let joined = segment
            .join(&self.application, self.runtime, incarnation)
            .map_err(|error| registry::Error::msg(error.to_string()))?;
        let lane = joined.lane;
        let bodies = Arc::new(
            TransportBodies::for_fleet(state.fleet_name(), lane, incarnation)
                .map_err(|error| registry::Error::msg(error.to_string()))?,
        );
        if let Err(error) = crate::management::publish_application_snapshot(
            &state,
            &segment,
            &self.application,
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
            .receiver(lane)
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
        let room = |exchanges: &JoinSet<()>| {
            self.max_in_flight.is_none_or(|limit| exchanges.len() < limit)
        };

        let mut exchanges = JoinSet::new();
        let shutdown = SessionShutdown { token: context.cancellation_token() };
        let mut frame = Vec::new();
        loop {
            tokio::select! {
                biased;
                () = context.cancelled() => break,
                Some(finished) = exchanges.join_next(), if !exchanges.is_empty() => report(finished),
                arrived = receiver.next(&mut frame), if room(&exchanges) => match arrived {
                    Ok(Received::Directory(_)) => observe_edges(
                        &state,
                        &mut edges,
                        segment.inbox(),
                        &self.application
                    ),
                    Ok(Received::Event) => match &link {
                        Some(link) => link.accept(&frame),
                        None => tracing::debug!("lane {lane} received an application event it bridges nowhere"),
                    },
                    Ok(Received::Frame) => match frame_kind(&frame) {
                        Ok(FrameKind::Request) => {
                            exchanges.spawn(serve(
                                std::mem::take(&mut frame),
                                lane,
                                Arc::clone(&application),
                                sessions.clone(),
                                Arc::clone(&segment),
                                Arc::clone(&bodies),
                                shutdown.clone()
                            ));
                        }
                        Ok(FrameKind::Control) => match ControlCommand::decode(&frame) {
                            Ok(command) => {
                                if command.application != self.application {
                                    tracing::warn!("lane {lane} control command named another application");
                                    continue;
                                }
                                if command.name == "status" && command.target.is_none() {
                                    if let Err(error) = crate::management::publish_application_snapshot(
                                        &state, &segment, &self.application,
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
                    },
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
        // Exchanges already taken are finished: their callers are still
        // waiting for an answer they can have.
        while let Some(finished) = exchanges.join_next().await {
            report(finished);
        }
        // Withdrawal stops routing, not ownership. Nobody may reuse the lane
        // identity while accepted streams are still running on it.
        drop(joined.hold);
        Ok(())
    }
}

#[cfg(test)]
#[path = "../tests/orbit.rs"]
mod tests;

impl OrbitIngress {
    /// The master's part: no lane, a directory watcher instead, so edges
    /// coming and going are heard in the process that serves nothing.
    async fn watch(
        &self,
        state: &AppState,
        context: RunContext,
    ) -> registry::Result<()> {
        let segment = TransportSegment::for_fleet(state.fleet_name())
            .map_err(|error| registry::Error::msg(error.to_string()))?;
        let mut watcher = segment
            .watch(self.runtime)
            .map_err(|error| registry::Error::msg(error.to_string()))?;
        // The master's own subscribers declare interest, and what it
        // publishes crosses too; it receives through its workers' lanes.
        let _link = ApplicationLinkSetup::resolve(state).and_then(|setup| {
            setup.for_watcher(&watcher, |role| role == EDGE_RUNTIME)
        });
        let mut edges = edge_peers(state);
        observe_edges(state, &mut edges, segment.inbox(), &self.application);
        loop {
            tokio::select! {
                biased;
                () = context.cancelled() => break,
                moved = watcher.next() => match moved {
                    Ok(_) => observe_edges(
                        state,
                        &mut edges,
                        segment.inbox(),
                        &self.application
                    ),
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
    for edge in came {
        tracing::info!(
            fleet_name = %edge.fleet,
            "🛰 edge '{}' reached '{application}' (lane {}, pid {})",
            edge.name,
            edge.lane,
            pid(edge.pid)
        );
        state
            .events()
            .emit(EdgeConnected { edge, application: application.to_owned() });
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
async fn serve(
    frame: Vec<u8>,
    lane: usize,
    application: Arc<Application>,
    sessions: Option<OrbitSession>,
    segment: Arc<TransportSegment>,
    bodies: Arc<TransportBodies>,
    shutdown: SessionShutdown,
) {
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
    let exchange = match Exchange::decode(&frame) {
        Ok(exchange) => exchange,
        Err(error) => {
            tracing::debug!(
                "lane {lane} received a malformed exchange: {error}"
            );
            return;
        }
    };
    let origin = exchange.origin as usize;
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

    // The slot the edge created carries both bodies: the request arrives on
    // one direction and the reply leaves on the other.
    let body = match exchange.body {
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
        let handshake = Handshake {
            segment: Arc::clone(&segment),
            origin,
            correlation,
            body: exchange.body,
        };
        let request = session_request(&exchange, shutdown);
        // A Pusher client's WebSocket is a broadcast session, served here
        // and not by the owner. The session is held before it is
        // acknowledged, so a 101 never announces one that could not start.
        #[cfg(feature = "broadcast")]
        if let (Ok(request), Some(broadcast)) =
            (&request, application.broadcast())
            && let Some(accepting) = broadcast.accept_orbit(request)
        {
            let host = request
                .headers()
                .get(http::header::HOST)
                .and_then(|host| host.to_str().ok())
                .unwrap_or_default()
                .to_owned();
            let webtransport = crate::broadcast::is_webtransport(request);
            let outcome = accepting.await;
            match (outcome, body) {
                (Ok(session), Some(mut endpoint)) => {
                    if !handshake.try_answer(101, Vec::new()) {
                        endpoint.reset();
                        return;
                    }
                    let _activity =
                        claviron_runtime::activity::ACTIVITY.open_session();
                    if let Err(error) = crate::broadcast::serve_orbit(
                        session,
                        host,
                        webtransport,
                        endpoint,
                    )
                    .await
                    {
                        tracing::debug!(
                            correlation,
                            "🛰 broadcast session ended: {error}"
                        );
                    }
                }
                (Ok(_), None) => {
                    handshake.refuse(400, None);
                }
                (Err(status), endpoint) => {
                    handshake.refuse(status.as_u16(), endpoint);
                }
            }
            return;
        }
        match (sessions, body, request) {
            (Some(sessions), Some(endpoint), Ok(request)) => {
                let _activity =
                    claviron_runtime::activity::ACTIVITY.open_session();
                sessions(request, endpoint, handshake).await;
            }
            // Nothing here speaks WebSocket, or the edge sent no slot for one.
            (None, endpoint, _) => {
                handshake.refuse(501, endpoint);
            }
            (_, endpoint, _) => {
                handshake.refuse(400, endpoint);
            }
        }
        return;
    }

    // The request body is handed on as it arrives rather than read to its end
    // first; a body the edge cut short ends in an error, not an end.
    let (mut reply_body, request_body) = match body {
        Some(endpoint) => {
            let (incoming, outgoing) = endpoint.split();
            let piece = declared_length(&exchange.headers)
                .map_or(TRANSPORT_BUFFER_BYTES, |length| {
                    length.clamp(1, TRANSPORT_BUFFER_BYTES)
                });
            (Some(outgoing), RequestBody::new(SlotBody::new(incoming, piece)))
        }
        None => (None, RequestBody::empty()),
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

    let Some(response) =
        before_head(reply_body.as_mut(), application.call(request)).await
    else {
        tracing::debug!(correlation, "🛰 reply reader left before the head");
        return;
    };
    let (parts, response_body) = response.into_parts();
    // A reply that is whole says its exact size. Taken here, it is in hand
    // before the head goes; anything else streams after the head.
    let whole = http_body::Body::size_hint(&response_body)
        .exact()
        .is_some_and(|size| size <= TRANSPORT_BUFFER_BYTES as u64);
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
    let reply = ExchangeReply {
        correlation,
        status: parts.status.as_u16(),
        headers: parts
            .headers
            .iter()
            .map(|(name, value)| {
                (name.as_str().as_bytes().to_vec(), value.as_bytes().to_vec())
            })
            .collect(),
        body: exchange.body,
    };

    // A whole body goes first when it fits the stream's window: ended before
    // the head, it is waiting when the head wakes the edge, which then never
    // parks on an empty slot. A larger body would wait for a reader not yet
    // told to read, so it follows the head.
    if whole && let Some(outgoing) = reply_body.as_mut() {
        if !bytes.is_empty()
            && let Err(error) = outgoing.write_all(&bytes).await
        {
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
    let status = reply.status;
    match reply.encode() {
        Ok(frame) => {
            if let Err(error) = publish_reply_head(
                segment.inbox(),
                origin,
                &frame,
                reply_body.as_mut(),
            ) {
                tracing::debug!(
                    correlation,
                    "🛰 ✗ reply was not delivered: {error}"
                );
                return;
            }
        }
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
    }
    // The head is away: the moment the edge can start answering its client.
    let answered = arrived.elapsed();

    let mut sent = bytes.len();
    if let (false, Some(outgoing), Some(mut response_body)) =
        (whole, reply_body.as_mut(), streaming)
    {
        // Streamed: each part to the slot as the application sends it, so a
        // server-sent event reaches the edge when it is produced.
        loop {
            let frame = tokio::select! {
                biased;
                _ = std::future::poll_fn(|cx| outgoing.poll_peer_dead(cx)) => {
                    tracing::debug!(correlation, "🛰 reply stream owner is gone");
                    return;
                }
                frame = response_body.frame() => frame,
            };
            let Some(frame) = frame else { break };
            let part = match frame {
                Ok(frame) => match frame.into_data() {
                    Ok(part) => part,
                    Err(_) => continue,
                },
                Err(error) => {
                    tracing::debug!(
                        correlation,
                        "🛰 ✗ reply body failed: {error}"
                    );
                    outgoing.reset();
                    return;
                }
            };
            sent += part.len();
            if let Err(error) = outgoing.write_all(&part).await {
                tracing::debug!(
                    correlation,
                    "🛰 ✗ reply body was cut short: {error}"
                );
                return;
            }
        }
        if let Err(error) = outgoing.shutdown().await {
            tracing::debug!(
                correlation,
                "🛰 ✗ reply body did not close: {error}"
            );
        }
    }
    tracing::debug!(
        correlation,
        "🛰 ← {status} in {:?} (head {answered:?}, {sent} B{})",
        arrived.elapsed(),
        if whole { ", buffered" } else { "" }
    );
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

fn session_request(
    exchange: &Exchange,
    shutdown: SessionShutdown,
) -> Result<AppRequest, String> {
    let mut request = build_request(exchange, RequestBody::empty())?;
    request.extensions_mut().insert(shutdown);
    Ok(request)
}

fn publish_reply_head(
    inbox: &Inbox,
    origin: usize,
    frame: &[u8],
    outgoing: Option<&mut claviron_orbit::stream::WriteHalf>,
) -> Result<(), String> {
    inbox.write(origin, frame).map(|_| ()).map_err(|error| {
        // The response direction is already owned by this exchange. RESET
        // wakes its caller even after a buffered response published FIN.
        if let Some(outgoing) = outgoing {
            outgoing.reset();
        }
        error.to_string()
    })
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
