use std::future::{Future, pending};
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use futures_util::stream::{FuturesUnordered, StreamExt};
use orbitive::events::{DefaultEventLayout, EventLayout};
use orbitive::link::LinkEvents;
use orbitive::stream::{Endpoint, Side};
use tokio::io::AsyncWriteExt;

use super::frame::{Body, Frame, HEADER, TAG};
use super::{Call, Handle, Limits, Peer, Shared, State};
use crate::invoke::{
    CallError as Error, CallResult as Result, Failure, Request,
};
use crate::link::{Connection, Incoming, LinkInterest, Received};

pub(super) struct Events<L: EventLayout> {
    pub bridge: LinkEvents<L>,
    pub interest: Arc<LinkInterest>,
}

/// Exactly one inbox reader, owned and polled by the hosting service.
pub struct Driver<L: EventLayout = DefaultEventLayout> {
    connection: Connection,
    events: Option<Events<L>>,
    shared: Arc<Shared>,
}

type Job<'a> = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

impl<L: EventLayout> Driver<L> {
    pub(super) fn new(
        connection: Connection,
        events: Option<Events<L>>,
        limits: Limits,
    ) -> Result<(Handle, Self)> {
        let spec = connection.segment().spec();
        limits.validate(spec.inbox.payload_capacity)?;
        if spec.streams.buffer_bytes < HEADER + 4 {
            return Err(Error::InvalidConfig(
                "result slot cannot hold a rejection record",
            ));
        }
        let lane = connection.lane();
        let incarnation = connection.segment().inbox().incarnation(lane)?;
        let bodies = connection.segment().bodies(lane, incarnation)?;
        let shared = Arc::new(Shared {
            segment: connection.segment().clone(),
            bodies,
            local: Peer { lane: lane as u16, incarnation },
            limits,
            state: Mutex::new(State::default()),
            changed: tokio::sync::Notify::new(),
        });
        Ok((Handle(shared.clone()), Self { connection, events, shared }))
    }

    fn changed_peers(&self) {
        let mut state =
            self.shared.state.lock().expect("invoke state poisoned");
        let gone: Vec<_> = state
            .pending
            .iter()
            .filter_map(|(&id, call)| {
                (!self.shared.live(call.peer)).then_some(id)
            })
            .collect();
        for id in gone {
            if let Some(call) = state.pending.remove(&id) {
                let _ = call.result.send(Err(Error::OutcomeUnknown));
            }
        }
    }

    /// Drive calls and optional events without awaiting one handler in the
    /// reader. Callback futures are polled here, not spawned. Hand blocking
    /// work to the host's bounded executor. Executing observations and calls
    /// share `accepted_calls`. One just-read observation may be held until a
    /// callback slot opens; the reader then applies native inbox backpressure.
    ///
    /// Each caller reserves a generation-protected Link stream slot before
    /// publishing. The service claims its result side once, before execution.
    /// Results therefore cannot be lost to a full event/call inbox. Abandoned
    /// result readers affect only their call, never stop the service.
    ///
    /// Shutdown closes local call admission, rejects new requests with Closing
    /// and drains accepted work and outgoing calls before withdrawing the lane.
    /// Do not abort this future when graceful ownership is required. Caller
    /// cancellation is not rollback. No timeout, polling or retry is installed.
    pub async fn run_until<'a, S, F, H, O, G>(
        mut self,
        shutdown: S,
        mut handle: F,
        mut observe: G,
    ) -> Result<()>
    where
        S: Future<Output = ()>,
        F: FnMut(Call) -> H,
        H: Future<Output = std::result::Result<Vec<u8>, Failure>> + Send + 'a,
        G: FnMut(Incoming) -> O,
        O: Future<Output = Result<()>> + Send + 'a,
    {
        let events = self.events.take();
        let mut interest =
            events.as_ref().map(|events| events.interest.watch());
        if let (Some(events), Some(watch)) = (&events, &mut interest) {
            events.bridge.interest(&watch.borrow_and_update()[..]);
        }
        let driving = async {
            match &events {
                Some(events) => events.bridge.drive().await,
                None => pending().await,
            }
        };
        tokio::pin!(driving, shutdown);
        let mut jobs: FuturesUnordered<Job<'a>> = FuturesUnordered::new();
        let mut closing = false;
        let mut reader_failed = false;
        let mut bridge_failed = false;
        let mut error = None;
        let mut bytes = Vec::new();
        let mut deferred = None;
        self.shared.state.lock().expect("invoke state poisoned").running = true;
        loop {
            tokio::task::consume_budget().await;
            if jobs.len() < self.shared.limits.accepted_calls
                && let Some(incoming) = deferred.take()
            {
                jobs.push(Box::pin(observe(incoming)));
            }
            if closing
                && jobs.is_empty()
                && deferred.is_none()
                && self
                    .shared
                    .state
                    .lock()
                    .expect("invoke state poisoned")
                    .pending
                    .is_empty()
            {
                break;
            }
            let step: Result<Option<Incoming>> = tokio::select! {
                biased;
                () = &mut shutdown, if !closing => {
                    closing = true;
                    self.shared.state.lock().expect("invoke state poisoned").running = false;
                    continue;
                }
                completed = jobs.next(), if !jobs.is_empty() => completed.expect("nonempty jobs").map(|()| None),
                () = self.shared.changed.notified() => { continue; }
                result = &mut driving, if !bridge_failed => {
                    bridge_failed = true;
                    match result { Ok(()) => Err(Error::Transport("event bridge ended".to_owned())), Err(error) => Err(error.into()) }
                }
                changed = async {
                    match &mut interest { Some(watch) => watch.changed().await, None => pending().await }
                } => {
                    if changed.is_err() { Err(Error::Closed) } else {
                        if let (Some(events), Some(watch)) = (&events, &mut interest) {
                            events.bridge.interest(&watch.borrow_and_update()[..]);
                        }
                        continue;
                    }
                }
                received = self.connection.next(&mut bytes), if !reader_failed && deferred.is_none() => {
                    match received {
                        Err(error) => { reader_failed = true; Err(error.into()) }
                        Ok(Received::Directory(generation)) => {
                            self.changed_peers();
                            Ok(Some(Incoming::Directory(generation)))
                        }
                        Ok(Received::Event) => match &events {
                            Some(events) => events.bridge.accept(&bytes).map(|(topic, payload)| {
                                Some(Incoming::Event { topic: topic.to_owned(), payload: payload.to_vec() })
                            }).map_err(Error::from),
                            None => Ok(Some(Incoming::Frame(std::mem::take(&mut bytes)))),
                        },
                        Ok(Received::Frame) => Ok(Some(Incoming::Frame(std::mem::take(&mut bytes)))),
                    }
                }
            };
            let incoming = match step {
                Ok(Some(incoming)) => incoming,
                Ok(None) => continue,
                Err(failure) => {
                    error.get_or_insert(failure);
                    closing = true;
                    self.shared
                        .state
                        .lock()
                        .expect("invoke state poisoned")
                        .close();
                    continue;
                }
            };
            if let Incoming::Frame(bytes) = &incoming
                && bytes.first() == Some(&TAG)
            {
                let frame = match Frame::decode(bytes) {
                    Ok(frame) => frame,
                    Err(error) => {
                        tracing::debug!(%error, "rejected malformed Link invoke frame");
                        continue;
                    }
                };
                if frame.to != self.shared.local
                    || !self.shared.live(frame.from)
                {
                    continue;
                }
                let Frame { from, to, sequence, reply_limit, ticket, body } =
                    frame;
                let Body::Request { operation, payload } = body else {
                    continue;
                };
                let Some(ticket) = ticket else {
                    continue;
                };
                if ticket.side != Side::B
                    || ticket.id.node() != from.lane
                    || ticket.id.kind()
                        != self.shared.segment.spec().streams.kind
                {
                    continue;
                }
                // This claim rejects duplicate requests before domain work.
                let mut endpoint = match self.shared.bodies.accept(ticket) {
                    Ok(endpoint) => endpoint,
                    Err(error) => {
                        tracing::debug!(%error, "invoke result reservation is unavailable");
                        continue;
                    }
                };
                let limits = self.shared.limits;
                let rejection = if closing {
                    Some(Failure::Closing)
                } else if jobs.len() >= limits.accepted_calls {
                    Some(Failure::Busy)
                } else if operation.len() > limits.operation_bytes
                    || payload.len() > limits.request_bytes
                {
                    Some(Failure::Rejected(Vec::new()))
                } else {
                    None
                };
                let mut reply = Frame {
                    from: to,
                    to: from,
                    sequence,
                    reply_limit: 0,
                    ticket: None,
                    body: Body::Reply(Ok(Vec::new())),
                };
                if let Some(failure) = rejection {
                    reply.body = Body::Reply(Err(failure));
                    // A newly claimed result slot is empty. Configuration
                    // guarantees room for this fixed, payload-free record.
                    let bytes = record(&reply)?;
                    if endpoint.try_write_exact(&bytes).is_ok() {
                        let _ = endpoint.finish();
                    }
                } else {
                    let future = handle(Call {
                        peer: from,
                        request: Request { operation, payload },
                    });
                    jobs.push(Box::pin(async move {
                        let result = future.await;
                        let length = match &result {
                            Ok(payload) | Err(Failure::Rejected(payload)) => {
                                payload.len()
                            }
                            _ => 0,
                        };
                        reply.body = Body::Reply(
                            if length
                                > limits
                                    .response_bytes
                                    .min(reply_limit as usize)
                            {
                                Err(Failure::ResponseTooLarge)
                            } else {
                                result
                            },
                        );
                        send_result(endpoint, reply).await
                    }));
                }
                continue;
            }
            if !closing {
                if jobs.len() >= self.shared.limits.accepted_calls {
                    // This is the reader's current frame, not another queue.
                    // Stop reading until it can enter the bounded executor.
                    deferred = Some(incoming);
                } else {
                    jobs.push(Box::pin(observe(incoming)));
                }
            }
        }
        self.connection.withdraw()?;
        if let Some(events) = &events {
            events.bridge.pump()?;
        }
        match error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

fn record(reply: &Frame) -> Result<Vec<u8>> {
    let frame = reply.encode()?;
    let length =
        u32::try_from(frame.len()).map_err(|_| Error::PayloadTooLarge)?;
    let mut bytes = Vec::with_capacity(4 + frame.len());
    bytes.extend_from_slice(&length.to_le_bytes());
    bytes.extend_from_slice(&frame);
    Ok(bytes)
}

async fn send_result(
    endpoint: Endpoint,
    reply: Frame,
) -> Result<()> {
    let bytes = record(&reply)?;
    let (_reader, mut writer) = endpoint.split();
    if let Err(error) = writer.write_all(&bytes).await {
        tracing::debug!(%error, "invoke caller no longer receives its result");
        return Ok(());
    }
    if let Err(error) = writer.shutdown().await {
        tracing::debug!(%error, "invoke result channel closed");
    }
    Ok(())
}

impl<L: EventLayout> Drop for Driver<L> {
    fn drop(&mut self) {
        self.shared.state.lock().expect("invoke state poisoned").close();
    }
}
