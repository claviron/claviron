use std::future::Future;
use std::sync::Arc;

use orbitive::events::{DefaultEventLayout, EventLayout, FleetEventBus};
use orbitive::link::LinkEvents;

use super::{
    Connection, InterestGuard, LinkInterest, Received, Result, Routing,
};

/// A connection observation handed to the caller's protocol handler.
///
/// Event topics/payloads retain the sender's wire representation. Interest is
/// a routing hint, not authorization: the handler still validates the exact
/// topic, schema and publisher policy. Unknown ordinary frames are exposed,
/// not interpreted as HTTP or silently discarded.
#[derive(Debug, PartialEq, Eq)]
pub enum Incoming {
    Event { topic: String, payload: Vec<u8> },
    Frame(Vec<u8>),
    Directory(u64),
}

/// Topic/payload events on an existing Link connection, with no extra service.
///
/// The supplied bus is the host's explicitly prepared SHM event bus; opening
/// this binding creates no event bus. A standalone service may prepare its own
/// SHM bus without a supervised Claviron application. An in-memory bus has no
/// native ring readiness and is not supported by `run_until`; it fails rather
/// than installing a polling fallback. Local publications cross only from
/// their publishing node; the receiving side
/// republishes once into its bus. `run_until` also hands the received event to
/// this process's handler. Do not deliver it a second time from an own-event
/// subscription. Delivery is bounded, without durable replay or remote ACKs.
pub struct EventConnection<L: EventLayout = DefaultEventLayout> {
    // Withdraw bridge interest before releasing the lane to a successor.
    bridge: LinkEvents<L>,
    connection: Connection,
    bus: Arc<FleetEventBus<L>>,
    interest: Arc<LinkInterest>,
}

impl<L: EventLayout> EventConnection<L> {
    pub(super) fn new(
        connection: Connection,
        bus: Arc<FleetEventBus<L>>,
        far: fn(&str) -> bool,
        routing: Routing,
    ) -> Result<Self> {
        let bridge = LinkEvents::for_lane(
            connection.segment(),
            connection.lane(),
            Arc::clone(&bus),
            far,
            routing,
        )?;
        Ok(Self {
            bridge,
            connection,
            bus,
            interest: Arc::new(LinkInterest::default()),
        })
    }

    /// Publish through this bus from other callbacks while `run_until` owns
    /// the reader. Publication commits locally; the driver forwards it.
    pub fn bus(&self) -> Arc<FleetEventBus<L>> {
        Arc::clone(&self.bus)
    }

    /// Connection-local subscription keys. Hold the returned guard while the
    /// caller can decode them. No separate event-service name is registered.
    pub fn subscribe<I, K>(
        &self,
        keys: I,
    ) -> InterestGuard
    where
        I: IntoIterator<Item = K>,
        K: Into<String>,
    {
        self.interest.add(keys)
    }

    /// A shared declaration handle for subscriptions changed by other tasks.
    pub fn interest(&self) -> Arc<LinkInterest> {
        Arc::clone(&self.interest)
    }

    /// Drive this connection until its owning service cancels it.
    ///
    /// A Claviron/Continuo Runnable passes its `RunContext::cancelled()`
    /// future. No task, interval or process signal handler is installed.
    /// Cancellation withdraws new admission, then awaits the currently
    /// accepted handler without an invented drain deadline. The owner must
    /// not abort this future if it requires that handler's graceful drain.
    /// After return the connection is released; retained SHM is preserved.
    ///
    /// `E` lets a domain handler retain its own error type. A transport error
    /// also ends admission and drains the accepted handler before returning.
    pub async fn run_until<S, F, H, E>(
        mut self,
        shutdown: S,
        mut handle: F,
    ) -> std::result::Result<(), E>
    where
        S: Future<Output = ()>,
        F: FnMut(Incoming) -> H,
        H: Future<Output = std::result::Result<(), E>>,
        E: From<super::Error>,
    {
        let mut interest = self.interest.watch();
        self.bridge.interest(&interest.borrow_and_update()[..]);
        let driving = self.bridge.drive();
        tokio::pin!(driving, shutdown);
        let mut frame = Vec::new();
        loop {
            // Immediately ready SHM must not starve the owner's cancellation.
            tokio::task::consume_budget().await;
            let received = tokio::select! {
                biased;
                () = &mut shutdown => {
                    self.connection.withdraw()?;
                    self.bridge.pump()?;
                    return Ok(());
                }
                result = &mut driving => {
                    result?;
                    return Ok(());
                }
                changed = interest.changed() => {
                    if changed.is_err() {
                        return Err(super::Error::Malformed(
                            "the Link interest owner is unavailable",
                        ).into());
                    }
                    self.bridge.interest(&interest.borrow_and_update()[..]);
                    continue;
                }
                result = self.connection.next(&mut frame) => result?,
            };
            let incoming = match received {
                Received::Event => {
                    let (topic, payload) = self.bridge.accept(&frame)?;
                    Incoming::Event {
                        topic: topic.to_owned(),
                        payload: payload.to_vec(),
                    }
                }
                Received::Frame => Incoming::Frame(std::mem::take(&mut frame)),
                Received::Directory(generation) => {
                    Incoming::Directory(generation)
                }
            };
            let handling = handle(incoming);
            tokio::pin!(handling);
            tokio::select! {
                biased;
                () = &mut shutdown => {
                    let withdrawn = self.connection.withdraw();
                    handling.await?;
                    withdrawn?;
                    self.bridge.pump()?;
                    return Ok(());
                }
                result = &mut driving => {
                    let withdrawn = self.connection.withdraw();
                    handling.await?;
                    withdrawn?;
                    result?;
                    return Ok(());
                }
                result = &mut handling => result?,
            }
        }
    }
}
