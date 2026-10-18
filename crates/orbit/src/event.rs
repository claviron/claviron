//! Typed bridge between raw Orbit events and claviron's local
//! `state.events()` bus.
//!
//! `orbitive::events` owns the raw transport: one event ring, string topics,
//! byte payloads, and cursors. This module adds Claviron semantics:
//! typed codecs, topic registration, local process-event dispatch, and
//! optional local-to-Orbit forwarding.

use std::any::TypeId;
use std::collections::{HashMap, VecDeque};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use claviron_runtime::events::Event;
use claviron_runtime::registry::{
    self, Bootable, Component, ComponentOrder, Runnable, priority,
};
use claviron_runtime::{AppState, RunContext};
use orbitive::RingSpec;
use orbitive::events::{
    DefaultEventLayout, EventLayout, FleetEventBus, FleetEventCursor,
};
use tracing::{debug, trace, warn};

use crate::placement::{Scope, Scoped};
use crate::shm::{
    ExistingBootPolicy, FinalizePolicy, ManagedShmProvider,
    ManagedShmResources, OrbitRingResource,
};
use crate::{FleetProvider, shm};

/// Cadence of the polling fallback. `drive_polling` is reachable on every
/// platform: the readiness arms above fall back to it whenever a ring has no
/// event fd, so this is not macOS-only.
const FALLBACK_POLL_INTERVAL: Duration = Duration::from_millis(10);
/// Typed event codec for Orbit-carried process events.
///
/// The topic is the stable cross-process name. The payload is arbitrary
/// bytes chosen by the event owner. `orbitive::events` never sees this trait; it
/// only carries the topic and bytes.
pub trait FleetEventCodec: Event + Sized {
    const TOPIC: &'static str;

    fn encode_orbit(&self) -> Vec<u8>;

    fn decode_orbit(bytes: &[u8]) -> Result<Self, String>;
}

/// A log reopen reaches every process of the fleet: the master, where the
/// operator's signal lands, publishes it, and each worker emits it locally.
impl FleetEventCodec for claviron_runtime::ReopenLogs {
    const TOPIC: &'static str = "claviron.logs.reopen.v1";

    fn encode_orbit(&self) -> Vec<u8> {
        Vec::new()
    }

    fn decode_orbit(bytes: &[u8]) -> Result<Self, String> {
        if bytes.is_empty() {
            Ok(Self)
        } else {
            Err(format!(
                "log reopen carries no payload, got {} bytes",
                bytes.len()
            ))
        }
    }
}

/// One Orbit event ring and the registry that serves it.
///
/// Claviron runs two: the fleet ring carries service events between master
/// and workers in small frames; the application ring carries what PHP and JS
/// applications publish and receive, sized for their payloads. Each is its own
/// SHM ring with its own retained window, so neither competes with the other.
pub trait EventRing: EventLayout {
    /// Provider name, also the log label.
    const NAME: &'static str;
    const SHM_PROVIDER_NAME: &'static str;
    const SHM_RESOURCE_NAME: &'static str;
}

impl EventRing for DefaultEventLayout {
    const NAME: &'static str = "FleetEventRegistry";
    const SHM_PROVIDER_NAME: &'static str = "FleetEventShmProvider";
    const SHM_RESOURCE_NAME: &'static str = "fleet-event-ring";
}

pub const APPLICATION_EVENT_RING_KIND: u8 = 234;
pub const APPLICATION_EVENT_RING_CAPACITY: usize =
    orbitive::core::compile::usize_from_env(
        option_env!("ORBIT_APP_EVENT_RING_CAPACITY"),
        256,
    );
pub const APPLICATION_EVENT_RING_PAYLOAD_CAPACITY: usize =
    orbitive::core::compile::usize_from_env(
        option_env!("ORBIT_APP_EVENT_RING_PAYLOAD_CAPACITY"),
        8_192,
    );
/// Largest application event frame: topic, payload and header together.
pub const APPLICATION_EVENT_PAYLOAD_MAX: usize =
    APPLICATION_EVENT_RING_PAYLOAD_CAPACITY;

/// The application event ring. Geometry comes from
/// `ORBIT_APP_EVENT_RING_CAPACITY` and `ORBIT_APP_EVENT_RING_PAYLOAD_CAPACITY`
/// at build time; every binary opening the fleet must agree on it.
#[derive(Clone, Copy, Debug, Default)]
pub struct ApplicationEventLayout;

impl EventLayout for ApplicationEventLayout {
    const RING_KIND: u8 = APPLICATION_EVENT_RING_KIND;
    const RING_SPEC: RingSpec = RingSpec::per_node(
        APPLICATION_EVENT_RING_CAPACITY,
        APPLICATION_EVENT_RING_PAYLOAD_CAPACITY,
    );
}

impl EventRing for ApplicationEventLayout {
    const NAME: &'static str = "ApplicationEventRegistry";
    const SHM_PROVIDER_NAME: &'static str = "ApplicationEventShmProvider";
    const SHM_RESOURCE_NAME: &'static str = "application-event-ring";
}

/// The registry over the fleet's own ring.
pub type FleetEventRegistry = EventRegistry<DefaultEventLayout>;
/// The registry over the application ring.
pub type ApplicationEventRegistry = EventRegistry<ApplicationEventLayout>;

struct ManagedEventRecord<L>(PhantomData<L>);

impl<L> Clone for ManagedEventRecord<L> {
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}

impl<L: EventRing> orbitive::OrbitTyped for ManagedEventRecord<L> {
    const KIND: u8 = L::RING_KIND;
    const RING_SPEC: RingSpec = RingSpec::per_node(
        L::RING_SPEC.capacity,
        L::RING_SPEC.payload_capacity,
    );
}

pub(crate) struct EventShmResources<L: EventRing> {
    ring: OrbitRingResource<ManagedEventRecord<L>>,
}

impl<L: EventRing> EventShmResources<L> {
    fn new() -> Self {
        Self {
            ring: OrbitRingResource::new(
                L::SHM_PROVIDER_NAME,
                L::SHM_RESOURCE_NAME,
                ExistingBootPolicy::Reuse,
                FinalizePolicy::Preserve,
            ),
        }
    }
}

impl<L: EventRing> ManagedShmResources for EventShmResources<L> {
    fn name(&self) -> &'static str {
        L::SHM_PROVIDER_NAME
    }

    fn boot_priority(&self) -> Option<u8> {
        Some(priority::EARLY)
    }

    fn shm_resources(&self) -> Vec<&dyn crate::shm::ManagedShmResource> {
        vec![&self.ring]
    }
}

pub(crate) fn register_shm<L: EventRing>(
    state: &AppState
) -> registry::Result<()> {
    shm::register(state, EventShmResources::<L>::new())
}

trait EventHandler: Send + Sync {
    fn event_type(&self) -> TypeId;
    fn forwards_local(&self) -> bool;

    fn emit_local(
        &self,
        events: &claviron_runtime::ProcessEventBus,
        payload: &[u8],
    ) -> Result<(), String>;
}

struct TypedEventHandler<E: FleetEventCodec> {
    suppress_next: Mutex<VecDeque<Vec<u8>>>,
    forwards_local: bool,
    _event: std::marker::PhantomData<E>,
}

impl<E: FleetEventCodec> TypedEventHandler<E> {
    fn new(forwards_local: bool) -> Self {
        Self {
            suppress_next: Mutex::new(VecDeque::new()),
            forwards_local,
            _event: std::marker::PhantomData,
        }
    }

    fn should_suppress(
        &self,
        payload: &[u8],
    ) -> bool {
        let mut guard = self
            .suppress_next
            .lock()
            .expect("orbit event suppress queue poisoned");
        if let Some(pos) =
            guard.iter().position(|candidate| candidate.as_slice() == payload)
        {
            guard.remove(pos);
            return true;
        }
        false
    }

    fn suppress_republish(
        &self,
        payload: &[u8],
    ) {
        let mut guard = self
            .suppress_next
            .lock()
            .expect("orbit event suppress queue poisoned");
        guard.push_back(payload.to_vec());
        while guard.len() > 64 {
            guard.pop_front();
        }
    }
}

impl<E: FleetEventCodec> EventHandler for TypedEventHandler<E> {
    fn event_type(&self) -> TypeId {
        TypeId::of::<E>()
    }

    fn forwards_local(&self) -> bool {
        self.forwards_local
    }

    fn emit_local(
        &self,
        events: &claviron_runtime::ProcessEventBus,
        payload: &[u8],
    ) -> Result<(), String> {
        let event = E::decode_orbit(payload)?;
        if self.forwards_local {
            self.suppress_republish(payload);
        }
        events.emit(event);
        Ok(())
    }
}

/// Registry and bridge for typed Orbit events over the ring named by `L`.
///
/// Both registries are inserted by `claviron_orbit::register`. Boot prepares
/// the transport and inbound cursor; one managed Runnable per ring polls
/// registered topics into the process-local bus. Outbound bridges are local
/// dispatcher callbacks.
pub struct EventRegistry<L: EventRing = DefaultEventLayout> {
    bus: arc_swap::ArcSwapOption<FleetEventBus<L>>,
    handlers: RwLock<HashMap<&'static str, Arc<dyn EventHandler>>>,
    inbound: Mutex<Option<Inbound<L>>>,
    _ring: PhantomData<L>,
}

struct Inbound<L: EventRing> {
    bus: Arc<FleetEventBus<L>>,
    cursor: FleetEventCursor,
    #[cfg(any(
        target_os = "linux",
        target_os = "freebsd",
        target_os = "macos"
    ))]
    event_fd: Option<tokio::io::unix::AsyncFd<orbitive::core::RingEventFd>>,
}

impl<L: EventRing> EventRegistry<L> {
    pub fn new() -> Self {
        Self {
            bus: arc_swap::ArcSwapOption::const_empty(),
            handlers: RwLock::new(HashMap::new()),
            inbound: Mutex::new(None),
            _ring: PhantomData,
        }
    }

    /// Register a typed Orbit event and subscribe the local process event bus
    /// so local emits of `E` are published to the shared Orbit event ring.
    ///
    /// The same event type can be registered in every process. When an
    /// Orbit event is decoded and emitted locally, the registry suppresses
    /// the immediate local re-publish of the same encoded payload to avoid
    /// echo loops.
    ///
    /// Registration starts no task and must precede runnable submission.
    /// Registering the same type again is a no-op; a topic cannot name two types.
    pub fn register_bridge<E>(
        self: &Arc<Self>,
        state: &AppState,
    ) -> registry::Result<()>
    where
        E: FleetEventCodec,
    {
        let mut handlers =
            self.handlers.write().expect("orbit event handlers lock poisoned");
        if let Some(handler) = handlers.get(E::TOPIC) {
            return if handler.event_type() == TypeId::of::<E>()
                && handler.forwards_local()
            {
                Ok(())
            } else {
                Err(registry::Error::msg(format!(
                    "Orbit topic {} already has a codec",
                    E::TOPIC
                )))
            };
        }
        let handler = Arc::new(TypedEventHandler::<E>::new(true));
        let outbound = handler.clone();
        // The callback must not own AppState: the state owns its dispatcher.
        let registry = self.clone();
        state.events().listen::<E, _>(move |_, event| {
            let handler = outbound.clone();
            let registry = registry.clone();
            async move {
                let payload = event.encode_orbit();
                if handler.should_suppress(&payload) {
                    trace!(
                        topic = E::TOPIC,
                        "orbit event local echo suppressed"
                    );
                    return Ok(());
                }
                registry
                    .publish_raw(E::TOPIC, &payload)
                    .map_err(registry::Error::msg)?;
                Ok(())
            }
        })?;
        handlers.insert(E::TOPIC, handler);
        Ok(())
    }

    /// Decode peer frames into local typed events without forwarding local emits.
    /// Use this with synchronous `publish_raw` producers that already committed
    /// their local effect. Registration starts no task and is idempotent for the
    /// same type and mode; a topic cannot switch modes implicitly.
    pub fn register_inbound<E>(&self) -> registry::Result<()>
    where
        E: FleetEventCodec,
    {
        let mut handlers =
            self.handlers.write().expect("orbit event handlers lock poisoned");
        if let Some(handler) = handlers.get(E::TOPIC) {
            return if handler.event_type() == TypeId::of::<E>()
                && !handler.forwards_local()
            {
                Ok(())
            } else {
                Err(registry::Error::msg(format!(
                    "Orbit topic {} already has a codec or forwarding mode",
                    E::TOPIC
                )))
            };
        }
        handlers.insert(E::TOPIC, Arc::new(TypedEventHandler::<E>::new(false)));
        Ok(())
    }

    /// The ring this registry publishes to, once it has booted.
    pub fn bus(&self) -> Option<Arc<FleetEventBus<L>>> {
        self.bus.load_full()
    }

    /// Hand an event that reached this process from outside its ring — over a
    /// link, from another fleet — to the local bus, as the inbound poller
    /// would: decoded by its registered codec, and not republished by this
    /// registry's own outbound bridge.
    pub fn deliver(
        &self,
        events: &claviron_runtime::ProcessEventBus,
        topic: &str,
        payload: &[u8],
    ) -> Result<(), String> {
        let handler = self
            .handlers
            .read()
            .expect("orbit event handlers lock poisoned")
            .get(topic)
            .cloned()
            .ok_or_else(|| format!("no codec is registered for {topic}"))?;
        handler.emit_local(events, payload)
    }

    /// Commit directly to the ring without a local event-forwarding task.
    /// Successful return does not imply peer consumption or acknowledgement.
    /// The outbound bus remains available after the inbound poller stops.
    pub fn publish_raw(
        &self,
        topic: &str,
        payload: &[u8],
    ) -> Result<orbitive::NetId64, String> {
        let bus = self
            .bus
            .load_full()
            .ok_or_else(|| "Orbit event bus is not booted".to_string())?;
        bus.publish(topic, payload).map_err(|err| err.to_string())
    }

    fn install_bus(
        &self,
        bus: Arc<FleetEventBus<L>>,
    ) {
        self.bus.store(Some(bus));
    }

    /// `NO_IMPLICIT_TERMINATION: EVENT_SHM_SURVIVES_MASTER_REPLACEMENT`.
    ///
    /// Prepare this process' view of the persistent fleet event transport.
    ///
    /// The shared ring deliberately survives master replacement. A live
    /// adopted worker may still be publishing into it, so process boot is not
    /// proof that the ring is quiescent and must never reset it implicitly.
    /// Starting the inbound cursor at the current head excludes retained
    /// history without destroying it for existing peers.
    fn boot_transport(
        &self,
        bus: Arc<FleetEventBus<L>>,
    ) -> registry::Result<()> {
        // TODO(NO_IMPLICIT_TERMINATION): audit every other master-owned SHM
        // lifecycle separately. Counters, snapshots, metrics, TLS sessions,
        // PHP invalidation and invocation transport have different ownership
        // semantics; event-ring persistence is not an implicit policy for them.
        self.prepare_inbound(bus)
    }

    fn prepare_inbound(
        &self,
        bus: Arc<FleetEventBus<L>>,
    ) -> registry::Result<()> {
        let mut inbound =
            self.inbound.lock().expect("orbit inbound lock poisoned");
        if self.bus.load().is_some() {
            return Err(registry::Error::msg(
                "Orbit event transport already prepared",
            ));
        }
        #[cfg(any(target_os = "linux", target_os = "freebsd"))]
        let event_fd = Some({
            let event_fd = bus.event_fd().map_err(|error| {
                registry::Error::msg(format!(
                    "Orbit eventfd creation failed: {error}"
                ))
            })?;
            // SAFETY: RingEventFd owns its stable descriptor until drop; the
            // inbound reader only drains it and never replaces the inner value.
            unsafe { tokio::io::unix::AsyncFd::register(event_fd) }.map_err(
                |error| {
                    registry::Error::msg(format!(
                        "Orbit eventfd registration failed: {error}"
                    ))
                },
            )?
        });
        #[cfg(target_os = "macos")]
        let event_fd = match bus.event_fd() {
            Ok(event_fd) => Some(
                // SAFETY: RingEventFd owns its stable descriptor until drop;
                // the inbound reader only drains it, without replacing it.
                unsafe { tokio::io::unix::AsyncFd::register(event_fd) }
                    .map_err(|error| {
                        registry::Error::msg(format!(
                            "Orbit eventfd registration failed: {error}"
                        ))
                    })?,
            ),
            Err(orbitive::events::Error::Io(error))
                if error.kind() == std::io::ErrorKind::Unsupported =>
            {
                warn!(
                    "Orbit event native readiness unavailable; using timer polling"
                );
                None
            }
            Err(error) => {
                return Err(registry::Error::msg(format!(
                    "Orbit eventfd creation failed: {error}"
                )));
            }
        };
        // Prepare readiness before the cursor, and the cursor before workers
        // spawn. run() must consume this snapshot, never take a newer one.
        *inbound = Some(Inbound {
            cursor: bus.cursor_at_head(),
            bus: bus.clone(),
            #[cfg(any(
                target_os = "linux",
                target_os = "freebsd",
                target_os = "macos"
            ))]
            event_fd,
        });
        self.install_bus(bus);
        Ok(())
    }

    fn poll_once(
        &self,
        state: &AppState,
        bus: &FleetEventBus<L>,
        cursor: &mut FleetEventCursor,
    ) {
        let poll = bus.poll(cursor);
        if poll.lagged > 0 {
            warn!(
                lagged = poll.lagged,
                "orbit event bridge lagged behind shared ring"
            );
        }

        for event in poll.events {
            if event.id.node() == bus.node_id().get() {
                trace!(topic = %event.topic, "orbit event ignored: local node already emitted it");
                continue;
            }
            let handler = {
                self.handlers
                    .read()
                    .expect("orbit event handlers lock poisoned")
                    .get(event.topic.as_str())
                    .cloned()
            };
            let Some(handler) = handler else {
                trace!(topic = %event.topic, "orbit event ignored: no registered handler");
                continue;
            };
            if let Err(err) =
                handler.emit_local(state.events().bus(), &event.payload)
            {
                warn!(topic = %event.topic, "orbit event decode/emit failed: {err}");
            }
        }
    }
}

impl<L: EventRing> Default for EventRegistry<L> {
    fn default() -> Self {
        Self::new()
    }
}

impl<L: EventRing> Scoped for EventRegistry<L> {
    fn scope(&self) -> Scope {
        // Event consumers include master lifecycle and management callbacks.
        Scope::Any
    }
}

impl<L: EventRing> Component<AppState> for EventRegistry<L> {
    type Deps = (Arc<FleetProvider>,);

    fn name(&self) -> &'static str {
        L::NAME
    }

    fn as_scoped(&self) -> Option<&dyn Scoped> {
        Some(self)
    }

    fn boot_priority(&self) -> Option<u8> {
        Some(priority::EARLY)
    }

    fn order(&self) -> ComponentOrder {
        ComponentOrder::new()
            .after::<ManagedShmProvider<EventShmResources<L>>>()
    }

    fn as_runnable(
        self: Arc<Self>
    ) -> Option<Arc<dyn Runnable<AppState, Self::Deps>>> {
        Some(self)
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<AppState, Self::Deps>> {
        Some(self)
    }
}

#[async_trait]
impl<L: EventRing> Bootable<AppState, (Arc<FleetProvider>,)>
    for EventRegistry<L>
{
    async fn boot(
        &self,
        (fleet_provider,): (Arc<FleetProvider>,),
        _state: &AppState,
    ) -> registry::Result<()> {
        let fleet = fleet_provider.handle().ok_or_else(|| {
            registry::Error::msg(format!(
                "FleetProvider is not booted before {}",
                L::NAME
            ))
        })?;
        let bus = FleetEventBus::<L>::with_layout(fleet).map_err(|error| {
            registry::Error::msg(format!("{}: {error}", L::NAME))
        })?;
        self.boot_transport(Arc::new(bus))?;
        debug!(registry = L::NAME, "🛰  Orbit event registry booted");
        Ok(())
    }
}

#[async_trait]
impl<L: EventRing> Runnable<AppState, (Arc<FleetProvider>,)>
    for EventRegistry<L>
{
    async fn run(
        self: Arc<Self>,
        _deps: (Arc<FleetProvider>,),
        state: AppState,
        context: RunContext,
    ) -> registry::Result<()> {
        let mut inbound = self
            .inbound
            .lock()
            .expect("orbit inbound lock poisoned")
            .take()
            .ok_or_else(|| {
                registry::Error::msg(
                    "Orbit event transport is not prepared or already running",
                )
            })?;
        if context.is_cancelled() {
            self.inbound
                .lock()
                .expect("orbit inbound lock poisoned")
                .replace(inbound);
            return Ok(());
        }
        // Also consume events retained between boot and the first run poll.
        self.poll_once(&state, &inbound.bus, &mut inbound.cursor);

        #[cfg(any(
            target_os = "linux",
            target_os = "freebsd",
            target_os = "macos"
        ))]
        let result = if let Some(event_fd) = &inbound.event_fd {
            loop {
                tokio::select! {
                    biased;
                    _ = context.cancelled() => break Ok(()),
                    ready = event_fd.readable() => {
                        let mut ready = match ready {
                            Ok(ready) => ready,
                            Err(error) => break Err(registry::Error::msg(format!("Orbit eventfd readiness failed: {error}")))
                        };
                        if let Err(error) = event_fd.get_ref().drain() {
                            break Err(registry::Error::msg(format!("Orbit eventfd drain failed: {error}")));
                        }
                        ready.clear_ready();
                        self.poll_once(&state, &inbound.bus, &mut inbound.cursor);
                    }
                }
            }
        } else {
            self.drive_polling(&state, &context, &mut inbound).await
        };

        #[cfg(not(any(
            target_os = "linux",
            target_os = "freebsd",
            target_os = "macos"
        )))]
        let result = self.drive_polling(&state, &context, &mut inbound).await;

        self.inbound
            .lock()
            .expect("orbit inbound lock poisoned")
            .replace(inbound);
        result
    }
}

impl<L: EventRing> EventRegistry<L> {
    async fn drive_polling(
        &self,
        state: &AppState,
        context: &RunContext,
        inbound: &mut Inbound<L>,
    ) -> registry::Result<()> {
        let mut interval = tokio::time::interval(FALLBACK_POLL_INTERVAL);
        interval
            .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                _ = context.cancelled() => return Ok(()),
                _ = interval.tick() => self.poll_once(state, &inbound.bus, &mut inbound.cursor),
            }
        }
    }
}

#[cfg(test)]
#[path = "../tests/event.rs"]
mod tests;
