use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use claviron_orbit::FleetProvider;
use claviron_orbit::placement::{Scope, Scoped};
use claviron_pusher::PublishedEvent;
use claviron_runtime::congif::{self, ConfigFile, Configurable};
use claviron_runtime::registry::{self, Component, Runnable, priority};
use claviron_runtime::{AppState, Gate, RunContext};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::SessionAuthenticator;
use crate::channels::ChannelRegistry;
use crate::config::BroadcastConfig;
use crate::error::SessionRefusal;
use crate::fleet::{FleetFanout, FleetPublication};
use crate::hooks::{BroadcastOccupancyService, Hooks};
use crate::session::BroadcastSession;
use crate::upgrade::BroadcastUpgradeHandler;
const OUTBOUND_QUEUE_CAPACITY: usize = 64;
static SOCKET_SEQUENCE: AtomicU64 = AtomicU64::new(1);

pub(crate) struct Generation {
    pub(crate) cancellation: CancellationToken,
    pub(crate) sessions: Gate,
    pub(crate) channels: ChannelRegistry,
    pub(crate) fleet: Option<FleetFanout>,
    pub(crate) receive_fleet: bool,
    pub(crate) groups: crate::groups::Groups,
}

impl Generation {
    /// The fleet to publish on, when there is anyone else to reach.
    pub(crate) fn peers(&self) -> Option<&FleetFanout> {
        self.fleet.as_ref().filter(|_| self.receive_fleet)
    }
}

pub struct BroadcastService {
    hooks: Hooks,
    config_path: PathBuf,
    /// The application catalog Congif prepared and `configure` applied.
    config: ArcSwapOption<BroadcastConfig>,
    generation: RwLock<Option<Arc<Generation>>>,
    authenticators: RwLock<HashMap<Arc<str>, Arc<dyn SessionAuthenticator>>>,
}

impl BroadcastService {
    /// Local hooks; register occupancy callbacks in master/standalone.
    pub fn hooks(&self) -> &Hooks {
        &self.hooks
    }

    /// Groups and addressed boxes on this service's fleet — the engine under
    /// Django Channels' layer. See [`crate::groups`].
    pub fn groups(self: &Arc<Self>) -> GroupLayer {
        GroupLayer { service: Arc::clone(self) }
    }

    fn current(&self) -> Result<Arc<Generation>, crate::groups::GroupError> {
        self.generation
            .read()
            .expect("Broadcast generation lock poisoned")
            .clone()
            .ok_or(crate::groups::GroupError::NotRunning)
    }

    /// A service with its catalog already in hand, for hosts and tests that
    /// own the configuration themselves.
    pub fn new(config: BroadcastConfig) -> Self {
        Self {
            hooks: Hooks::default(),
            config_path: PathBuf::new(),
            config: ArcSwapOption::from_pointee(config),
            generation: RwLock::new(None),
            authenticators: RwLock::new(HashMap::new()),
        }
    }

    /// A service whose catalog Congif prepares from `broadcast.yaml`.
    fn unconfigured(config_path: PathBuf) -> Self {
        Self {
            hooks: Hooks::default(),
            config_path,
            config: ArcSwapOption::empty(),
            generation: RwLock::new(None),
            authenticators: RwLock::new(HashMap::new()),
        }
    }

    /// The application catalog. Configuration is applied before any
    /// provider boots or runs; a service asked earlier is a lifecycle fault.
    pub fn applications(&self) -> Arc<BroadcastConfig> {
        self.config.load_full().expect(
            "BroadcastService used before its configuration was applied",
        )
    }

    /// Require an existing application session for every native entrance.
    /// Register before serving; neither the catalog nor channel signatures
    /// become a session authority. Other applications remain unchanged.
    pub fn require_session(
        &self,
        application: impl Into<Arc<str>>,
        authenticator: Arc<dyn SessionAuthenticator>,
    ) -> crate::Result<()> {
        let application = application.into();
        if !self
            .applications()
            .applications()
            .contains_key(application.as_ref())
        {
            return Err(crate::Error::UnknownApplication);
        }
        self.authenticators
            .write()
            .expect("broadcast authentication registry lock poisoned")
            .insert(application, authenticator);
        Ok(())
    }

    pub fn upgrade_handler(
        self: &Arc<Self>,
        application: impl Into<Arc<str>>,
    ) -> Arc<BroadcastUpgradeHandler> {
        Arc::new(BroadcastUpgradeHandler {
            service: Arc::clone(self),
            application: application.into(),
        })
    }

    pub fn publish_local(
        &self,
        application: &str,
        event: &PublishedEvent,
    ) -> crate::Result<usize> {
        let generation = self
            .generation
            .read()
            .expect("Broadcast generation lock poisoned")
            .clone()
            .ok_or(crate::Error::Unavailable)?;
        generation.channels.publish(application, event)
    }

    pub fn publish(
        &self,
        application: &str,
        event: &PublishedEvent,
    ) -> crate::Result<usize> {
        let generation = self
            .generation
            .read()
            .expect("Broadcast generation lock poisoned")
            .clone()
            .ok_or(crate::Error::Unavailable)?;
        if let Some(fleet) = &generation.fleet {
            fleet.publish(application, event)?;
        }
        generation.channels.publish(application, event)
    }

    /// Publish for the application identified by its configured Pusher key.
    /// An unknown key is rejected; it never selects another application.
    pub fn publish_by_key(
        &self,
        key: &str,
        event: &PublishedEvent,
    ) -> crate::Result<usize> {
        let catalog =
            self.config.load_full().ok_or(crate::Error::Unavailable)?;
        let (application, _) = catalog
            .application_by_key(key)
            .ok_or(crate::Error::UnknownApplication)?;
        self.publish(application, event)
    }

    pub fn accept_session(
        &self,
        application: Arc<str>,
    ) -> Result<BroadcastSession, SessionRefusal> {
        let catalog = self.applications();
        let application_config = catalog
            .applications()
            .get(application.as_ref())
            .ok_or(SessionRefusal::UnknownApplication)?;
        let generation = self
            .generation
            .read()
            .expect("Broadcast generation lock poisoned")
            .clone()
            .ok_or(SessionRefusal::NotRunning)?;
        let permit = generation
            .sessions
            .try_enter()
            .map_err(|_| SessionRefusal::Stopping)?;
        let socket_id = Arc::<str>::from(format!(
            "{}.{}",
            std::process::id(),
            SOCKET_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        let cancellation = generation.cancellation.child_token();
        let (sender, outbound) = mpsc::channel(OUTBOUND_QUEUE_CAPACITY);
        let authenticator = self
            .authenticators
            .read()
            .expect("broadcast authentication registry lock poisoned")
            .get(application.as_ref())
            .cloned();
        Ok(BroadcastSession {
            application,
            app_id: application_config.id.clone(),
            key: application_config.key.clone(),
            secret: application_config.secret.clone(),
            client_events: application_config.client_events,
            socket_id,
            cancellation,
            generation,
            outbound,
            sender,
            _permit: permit,
            authenticator,
            authenticated: false,
        })
    }
}

impl Component<AppState> for BroadcastService {
    // Direct local hosts can run without fleet fanout.
    type Deps = (Option<Arc<FleetProvider>>,);

    fn name(&self) -> &'static str {
        "BroadcastService"
    }

    fn run_priority(&self) -> Option<u8> {
        Some(priority::EARLY)
    }

    fn as_runnable(
        self: Arc<Self>
    ) -> Option<Arc<dyn Runnable<AppState, Self::Deps>>> {
        Some(self)
    }

    fn as_configurable(&self) -> Option<&dyn congif::ConfigRegistration> {
        (!self.config_path.as_os_str().is_empty()).then_some(self)
    }

    fn as_scoped(&self) -> Option<&dyn Scoped> {
        Some(self)
    }
}

/// Sessions are served where requests are: in the workers, or in a
/// standalone process. The master holds the catalog and nothing more.
impl Scoped for BroadcastService {
    fn scope(&self) -> Scope {
        Scope::Worker
    }
}

/// `broadcast.yaml` is required wherever the service is registered; there is
/// no absent form. The catalog is applied once: sessions are checked against
/// it when they are accepted, and a changed file takes effect with the next
/// explicit reload cycle of the process that reads it.
#[async_trait]
impl Configurable for BroadcastService {
    type Config = BroadcastConfig;

    fn source(&self) -> ConfigFile {
        ConfigFile::yaml(&self.config_path)
    }

    fn validate_config(
        &self,
        config: &BroadcastConfig,
    ) -> congif::Result<()> {
        config.clone().validate().map(|_| ()).map_err(congif::Error::validation)
    }

    async fn configure(
        &self,
        config: &BroadcastConfig,
    ) -> congif::Result<()> {
        self.config.store(Some(Arc::new(config.clone())));
        Ok(())
    }
}

#[async_trait]
impl Runnable<AppState, (Option<Arc<FleetProvider>>,)> for BroadcastService {
    async fn run(
        self: Arc<Self>,
        (fleet,): (Option<Arc<FleetProvider>>,),
        state: AppState,
        context: RunContext,
    ) -> registry::Result<()> {
        let fleet =
            fleet.and_then(|provider| provider.handle()).map(FleetFanout::new);
        let node = fleet.as_ref().map_or(0, FleetFanout::node_id);
        let generation = Arc::new(Generation {
            groups: crate::groups::Groups::new(node),
            cancellation: CancellationToken::new(),
            sessions: Gate::new(None, Duration::ZERO),
            channels: ChannelRegistry::default(),
            fleet,
            receive_fleet: !state.is_standalone(),
        });
        {
            let mut current = self
                .generation
                .write()
                .expect("Broadcast generation lock poisoned");
            if current.is_some() {
                return Err(registry::Error::msg(
                    "Broadcast service generation is already running",
                ));
            }
            *current = Some(Arc::clone(&generation));
        }
        info!(
            applications = self.applications().applications().len(),
            "🧩 Broadcast service started"
        );

        let result = run_generation(&generation, &context).await;

        generation.sessions.graceful_shutdown(None);
        generation.cancellation.cancel();
        let _ = generation.sessions.wait_all_done().await;
        self.generation
            .write()
            .expect("Broadcast generation lock poisoned")
            .take();
        info!("🔸 Broadcast service stopped");
        result
    }
}

async fn run_generation(
    generation: &Generation,
    context: &RunContext,
) -> registry::Result<()> {
    if !generation.receive_fleet {
        context.cancelled().await;
        return Ok(());
    }
    let Some(fleet) = &generation.fleet else {
        context.cancelled().await;
        return Ok(());
    };

    #[cfg(any(target_os = "linux", target_os = "freebsd"))]
    let event_fd = Some(
        fleet
            .event_fd()
            .and_then(|event_fd| {
                // SAFETY: RingEventFd owns its stable descriptor until drop;
                // this generation only drains it and never replaces it.
                unsafe { tokio::io::unix::AsyncFd::register(event_fd) }
                    .map_err(std::io::Error::from)
            })
            .map_err(|error| {
                registry::Error::msg(format!(
                    "Broadcast Orbit readiness failed: {error}"
                ))
            })?,
    );
    #[cfg(target_os = "macos")]
    let event_fd = match fleet.event_fd() {
        Ok(event_fd) => {
            // SAFETY: RingEventFd owns its stable descriptor until drop;
            // this generation only drains it and never replaces it.
            Some(
                unsafe { tokio::io::unix::AsyncFd::register(event_fd) }
                    .map_err(|error| {
                        registry::Error::msg(format!(
                            "Broadcast Orbit readiness failed: {error}"
                        ))
                    })?,
            )
        }
        Err(error) if error.kind() == std::io::ErrorKind::Unsupported => {
            tracing::warn!(
                "Broadcast Orbit native readiness unavailable; using timer polling"
            );
            None
        }
        Err(error) => {
            return Err(registry::Error::msg(format!(
                "Broadcast Orbit readiness failed: {error}"
            )));
        }
    };

    let mut cursor = fleet.cursor_at_head();
    fleet.publish_presence_reset().map_err(|error| {
        registry::Error::msg(format!(
            "Broadcast presence reset failed: {error}"
        ))
    })?;
    fleet.publish_subscription_reset().map_err(|error| {
        registry::Error::msg(format!(
            "Broadcast subscription reset failed: {error}"
        ))
    })?;
    // Readiness is installed before the cursor, and this first drain closes the
    // startup interval before the task begins waiting for another notification.
    deliver_fleet(generation, fleet, &mut cursor);

    #[cfg(any(
        target_os = "linux",
        target_os = "freebsd",
        target_os = "macos"
    ))]
    if let Some(event_fd) = event_fd {
        loop {
            tokio::select! {
                biased;
                _ = context.cancelled() => break,
                ready = event_fd.readable() => {
                    let mut ready = ready.map_err(|error| registry::Error::msg(format!("Broadcast Orbit readiness failed: {error}")))?;
                    event_fd.get_ref().drain().map_err(|error| registry::Error::msg(format!("Broadcast Orbit readiness drain failed: {error}")))?;
                    ready.clear_ready();
                    deliver_fleet(generation, fleet, &mut cursor);
                }
            }
        }
        return Ok(());
    }

    let mut interval = tokio::time::interval(Duration::from_millis(10));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            _ = context.cancelled() => break,
            _ = interval.tick() => deliver_fleet(generation, fleet, &mut cursor)
        }
    }
    Ok(())
}

fn deliver_fleet(
    generation: &Generation,
    fleet: &FleetFanout,
    cursor: &mut orbitive::fleet::FleetLaneCursor,
) {
    for (kind, source_node, payload) in fleet.poll_all(cursor) {
        match kind {
            crate::fleet::FRAME_KIND_PUSHER => {
                if let Some(publication) = FleetFanout::decode_pusher(&payload)
                {
                    deliver_pusher(generation, fleet, source_node, publication);
                }
            }
            crate::fleet::FRAME_KIND_GROUPS => {
                generation.groups.deliver(&payload)
            }
            // A kind this build does not know is another consumer's.
            _ => {}
        }
    }
}

/// One Pusher publication from another process.
fn deliver_pusher(
    generation: &Generation,
    fleet: &FleetFanout,
    source_node: u16,
    publication: FleetPublication,
) {
    let inbound = crate::fleet::FleetInbound { source_node, publication };
    {
        match inbound.publication {
            FleetPublication::Event { application, event } => {
                match generation.channels.publish(&application, &event) {
                    Ok(delivered) => {
                        debug!(%application, delivered, event = event.name, "Pusher fleet event delivered locally")
                    }
                    Err(error) => {
                        tracing::warn!(%application, %error, "Pusher fleet event delivery failed")
                    }
                }
            }
            FleetPublication::PresenceJoined {
                application,
                channel,
                socket_id,
                member,
            } => {
                if let Err(error) = generation.channels.join_presence(
                    application.into(),
                    channel.into(),
                    socket_id.into(),
                    member,
                    inbound.source_node,
                ) {
                    tracing::warn!(%error, "Pusher fleet presence join failed");
                }
            }
            FleetPublication::PresenceLeft {
                application,
                channel,
                socket_id,
            } => {
                if let Err(error) = generation.channels.leave_presence(
                    &application,
                    &channel,
                    &socket_id,
                ) {
                    tracing::warn!(%error, "Pusher fleet presence leave failed");
                }
            }
            FleetPublication::PresenceReset => {
                if let Err(error) =
                    generation.channels.reset_presence_node(inbound.source_node)
                {
                    tracing::warn!(%error, source_node = inbound.source_node, "Pusher fleet presence reset failed");
                    return;
                }
                for (application, channel, socket_id, member) in
                    generation.channels.local_presence(fleet.node_id())
                {
                    if let Err(error) = fleet.publish_presence_joined(
                        &application,
                        &channel,
                        &socket_id,
                        &member,
                    ) {
                        tracing::warn!(%error, source_node = inbound.source_node, "Pusher fleet presence resync failed");
                    }
                }
            }
            FleetPublication::SubscriptionReset => {
                for (app_id, channel, socket_id, member) in
                    generation.channels.local_subscriptions()
                {
                    if let Err(error) = fleet.publish_subscription_joined(
                        &app_id,
                        &channel,
                        &socket_id,
                        member.as_ref(),
                    ) {
                        tracing::warn!(%error, source_node = inbound.source_node, "Pusher fleet subscription resync failed");
                    }
                }
            }
            FleetPublication::SubscriptionJoined { .. }
            | FleetPublication::SubscriptionLeft { .. } => {}
        }
    }
}

pub fn register(state: &AppState) -> registry::Result<()> {
    if state.registry_ref().resolve::<BroadcastService>().is_some() {
        return Ok(());
    }
    register_service(
        state,
        BroadcastService::unconfigured(
            state.config_dir().join("broadcast.yaml"),
        ),
    )
}

/// Register a host-owned catalog through the normal fleet/session composition.
pub fn register_with_config(
    state: &AppState,
    config: BroadcastConfig,
) -> registry::Result<()> {
    let config = config
        .validate()
        .map_err(|error| registry::Error::msg(error.to_string()))?;
    register_service(state, BroadcastService::new(config))
}

fn register_service(
    state: &AppState,
    service: BroadcastService,
) -> registry::Result<()> {
    claviron_orbit::register(state)?;
    if let Some(events) = state
        .registry_ref()
        .resolve::<claviron_orbit::ApplicationEventRegistry>()
    {
        events.register_bridge::<claviron_app::ApplicationEvent>(state)?;
    }
    claviron_orbit::shm::register_reusable_ring::<
        crate::fleet::PublicationRecord,
    >(state, "BroadcastPublicationShm", "broadcast-publication-ring")?;
    claviron_orbit::shm::register_reusable_ring::<crate::fleet::PayloadRecord>(
        state,
        "BroadcastPayloadShm",
        "broadcast-payload-ring",
    )?;
    // Both services register in every role and declare where they run:
    // occupancy is decided master-side, sessions are served worker-side, and
    // a standalone process does both.
    let service = Arc::new(service);
    state
        .registry_ref()
        .register(Arc::new(BroadcastOccupancyService(service.hooks.clone())))?;
    state.bind_config_environment::<BroadcastService>()?;
    state.registry_ref().register(service)?;
    Ok(())
}

/// A handle on the service's groups. Each call reads the running generation,
/// so a handle kept across a restart reaches the new one.
#[derive(Clone)]
pub struct GroupLayer {
    service: Arc<BroadcastService>,
}

impl GroupLayer {
    /// A new box in this process, and its name.
    pub fn open(
        &self,
        prefix: &str,
        capacity: Option<usize>,
        expiry: Option<Duration>,
    ) -> Result<String, crate::groups::GroupError> {
        Ok(self.service.current()?.groups.open(prefix, capacity, expiry))
    }

    pub fn close(
        &self,
        channel: &str,
    ) {
        if let Ok(generation) = self.service.current() {
            generation.groups.close(channel);
        }
    }

    pub fn send(
        &self,
        channel: &str,
        message: bytes::Bytes,
    ) -> Result<(), crate::groups::GroupError> {
        let generation = self.service.current()?;
        generation.groups.send(generation.peers(), channel, message)
    }

    pub async fn receive(
        &self,
        channel: &str,
    ) -> Result<bytes::Bytes, crate::groups::GroupError> {
        let generation = self.service.current()?;
        generation.groups.receive(channel).await
    }

    /// The next message without waiting, or `None` with `wake` kept and
    /// called on the thread that puts the next one in — for a receiver whose
    /// waits are resolved by its own loop rather than by an async runtime.
    pub fn receive_or_wake(
        &self,
        channel: &str,
        wake: crate::groups::Wake,
    ) -> Result<Option<bytes::Bytes>, crate::groups::GroupError> {
        self.service.current()?.groups.receive_or_wake(channel, wake)
    }

    pub fn add(
        &self,
        group: &str,
        channel: &str,
    ) -> Result<(), crate::groups::GroupError> {
        self.service.current()?.groups.add(group, channel)
    }

    pub fn discard(
        &self,
        group: &str,
        channel: &str,
    ) {
        if let Ok(generation) = self.service.current() {
            generation.groups.discard(group, channel);
        }
    }

    pub fn group_send(
        &self,
        group: &str,
        message: bytes::Bytes,
    ) -> Result<usize, crate::groups::GroupError> {
        let generation = self.service.current()?;
        generation.groups.group_send(generation.peers(), group, message)
    }
}
