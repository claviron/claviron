use std::env;
use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime};

use async_trait::async_trait;
use continuo::{HasEvents, HasRegistry, ReloadState, SharedState};
use tokio_util::sync::CancellationToken;

use crate::env::{ConfigEnvironment, EnvSnapshot};
use crate::events::{EventDispatcher, ProcessEventBus};
use crate::registry::Registry;

/// Default shared Orbit domain for Claviron compositions.
///
/// This is a fleet identity, not a process-instance identity. Independent
/// process families may deliberately join it when their node ids are allocated
/// from the same fleet geometry.
pub const DEFAULT_FLEET_NAME: &str = "clv";

/// Process identity only. Fleet geometry and the supervisor's desired worker
/// count are separate state because neither is part of a process role.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunMode {
    Master,
    Standalone,
    Worker(WorkerId),
}

impl From<RunMode> for continuo::fleet::Scope {
    fn from(mode: RunMode) -> Self {
        match mode {
            RunMode::Master => Self::Master,
            RunMode::Worker(_) => Self::Worker,
            RunMode::Standalone => Self::Any,
        }
    }
}

/// One-based process-family slot identity. Ordered so slots sort and key
/// tables naturally; both `Display` and `Debug` print the bare number, which is
/// how logs and snapshots have always spelled it.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct WorkerId(pub u8);
impl WorkerId {
    pub const fn new(value: u8) -> Self {
        Self(value)
    }

    pub const fn raw(self) -> u8 {
        self.0
    }
}

/// Fleet-wide physical lane identity. It is deliberately distinct from a
/// process family's local [`WorkerId`]; multi-family fleets may allocate them
/// independently.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct FleetNodeId(u16);

impl FleetNodeId {
    pub const ZERO: Self = Self(0);

    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    pub const fn raw(self) -> u16 {
        self.0
    }
}

/// Which incarnation of its fleet node this process is, in the master's count.
///
/// The master mints it per worker slot and bumps it on every replacement —
/// recovery, rotation, restart and resize all continue one slot's sequence
/// rather than restarting it — so no two live processes of a slot ever share
/// one. Orbit tables are stamped with it, and a death report that names the
/// wrong incarnation is a silent no-op that leaks the budget, which is why it
/// is carried rather than re-derived.
///
/// A master or standalone process is [`FleetIncarnation::FIRST`]: it joins the
/// fleet but registers no pooled resource, so there is nothing for a later
/// incarnation of it to be confused with. That stops being true the day the
/// master owns a pooled resource of its own.
///
/// Carried in the registry rather than on [`State`] because it is a fact the
/// master hands this process, exactly like the worker identity beside it, and
/// not part of the boot geometry every caller of `initialize` must supply.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct FleetIncarnation(u64);

impl FleetIncarnation {
    /// A process whose slot has had no replacement yet, and every primary.
    pub const FIRST: Self = Self(1);

    /// Zero is not an incarnation; the supervisor rejects it at the envelope
    /// boundary, and it is clamped here rather than trusted.
    pub const fn new(value: u64) -> Self {
        Self(if value == 0 { 1 } else { value })
    }

    pub const fn raw(self) -> u64 {
        self.0
    }
}

impl Default for FleetIncarnation {
    fn default() -> Self {
        Self::FIRST
    }
}

/// What the launch decided, as the embedding application encoded it.
///
/// Opaque here and opaque to the supervisor: neither reads inside it. The
/// application hands it to `ServerOptions` in the primary process, the
/// supervisor copies it into each worker's bootstrap envelope verbatim, and
/// the application reads it back in the worker. That is the whole contract.
///
/// It exists because a worker is a re-exec, not a fork, and the supervisor
/// deliberately does not pass the command line along — an embedding binary
/// may define any flags it likes, and the supervisor has no business
/// parsing them. Without this slot, everything a launch decided on the
/// command line is lost the moment the process family grows a worker.
///
/// **It is a launch, not a secret.** The envelope travels in the environment,
/// which any process of the same user can read. Paths belong here; the
/// contents of a key file do not.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LaunchBlob(Vec<u8>);

impl LaunchBlob {
    /// The largest blob that will be carried.
    ///
    /// `execve` bounds the environment and the command line together, and the
    /// smallest limit among the Unixes this runs on is a little over 256 KiB.
    /// This leaves that budget overwhelmingly to everything else: a launch is
    /// a handful of options, and anything approaching this size is a
    /// configuration file that should be read from disk by path.
    pub const MAX: usize = 64 * 1024;

    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// This process only checks configuration (`--check-config`): it boots
/// providers to read their files without mutating application data. Storage
/// drivers may perform ordinary read-only coordination, such as SQLite WAL
/// index locks and sidecars. A check may run beside the live service — same
/// user, same fleet, same shared memory — and the service's segments are not
/// its to reset, recreate or unlink. Present in the registry of a process started
/// through `Server::initialize_for_check`, absent everywhere else.
#[derive(Clone, Copy, Debug)]
pub struct ConfigCheck;

impl Display for FleetIncarnation {
    fn fmt(
        &self,
        f: &mut Formatter<'_>,
    ) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl Display for FleetNodeId {
    fn fmt(
        &self,
        f: &mut Formatter<'_>,
    ) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl Display for WorkerId {
    fn fmt(
        &self,
        f: &mut Formatter<'_>,
    ) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl std::fmt::Debug for WorkerId {
    fn fmt(
        &self,
        f: &mut Formatter<'_>,
    ) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.0, f)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct WorkerCount(u8);
impl WorkerCount {
    pub const ZERO: Self = Self(0);

    pub const fn new(value: u8) -> Self {
        Self(value)
    }

    pub const fn raw(self) -> u8 {
        self.0
    }

    pub const fn is_zero(self) -> bool {
        self.0 == 0
    }

    pub const fn as_usize(self) -> usize {
        self.0 as usize
    }
}

impl From<u8> for WorkerCount {
    fn from(value: u8) -> Self {
        Self::new(value)
    }
}

impl Display for WorkerCount {
    fn fmt(
        &self,
        f: &mut Formatter<'_>,
    ) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Immutable number of physical Orbit node lanes reserved for one shared fleet
/// generation. Every process family joining that fleet must agree on it.
/// Claviron currently reserves node zero for a master/standalone process and
/// supports at most 255 additional lanes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct FleetCapacity(u16);

/// Decimal only; anything else stops the build.
const fn parse_compiled_u16(value: &str) -> u16 {
    let bytes = value.as_bytes();
    assert!(!bytes.is_empty(), "CLAVIRON_FLEET_CAPACITY cannot be empty");
    let mut parsed: u16 = 0;
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        assert!(
            byte >= b'0' && byte <= b'9',
            "CLAVIRON_FLEET_CAPACITY must be a decimal number"
        );
        parsed = match parsed.checked_mul(10) {
            Some(shifted) => match shifted.checked_add((byte - b'0') as u16) {
                Some(next) => next,
                None => panic!("CLAVIRON_FLEET_CAPACITY overflows"),
            },
            None => panic!("CLAVIRON_FLEET_CAPACITY overflows"),
        };
        index += 1;
    }
    parsed
}

impl FleetCapacity {
    pub const MIN: u16 = 1;
    pub const MAX: u16 = u8::MAX as u16 + 1;
    pub const ONE: Self = Self(1);

    /// Seats when the build pins nothing: a master and sixteen workers.
    pub const DEFAULT: Self = Self(17);

    /// The capacity this binary was built for: `CLAVIRON_FLEET_CAPACITY` in
    /// the application's `.cargo/config.toml`, beside the ring geometry it
    /// has to agree with, or [`Self::DEFAULT`]. Lane geometry is what every
    /// peer must share, so it belongs with the rest of the geometry rather
    /// than in an option or an environment file a host can edit alone; there
    /// is no runtime way to set it.
    pub const BUILT: Self = match option_env!("CLAVIRON_FLEET_CAPACITY") {
        Some(value) => match Self::new(parse_compiled_u16(value)) {
            Some(capacity) => capacity,
            None => panic!("CLAVIRON_FLEET_CAPACITY must be 1..=256"),
        },
        None => Self::DEFAULT,
    };

    pub const fn new(value: u16) -> Option<Self> {
        if value >= Self::MIN && value <= Self::MAX {
            Some(Self(value))
        } else {
            None
        }
    }

    pub const fn raw(self) -> u16 {
        self.0
    }

    pub const fn worker_capacity(self) -> u8 {
        (self.0 - 1) as u8
    }

    pub const fn admits(
        self,
        worker_count: WorkerCount,
    ) -> bool {
        worker_count.raw() <= self.worker_capacity()
    }
}

impl Display for FleetCapacity {
    fn fmt(
        &self,
        f: &mut Formatter<'_>,
    ) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl RunMode {
    pub fn worker_id(&self) -> Option<u8> {
        match self {
            Self::Worker(worker_id) => Some(worker_id.raw()),
            Self::Master | Self::Standalone => None,
        }
    }

    pub fn as_env_value(&self) -> &'static str {
        match self {
            Self::Master => "master",
            Self::Worker(_) => "worker",
            Self::Standalone => "standalone",
        }
    }

    /// Human-readable process role for logs and diagnostics.
    ///
    /// Unlike `as_env_value`, this includes the worker id because logs
    /// answer "who is speaking?" rather than "which broad mode is this?".
    pub fn as_process_label(&self) -> String {
        match self {
            Self::Master => "master".to_string(),
            Self::Worker(worker_id) => format!("worker_{}", worker_id.raw()),
            Self::Standalone => "standalone".to_string(),
        }
    }
}

/// Non-exported inner state that actually holds the fields.
struct Inner {
    runtime_started_at: SystemTime,
    runtime_started: Instant,
    run_mode: RunMode,
    fleet_node_id: FleetNodeId,
    /// Supervisor desired state. Mutable only in the master process; worker
    /// processes receive the current value in their private bootstrap envelope.
    worker_count: AtomicU8,
    /// Physical Orbit mapping geometry. Frozen for the shared fleet generation.
    fleet_capacity: FleetCapacity,
    config_dir: PathBuf,
    /// Process-instance identity. Defaults to the binary's basename
    /// (`argv[0]`) and may be overridden via the embedding application's
    /// typed server options. Used as JWT issuer/scope, pubsub envelope
    /// filter, gRPC greeter, and the `hosting.servers.name` lookup key.
    name: Arc<str>,
    /// Physical Orbit sharing domain, independent of process-instance identity.
    fleet_name: Arc<str>,
    hostname: Arc<str>,
    /// Frozen snapshot of the process env, captured at `State::initialize`
    /// — before any provider runs and before the worker scrub. Single
    /// Legacy consumers retain these startup values across reloads. Congif
    /// consumers instead use `config_environment`, whose recipe can refresh
    /// file sources without reading the live process environment. `EnvSnapshot`
    /// is already a cheap clone (`Arc<HashMap>` inside).
    env_snapshot: EnvSnapshot,
    shutdown_token: CancellationToken,
    registry: Arc<Registry<State>>,
    config_reader: Arc<congif::ConfigReader>,
    config_environment: dotenw::EnvHandle,
    /// Process-local event callbacks and the underlying typed event bus.
    /// The registry owns the same dispatcher Arc as its single runnable.
    /// See `crate::events` for the design rationale.
    events: Arc<EventDispatcher>,
    control_pid: u32,
    reload_count: AtomicUsize,
}

/// Public handle type. Cloning is cheap and shares the same inner state.
#[derive(Clone)]
pub struct State(Arc<Inner>);

impl State {
    /// Build a standalone state handle for configless or single-process embedders.
    ///
    /// `worker_count()` reports `0` because no worker children exist.
    /// `serving_worker_count()` reports `1` because the current process
    /// serves traffic by itself.
    pub fn standalone(
        name: impl Into<Arc<str>>,
        config_dir: impl Into<PathBuf>,
    ) -> Self {
        Self::standalone_in_fleet(name, DEFAULT_FLEET_NAME, config_dir)
    }

    /// Build a standalone state in an explicitly selected Orbit fleet.
    pub fn standalone_in_fleet(
        name: impl Into<Arc<str>>,
        fleet_name: impl Into<Arc<str>>,
        config_dir: impl Into<PathBuf>,
    ) -> Self {
        Self::initialize_in_fleet(
            RunMode::Standalone,
            FleetNodeId::ZERO,
            WorkerCount::ZERO,
            FleetCapacity::ONE,
            Some(name.into()),
            fleet_name,
            config_dir,
        )
    }

    /// Boot and build state in the default [`DEFAULT_FLEET_NAME`] fleet.
    pub fn initialize(
        run_mode: RunMode,
        fleet_node_id: FleetNodeId,
        worker_count: WorkerCount,
        fleet_capacity: FleetCapacity,
        name: Option<Arc<str>>,
        config_dir: impl Into<PathBuf>,
    ) -> Self {
        Self::initialize_in_fleet(
            run_mode,
            fleet_node_id,
            worker_count,
            fleet_capacity,
            name,
            DEFAULT_FLEET_NAME,
            config_dir,
        )
    }

    /// Boot and build state in an explicitly selected Orbit fleet.
    pub fn initialize_in_fleet(
        run_mode: RunMode,
        fleet_node_id: FleetNodeId,
        worker_count: WorkerCount,
        fleet_capacity: FleetCapacity,
        name: Option<Arc<str>>,
        fleet_name: impl Into<Arc<str>>,
        config_dir: impl Into<PathBuf>,
    ) -> Self {
        Self::initialize_with_hostname_in_fleet(
            run_mode,
            fleet_node_id,
            worker_count,
            fleet_capacity,
            name,
            fleet_name,
            claviron_sys::short_hostname(),
            config_dir,
        )
    }

    /// Boot state with a hostname captured earlier in the same process setup,
    /// joining the default [`DEFAULT_FLEET_NAME`] fleet.
    pub fn initialize_with_hostname(
        run_mode: RunMode,
        fleet_node_id: FleetNodeId,
        worker_count: WorkerCount,
        fleet_capacity: FleetCapacity,
        name: Option<Arc<str>>,
        hostname: impl Into<Arc<str>>,
        config_dir: impl Into<PathBuf>,
    ) -> Self {
        Self::initialize_with_hostname_in_fleet(
            run_mode,
            fleet_node_id,
            worker_count,
            fleet_capacity,
            name,
            DEFAULT_FLEET_NAME,
            hostname,
            config_dir,
        )
    }

    /// Boot state with an explicit Orbit fleet and pre-captured hostname.
    #[allow(clippy::too_many_arguments)]
    pub fn initialize_with_hostname_in_fleet(
        run_mode: RunMode,
        fleet_node_id: FleetNodeId,
        worker_count: WorkerCount,
        fleet_capacity: FleetCapacity,
        name: Option<Arc<str>>,
        fleet_name: impl Into<Arc<str>>,
        hostname: impl Into<Arc<str>>,
        config_dir: impl Into<PathBuf>,
    ) -> Self {
        let hostname = hostname.into();
        let inherited = EnvSnapshot::capture(&hostname);
        let environment = ConfigEnvironment::from_snapshot(
            dotenw::Snapshot::new(inherited.values().clone()),
            crate::env::PRODUCTION,
            None,
        )
        .expect("captured environment is valid for configuration");
        Self::initialize_with_environment_in_fleet(
            run_mode,
            fleet_node_id,
            worker_count,
            fleet_capacity,
            name,
            fleet_name,
            hostname,
            config_dir,
            environment,
        )
    }

    /// Build state using an environment already prepared by the process host.
    /// File selection and failure reporting happen before logging/registration.
    #[allow(clippy::too_many_arguments)]
    pub fn initialize_with_environment_in_fleet(
        run_mode: RunMode,
        fleet_node_id: FleetNodeId,
        worker_count: WorkerCount,
        fleet_capacity: FleetCapacity,
        name: Option<Arc<str>>,
        fleet_name: impl Into<Arc<str>>,
        hostname: impl Into<Arc<str>>,
        config_dir: impl Into<PathBuf>,
        environment: ConfigEnvironment,
    ) -> Self {
        assert!(
            fleet_capacity.admits(worker_count),
            "worker count exceeds fleet capacity"
        );
        assert!(
            fleet_node_id.raw() < fleet_capacity.raw(),
            "fleet node id exceeds fleet capacity"
        );
        match run_mode {
            RunMode::Standalone => {
                assert!(
                    worker_count.is_zero(),
                    "standalone cannot have worker children"
                );
            }
            RunMode::Master => {}
            RunMode::Worker(worker_id) => {
                assert!(
                    worker_id.raw() > 0,
                    "worker id must be greater than zero"
                );
                assert!(
                    worker_id.raw() <= worker_count.raw(),
                    "worker id exceeds desired worker count"
                );
            }
        }
        // Normalize to a directory: if the caller passed a file path
        // (e.g. `etc/claviron/claviron.yaml`), use its parent. If they
        // passed a directory (e.g. `etc/claviron/`), keep as-is.
        let raw = config_dir.into();
        let config_dir = if raw.is_dir() {
            raw
        } else {
            raw.parent().unwrap_or(std::path::Path::new(".")).to_path_buf()
        };

        let hostname = hostname.into();
        assert!(!hostname.is_empty(), "hostname must not be empty");
        let name = name.unwrap_or_else(|| Arc::from(default_name()));
        let fleet_name = fleet_name.into();
        assert!(!fleet_name.is_empty(), "fleet name must not be empty");
        assert!(
            !fleet_name.contains('/') && !fleet_name.contains('\0'),
            "fleet name must not contain '/' or a NUL byte"
        );
        let control_pid = control_pid_for_run_mode(run_mode);
        let events = Arc::new(EventDispatcher::default());
        let registry = Arc::new(Registry::<State>::default());
        registry
            .register(events.clone())
            .expect("fresh runtime registry accepts EventDispatcher");

        let ConfigEnvironment {
            snapshot: env_snapshot,
            store: environments,
            handle: config_environment,
        } = environment;
        Self(Arc::new(Inner {
            runtime_started_at: SystemTime::now(),
            runtime_started: Instant::now(),
            run_mode,
            fleet_node_id,
            worker_count: AtomicU8::new(worker_count.raw()),
            fleet_capacity,
            config_dir,
            shutdown_token: CancellationToken::default(),
            registry,
            config_reader: Arc::new(
                congif::ConfigReader::new()
                    .with_environment_store(environments)
                    .with_builtin_parsers()
                    .expect("fresh config reader accepts builtin parsers"),
            ),
            name,
            fleet_name,
            env_snapshot,
            config_environment,
            hostname,
            events,
            control_pid,
            reload_count: AtomicUsize::new(0),
        }))
    }

    pub fn registry(&self) -> Arc<Registry<State>> {
        Arc::clone(&self.0.registry)
    }

    /// The process-owned configuration catalog, shared by all config owners.
    pub fn config_reader(&self) -> &congif::ConfigReader {
        &self.0.config_reader
    }

    /// Share the catalog without retaining the entire application state.
    pub fn config_reader_handle(&self) -> Arc<congif::ConfigReader> {
        Arc::clone(&self.0.config_reader)
    }

    /// Deployment environment selected at startup (for example `production`).
    /// It is not inferred from dotenv values or the physical hostname.
    pub fn environment(&self) -> &str {
        self.config_reader().environments().environment()
    }

    /// Whether the selected environment is [`PRODUCTION`](crate::env::PRODUCTION).
    /// The one question to ask before behaving differently in production.
    pub fn is_production(&self) -> bool {
        self.environment() == crate::env::PRODUCTION
    }

    /// Bind a provider's config to the process-owned environment recipe.
    /// This does not read or modify the live process environment.
    pub fn bind_config_environment<C: congif::Configurable + 'static>(
        &self
    ) -> congif::Result<()> {
        self.config_reader()
            .bind_environment::<C>(self.0.config_environment.clone())
    }

    /// The environment a program — a PHP application or script, a task's
    /// process — is started with, registered with the configuration reader's
    /// store so a reload re-reads its files.
    ///
    /// Each relative `files` entry is resolved from the process working
    /// directory (not the config directory or an executor's `cwd`) and selects
    /// that file and, beside it, `<file>.<environment>` for
    /// [`environment`](Self::environment) — the layering the runtime's own
    /// `.env` + `.env.<environment>` follows. The first file must exist, the
    /// rest and every variant are optional. Later files override earlier
    /// ones and `values` override every file. `${NAME}` in a file expands
    /// from the entries before it and from the configuration environment
    /// (the runtime's `.env` files over the captured process environment),
    /// but the result holds only what the files and values declare: the
    /// program's environment, not the host's. The owner keeps the handle per
    /// program and takes `snapshot()` when it starts one.
    pub fn program_environment(
        &self,
        files: impl IntoIterator<Item = impl AsRef<std::path::Path>>,
        values: impl IntoIterator<Item = (String, String)>,
    ) -> congif::Result<dotenw::EnvHandle> {
        let recipe = self.program_recipe(files, values)?;
        Ok(self.config_reader().environments().register(recipe)?)
    }

    /// Move a program's handle to a new recipe — its files or values changed
    /// in a reload — keeping the handle, and so the store's node, the same.
    pub fn replace_program_environment(
        &self,
        handle: &dotenw::EnvHandle,
        files: impl IntoIterator<Item = impl AsRef<std::path::Path>>,
        values: impl IntoIterator<Item = (String, String)>,
    ) -> congif::Result<()> {
        let recipe = self.program_recipe(files, values)?;
        Ok(handle.replace(recipe)?)
    }

    /// Evaluate one execution's environment without registering a catalog node.
    /// Files and deployment variants use the same recipe as
    /// [`Self::program_environment`]; no values are written to process env.
    pub fn read_program_environment(
        &self,
        files: impl IntoIterator<Item = impl AsRef<std::path::Path>>,
        values: impl IntoIterator<Item = (String, String)>,
    ) -> congif::Result<dotenw::Snapshot> {
        let recipe = self.program_recipe(files, values)?;
        Ok(self.config_reader().environments().read(recipe)?)
    }

    /// Watch only for the lifetime of a running execution. Its existing
    /// cancellation wait can select on `notified()` without another task.
    /// Dropping the notification unregisters this execution, not its peers.
    pub fn observe_program_environment(
        &self,
        files: impl IntoIterator<Item = impl AsRef<std::path::Path>>,
        values: impl IntoIterator<Item = (String, String)>,
    ) -> congif::Result<dotenw::EnvNotification> {
        let recipe = self.program_recipe(files, values)?;
        Ok(self.config_reader().environments().observe(recipe)?)
    }

    /// Build an execution recipe without retaining AppState in a hosted owner.
    /// The recipe carries its base handle, not the registry or process lifecycle.
    pub fn program_recipe(
        &self,
        files: impl IntoIterator<Item = impl AsRef<std::path::Path>>,
        values: impl IntoIterator<Item = (String, String)>,
    ) -> congif::Result<dotenw::Environment> {
        let mut recipe = dotenw::Environment::new()
            .with_base(&self.0.config_environment)
            .detached();
        for (index, file) in files.into_iter().enumerate() {
            let file = file.as_ref();
            let path = file.to_path_buf();
            if index == 0 && !path.is_file() {
                return Err(congif::Error::msg(format!(
                    "env_file {} does not exist",
                    path.display()
                )));
            }
            recipe =
                recipe.with_environment_file(path, dotenw::MergeMode::Override);
        }
        let values: dotenw::Values = values.into_iter().collect();
        if !values.is_empty() {
            recipe = recipe.with_values(values, dotenw::MergeMode::Override);
        }
        Ok(recipe)
    }

    pub fn registry_ref(&self) -> &Registry<State> {
        &self.0.registry
    }

    pub fn is_shutting_down(&self) -> bool {
        self.0.shutdown_token.is_cancelled()
    }

    pub fn initiate_shutdown(&self) {
        self.0.events.initiate_shutdown(&self.0.shutdown_token);
        tracing::info!(
            "💥 Shutdown initiated. Graceful shutdown in progress..."
        );
    }

    /// Process-local events. Register callbacks with `listen` during setup;
    /// `emit` publishes through the shared primitive and `subscribe` still
    /// exposes a receiver for callers that own their receive loop.
    /// See `crate::events` for usage.
    pub fn events(&self) -> &EventDispatcher {
        &self.0.events
    }

    pub fn on_shutdown(&self) -> impl Future<Output = ()> + '_ {
        self.0.shutdown_token.cancelled()
    }

    pub fn shutdown_token(&self) -> CancellationToken {
        self.0.shutdown_token.clone()
    }

    pub fn is_master(&self) -> bool {
        matches!(self.0.run_mode, RunMode::Master)
    }

    pub fn is_worker(&self) -> bool {
        matches!(self.0.run_mode, RunMode::Worker(_))
    }

    /// Single-process mode (no master/worker split). When true, in-process
    /// state mutations (e.g. cert resolver upserts) are fully effective —
    /// in master/worker topologies the same mutation must be broadcast.
    pub fn is_standalone(&self) -> bool {
        matches!(self.0.run_mode, RunMode::Standalone)
    }

    /// Get the worker number (1, 2, 3...) if this is a worker process
    pub fn worker_id(&self) -> Option<u8> {
        self.0.run_mode.worker_id()
    }

    /// Physical Orbit lane identity assigned at process-family boot.
    pub fn fleet_node_id(&self) -> FleetNodeId {
        self.0.fleet_node_id
    }

    /// PID to use for process-family control operations such as reload and shutdown.
    ///
    /// Master and standalone processes use their own PID. Workers use their
    /// parent PID, which is the master during normal supervised worker boot.
    pub fn control_pid(&self) -> u32 {
        self.0.control_pid
    }

    /// Immutable physical node-lane geometry reserved for Orbit surfaces.
    pub fn fleet_capacity(&self) -> FleetCapacity {
        self.0.fleet_capacity
    }

    /// Maximum number of worker ids admitted by the physical fleet geometry.
    pub fn worker_capacity(&self) -> u8 {
        self.0.fleet_capacity.worker_capacity()
    }

    /// Number of configured worker children for this run.
    ///
    /// * `Standalone` → 0 (no worker children exist)
    /// * `Master` → supervisor desired count
    /// * `Worker(_)` → count supplied by the supervisor bootstrap
    pub fn worker_count(&self) -> u8 {
        self.0.worker_count.load(Ordering::Acquire)
    }

    /// Replace the master process's desired worker count.
    ///
    /// This is a low-level supervisor integration hook. Fleet management must
    /// reconcile the added or removed slot in the process table. Retained
    /// workers keep their bootstrap count; current fleet membership comes from
    /// the supervisor's authoritative observation, not that birth snapshot.
    pub fn set_worker_count(
        &self,
        worker_count: WorkerCount,
    ) -> Result<(), String> {
        if !self.is_master() {
            return Err(
                "worker count can only be changed by the master process"
                    .to_string(),
            );
        }
        if !self.fleet_capacity().admits(worker_count) {
            return Err(format!(
                "worker count {} exceeds fleet worker capacity {}",
                worker_count.raw(),
                self.worker_capacity()
            ));
        }
        self.0.worker_count.store(worker_count.raw(), Ordering::Release);
        Ok(())
    }

    /// Number of traffic-serving slots represented by this run.
    ///
    /// Standalone has no worker children but still has one serving
    /// process. Master/worker fleet runs use the configured worker
    /// count because the master is control-plane only for traffic.
    pub fn serving_worker_count(&self) -> u8 {
        match self.0.run_mode {
            RunMode::Standalone => 1,
            RunMode::Master | RunMode::Worker(_) => self.worker_count(),
        }
    }

    /// Number of runtime processes participating in process-wide shared
    /// state: standalone is one process, master/worker topologies are
    /// `master + workers`.
    pub fn runtime_process_count(&self) -> u16 {
        u16::from(self.worker_count()) + 1
    }

    /// Directory holding all per-service yaml files (`http.yaml`,
    /// `vhost.yaml`, `tls.yaml`, …). Service loaders should join their
    /// filename onto this directly.
    pub fn config_dir(&self) -> &std::path::Path {
        &self.0.config_dir
    }

    pub fn hostname(&self) -> &str {
        &self.0.hostname
    }

    /// Process-instance identity — the override passed at initialize time, or
    /// the binary basename if none was supplied.
    pub fn name(&self) -> &str {
        &self.0.name
    }

    /// Physical Orbit sharing domain selected by the embedding composition.
    /// This is independent of [`Self::name`].
    pub fn fleet_name(&self) -> &str {
        &self.0.fleet_name
    }

    pub fn run_mode(&self) -> RunMode {
        self.0.run_mode
    }

    /// Frozen snapshot of the process env captured at construction.
    /// Single source of truth for `${ENV_KEY}` substitution in all
    /// providers; survives the worker scrub that runs post-`boot_all`.
    pub fn env_snapshot(&self) -> &EnvSnapshot {
        &self.0.env_snapshot
    }

    pub fn reload_count(&self) -> usize {
        self.0.reload_count.load(Ordering::Relaxed)
    }

    /// Start of this process's runtime, not the master or the whole fleet.
    pub fn runtime_started_at(&self) -> SystemTime {
        self.0.runtime_started_at
    }

    /// Monotonic age of this process's runtime; reload does not reset it.
    pub fn runtime_uptime(&self) -> Duration {
        self.0.runtime_started.elapsed()
    }
}

fn control_pid_for_run_mode(run_mode: RunMode) -> u32 {
    match run_mode {
        RunMode::Master | RunMode::Standalone => std::process::id(),
        RunMode::Worker(_) => {
            let parent = unsafe { libc::getppid() };
            u32::try_from(parent).expect("parent pid should be non-negative")
        }
    }
}

#[async_trait]
/// Supply the process catalog; Continuo owns configuration preparation and
/// application acknowledgement around the provider reload hooks.
impl ReloadState for State {
    fn reload_config_reader(&self) -> Option<&congif::ConfigReader> {
        Some(self.config_reader())
    }

    async fn reload(&self) -> crate::registry::Result<()> {
        self.0.reload_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

impl SharedState for State {
    fn process_scope(&self) -> continuo::fleet::Scope {
        self.run_mode().into()
    }

    fn shutdown_token(&self) -> CancellationToken {
        State::shutdown_token(self)
    }

    fn initiate_shutdown(&self) {
        State::initiate_shutdown(self);
    }

    fn is_shutting_down(&self) -> bool {
        State::is_shutting_down(self)
    }
}

impl congif::HasConfigReader for State {
    fn config_reader(&self) -> &congif::ConfigReader {
        State::config_reader(self)
    }
}

impl HasRegistry for State {
    fn registry_ref(&self) -> &Registry<Self> {
        State::registry_ref(self)
    }
}

impl HasEvents for State {
    fn events(&self) -> &ProcessEventBus {
        State::events(self).bus()
    }
}

/// Default process-instance name — the basename of `argv[0]`. Used when
/// the embedding application does not supply an explicit instance name.
pub fn default_name() -> String {
    env::args()
        .next()
        .and_then(|path| {
            PathBuf::from(path)
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
        })
        .unwrap_or_else(|| "unknown".to_string())
}

#[cfg(test)]
#[path = "../tests/state.rs"]
mod tests;
