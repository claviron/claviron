//! Private master-to-worker process bootstrap protocol.
//!
//! Worker identity is not part of an embedding application's CLI. The master
//! resolves the process-family options once, serializes this envelope into the
//! child environment, and re-executes the current binary. Every downstream
//! binary can therefore own an arbitrary CLI without the supervisor assuming
//! flags such as `--worker-id` or `--config-dir` exist.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Command;

use claviron_runtime::{
    FleetCapacity, FleetNodeId, LaunchBlob, RunMode, WorkerCount, WorkerId,
};
use claviron_sys::process::{self, ProcessIdentity};
use serde::{Deserialize, Serialize};

use crate::lifecycle::MasterIdentity;
use crate::{Error, Result};

/// Reserved environment key carrying the versioned worker bootstrap envelope.
///
/// This is an internal process-family protocol, not an operator-facing config
/// value. Downstream applications must not set it themselves.
pub const WORKER_BOOTSTRAP_ENV: &str = "CLAVIRON_INTERNAL_WORKER_BOOTSTRAP";

const BOOTSTRAP_VERSION: u8 = 6;

/// How the current executable entered the Claviron server bootstrap.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProcessEntry {
    /// Externally launched process. Server options decide whether it becomes
    /// standalone or a master with workers.
    Primary,
    /// Child process re-executed by [`crate::WorkerControl`].
    Worker(WorkerBootstrap),
}

impl ProcessEntry {
    /// Detect and validate the current process entry from the reserved worker
    /// bootstrap environment.
    ///
    /// A missing envelope means only "primary entry"; it does not by itself
    /// mean master. A malformed or unsupported envelope is a hard startup
    /// error and never falls back to primary mode.
    ///
    /// The envelope names the master as an exact process. The parent must be
    /// that process — same PID *and* same kernel start stamp — so an envelope
    /// replayed under a different parent, or under a process that inherited
    /// the master's PID, is refused.
    pub fn detect() -> Result<Self> {
        let Some(value) = std::env::var_os(WORKER_BOOTSTRAP_ENV) else {
            return Ok(Self::Primary);
        };

        let worker = WorkerBootstrap::decode(value)?;
        let parent_pid = u32::try_from(nix::unistd::getppid().as_raw())
            .map_err(|_| Error::bootstrap("parent pid must be non-negative"))?;
        let master = worker.master().process();
        if master.pid() != parent_pid {
            return Err(Error::bootstrap(format!(
                "worker bootstrap master pid {} does not match parent pid {parent_pid}",
                master.pid()
            )));
        }
        let parent = process::identity(parent_pid).ok_or_else(|| {
            Error::bootstrap(format!(
                "cannot read the kernel identity of parent pid {parent_pid}"
            ))
        })?;
        if parent != master {
            return Err(Error::bootstrap(format!(
                "worker bootstrap master start stamp {} does not match parent pid {parent_pid} start stamp {}",
                master.started_at(),
                parent.started_at()
            )));
        }

        Ok(Self::Worker(worker))
    }
}

/// Immutable process-family values assigned to one worker by the master.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerBootstrap {
    worker_id: WorkerId,
    fleet_node_id: FleetNodeId,
    worker_count: WorkerCount,
    fleet_capacity: FleetCapacity,
    incarnation: u64,
    master: MasterIdentity,
    instance_name: String,
    fleet_name: String,
    environment: String,
    config_dir: PathBuf,
    /// What the launch decided, exactly as the embedding application encoded
    /// it. Carried, never read: the supervisor has no business parsing an
    /// application's options, and this exists only because a worker is a
    /// re-exec that deliberately does not inherit the command line.
    launch: LaunchBlob,
}

impl WorkerBootstrap {
    /// Construct and validate a worker bootstrap envelope.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        worker_id: WorkerId,
        fleet_node_id: FleetNodeId,
        worker_count: WorkerCount,
        fleet_capacity: FleetCapacity,
        incarnation: u64,
        master: MasterIdentity,
        instance_name: impl Into<String>,
        fleet_name: impl Into<String>,
        config_dir: impl Into<PathBuf>,
        launch: LaunchBlob,
    ) -> Result<Self> {
        if launch.as_bytes().len() > LaunchBlob::MAX {
            return Err(Error::bootstrap(format!(
                "launch options are {} bytes; the envelope carries at most {}. `execve` bounds \
                 the environment and the command line together, so a launch this large would \
                 fail the worker at exec with a message about neither",
                launch.as_bytes().len(),
                LaunchBlob::MAX
            )));
        }
        let instance_name = instance_name.into();
        let fleet_name = fleet_name.into();
        let config_dir = config_dir.into();

        if worker_id.raw() == 0 {
            return Err(Error::bootstrap(
                "worker id must be greater than zero",
            ));
        }
        if fleet_node_id.raw() >= fleet_capacity.raw() {
            return Err(Error::bootstrap(format!(
                "fleet node id {} is outside fleet capacity {}",
                fleet_node_id.raw(),
                fleet_capacity.raw()
            )));
        }
        if worker_count.is_zero() {
            return Err(Error::bootstrap(
                "worker count must be greater than zero",
            ));
        }
        if !fleet_capacity.admits(worker_count) {
            return Err(Error::bootstrap(format!(
                "worker count {} exceeds fleet worker capacity {}",
                worker_count.raw(),
                fleet_capacity.worker_capacity()
            )));
        }
        if worker_id.raw() > worker_count.raw() {
            return Err(Error::bootstrap(format!(
                "worker id {} exceeds worker count {}",
                worker_id.raw(),
                worker_count.raw()
            )));
        }
        if incarnation == 0 {
            return Err(Error::bootstrap(
                "worker incarnation must be greater than zero",
            ));
        }
        if master.process().pid() == 0 {
            return Err(Error::bootstrap(
                "master pid must be greater than zero",
            ));
        }
        if master.generation() == 0 {
            return Err(Error::bootstrap(
                "master generation must be greater than zero",
            ));
        }
        if instance_name.trim().is_empty() {
            return Err(Error::bootstrap("instance name must not be empty"));
        }
        if fleet_name.is_empty() {
            return Err(Error::bootstrap("fleet name must not be empty"));
        }
        if fleet_name.contains('/') || fleet_name.contains('\0') {
            return Err(Error::bootstrap(
                "fleet name must not contain '/' or a NUL byte",
            ));
        }
        if config_dir.as_os_str().is_empty() {
            return Err(Error::bootstrap("config directory must not be empty"));
        }

        Ok(Self {
            worker_id,
            fleet_node_id,
            worker_count,
            fleet_capacity,
            incarnation,
            master,
            instance_name,
            fleet_name,
            environment: claviron_runtime::env::PRODUCTION.into(),
            config_dir,
            launch,
        })
    }

    pub const fn worker_id(&self) -> WorkerId {
        self.worker_id
    }

    pub const fn fleet_node_id(&self) -> FleetNodeId {
        self.fleet_node_id
    }

    pub const fn worker_count(&self) -> WorkerCount {
        self.worker_count
    }

    pub const fn fleet_capacity(&self) -> FleetCapacity {
        self.fleet_capacity
    }

    /// Which incarnation of its slot this process is, in the master's count.
    pub const fn incarnation(&self) -> u64 {
        self.incarnation
    }

    /// The exact master process that spawned this worker, and its generation.
    pub const fn master(&self) -> MasterIdentity {
        self.master
    }

    pub fn instance_name(&self) -> &str {
        &self.instance_name
    }

    pub fn fleet_name(&self) -> &str {
        &self.fleet_name
    }

    /// Carry the master's deployment selection, independent of a worker's CLI.
    pub fn with_environment(
        mut self,
        environment: impl Into<String>,
    ) -> Result<Self> {
        let environment = environment.into();
        claviron_runtime::env::ConfigEnvironment::validate_environment(
            &environment,
        )
        .map_err(|error| Error::bootstrap(error.to_string()))?;
        self.environment = environment;
        Ok(self)
    }

    pub fn environment(&self) -> &str {
        &self.environment
    }

    pub fn config_dir(&self) -> &Path {
        &self.config_dir
    }

    /// What the launch decided, for the embedding application to decode.
    pub fn launch(&self) -> &LaunchBlob {
        &self.launch
    }

    pub const fn run_mode(&self) -> RunMode {
        RunMode::Worker(self.worker_id)
    }

    /// Attach this bootstrap envelope to a worker command.
    pub fn apply_to(
        &self,
        command: &mut Command,
    ) -> Result<()> {
        command.env(WORKER_BOOTSTRAP_ENV, self.encode()?);
        Ok(())
    }

    fn encode(&self) -> Result<String> {
        serde_json::to_string(&WorkerBootstrapWire::from(self)).map_err(|err| {
            Error::bootstrap(format!(
                "failed to encode worker bootstrap: {err}"
            ))
        })
    }

    fn decode(value: OsString) -> Result<Self> {
        let value = value.into_string().map_err(|_| {
            Error::bootstrap(format!(
                "{WORKER_BOOTSTRAP_ENV} must contain valid UTF-8"
            ))
        })?;
        let wire: WorkerBootstrapWire =
            serde_json::from_str(&value).map_err(|err| {
                Error::bootstrap(format!(
                    "failed to decode {WORKER_BOOTSTRAP_ENV}: {err}"
                ))
            })?;

        if wire.version != BOOTSTRAP_VERSION {
            return Err(Error::bootstrap(format!(
                "unsupported worker bootstrap version {}; expected {}",
                wire.version, BOOTSTRAP_VERSION
            )));
        }

        let launch = match wire.launch.as_deref() {
            None => LaunchBlob::default(),
            Some(text) => decode_launch(text)?,
        };
        Self::new(
            WorkerId::new(wire.worker_id),
            FleetNodeId::new(wire.fleet_node_id),
            WorkerCount::new(wire.worker_count),
            FleetCapacity::new(wire.fleet_capacity).ok_or_else(|| {
                Error::bootstrap(format!(
                    "fleet capacity must be {}..={}, got {}",
                    FleetCapacity::MIN,
                    FleetCapacity::MAX,
                    wire.fleet_capacity
                ))
            })?,
            wire.incarnation,
            MasterIdentity::new(
                ProcessIdentity::new(wire.master.pid, wire.master.started_at),
                wire.master.generation,
            ),
            wire.instance_name,
            wire.fleet_name,
            wire.config_dir,
            launch,
        )?
        .with_environment(wire.environment)
    }
}

/// Private re-exec envelope before semantic validation into WorkerBootstrap.
/// Separates serialized protocol fields from trusted process/topology identity;
/// malformed input must not become a primary-process launch.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WorkerBootstrapWire {
    version: u8,
    worker_id: u8,
    fleet_node_id: u16,
    worker_count: u8,
    fleet_capacity: u16,
    incarnation: u64,
    master: MasterIdentityWire,
    instance_name: String,
    fleet_name: String,
    environment: String,
    config_dir: PathBuf,
    /// Base64 because an environment value carries no NUL and the blob is
    /// arbitrary bytes. Absent means the launch set none, which is not the
    /// same as setting an empty one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    launch: Option<String>,
}

/// The master's kernel identity and generation, as bytes on the wire. The
/// start stamp is the raw kernel value: platform-specific, never a wall clock.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct MasterIdentityWire {
    pid: u32,
    started_at: u64,
    generation: u64,
}

impl From<&WorkerBootstrap> for WorkerBootstrapWire {
    fn from(value: &WorkerBootstrap) -> Self {
        Self {
            version: BOOTSTRAP_VERSION,
            worker_id: value.worker_id.raw(),
            fleet_node_id: value.fleet_node_id.raw(),
            worker_count: value.worker_count.raw(),
            fleet_capacity: value.fleet_capacity.raw(),
            incarnation: value.incarnation,
            master: MasterIdentityWire {
                pid: value.master.process().pid(),
                started_at: value.master.process().started_at(),
                generation: value.master.generation(),
            },
            instance_name: value.instance_name.clone(),
            fleet_name: value.fleet_name.clone(),
            environment: value.environment.clone(),
            config_dir: value.config_dir.clone(),
            launch: (!value.launch.is_empty())
                .then(|| encode_launch(value.launch.as_bytes())),
        }
    }
}

/// The envelope is JSON in an environment variable; the blob is bytes. This is
/// the smallest encoding that survives both, written here rather than taken as
/// a dependency because it is twenty lines and the supervisor has no other use
/// for one.
fn encode_launch(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n =
            (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for slot in 0..4 {
            if slot <= chunk.len() {
                out.push(
                    ALPHABET[((n >> (18 - slot * 6)) & 0x3f) as usize] as char,
                );
            } else {
                out.push('=');
            }
        }
    }
    out
}

fn decode_launch(text: &str) -> Result<LaunchBlob> {
    let mut bytes = Vec::with_capacity(text.len() / 4 * 3);
    let mut buffer = 0u32;
    let mut held = 0u8;
    for byte in text.bytes() {
        let value = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            b'=' => break,
            _ => {
                return Err(Error::bootstrap(
                    "launch options are not valid base64".to_owned(),
                ));
            }
        };
        buffer = (buffer << 6) | u32::from(value);
        held += 6;
        if held >= 8 {
            held -= 8;
            bytes.push(((buffer >> held) & 0xff) as u8);
        }
    }
    Ok(LaunchBlob::new(bytes))
}

#[cfg(test)]
#[path = "../tests/bootstrap.rs"]
mod tests;
