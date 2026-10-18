//! Transport-neutral worker status snapshots and their Orbit wire shape.
//!
//! The snapshot is the master's read model of its own process table: enough
//! for a GUI or CLI to explain a slot — including one that is blocked — without
//! reading the master's log. It is derived on every publication and stored
//! nowhere else; the table keeps the facts, this carries them.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use claviron_orbit::{Fleet, NodeId, OrbitTyped, RingSpec};
use claviron_sys::process::ProcessIdentity;

use crate::lifecycle::{
    BuildIdentity, CommandKey, DrainReason, ExitDisposition, ExitOutcome,
    FleetMode, MasterIdentity, OperationId, OperationKind, OperationOrigin,
    WorkerBlockReason, WorkerIntent, WorkerObservedPhase,
};

/// Master-published worker snapshots, including the master's runtime clock.
///
/// Kind `197` replaces the 160-byte-payload kind `240`. Incompatible shapes
/// must coexist without resizing or unlinking a segment held by older peers.
/// Boot reuses compatible storage; freshness rejects retained observations.
pub(crate) const WORKER_SNAPSHOT_RING_KIND: u8 = 197;
const WORKER_SNAPSHOT_FRAME_KIND: u8 = 1;
/// One batch holds one row per configured worker; ids are `u8`, so 256 rows
/// is the most a batch can be.
const WORKER_SNAPSHOT_RING_CAPACITY: usize = 256;
// The 171-byte wire row fits one fixed 192-byte payload bank.
const WORKER_SNAPSHOT_PAYLOAD_CAPACITY: usize = 192;
const WIRE_BYTES: usize = 171;

const FLAG_BUILD_DIRTY: u8 = 0b0000_0001;
const FLAG_MASTER: u8 = 0b0000_0010;
const FLAG_PROCESS: u8 = 0b0000_0100;
const FLAG_OPERATION: u8 = 0b0000_1000;
const FLAG_HEARTBEAT: u8 = 0b0001_0000;
const FLAG_BLOCK_REASON: u8 = 0b0010_0000;
const FLAG_COMMAND: u8 = 0b0100_0000;
const FLAG_MASTER_RUNTIME: u8 = 0b1000_0000;

/// The publishing master's runtime clocks, observed at the batch capture time.
/// Start is UTC Unix milliseconds, not the kernel stamp in `MasterIdentity`.
/// Uptime is measured monotonically by that master; neither clock resets on reload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MasterRuntimeSnapshot {
    pub started_at_unix_ms: u64,
    pub uptime: Duration,
    pub reload_revision: u64,
}

/// One complete view of the master's authoritative worker process table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerListSnapshot {
    pub captured_at: SystemTime,
    pub workers_expected: u8,
    /// The source the publishing master was built from. Compared by revision
    /// and dirty flag; the crate version does not cross the wire.
    pub build: BuildIdentity,
    /// The exact master process that published this view, once it has
    /// established itself; `None` only before that.
    pub master: Option<MasterIdentity>,
    pub master_runtime: Option<MasterRuntimeSnapshot>,
    pub fleet_mode: FleetMode,
    pub workers: Vec<WorkerStatus>,
}

/// One authoritative worker-table observation narrowed to a configured slot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerStatusSnapshot {
    pub captured_at: SystemTime,
    pub workers_expected: u8,
    pub build: BuildIdentity,
    pub master: Option<MasterIdentity>,
    pub master_runtime: Option<MasterRuntimeSnapshot>,
    pub fleet_mode: FleetMode,
    pub worker: WorkerStatus,
}

/// Operator-relevant state for one configured worker slot.
///
/// `phase` and `desired` are the two facts the concise label is derived from,
/// published so a client can show why a slot reads as it does and how long it
/// has; the label itself is [`Self::lifecycle`], computed, never stored. Each
/// `*_age` is measured by the master at capture.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerStatus {
    pub worker_id: u8,
    /// How this master owns the worker represented by the current snapshot.
    /// Recomputed from the live process handle on every publication.
    pub ownership: WorkerOwnership,
    /// The exact process in the slot — PID and kernel start stamp — when one
    /// is known: the handle this master owns, or, for a slot it could not
    /// claim, the worker-written presence that occupies the lane. `Unowned`
    /// with a process is exactly "boot is blocked by this PID".
    pub process: Option<ProcessIdentity>,
    pub uptime: Option<Duration>,
    /// Automatic crash-recovery attempts in the current recovery epoch.
    pub recovery_attempts: u32,
    /// Process incarnation for this slot across master adoption.
    pub incarnation: u64,
    /// Completed in-process reload cycles for this worker slot in the current master lifetime.
    pub reload_revision: u64,
    /// What the master has observed of the process.
    pub phase: WorkerObservedPhase,
    /// How long the slot has been in `phase`. For `DrainAcknowledged` the
    /// acknowledgement age; for `ReconciliationBlocked` how long the block has stood.
    pub phase_age: Duration,
    /// What the master wants of the slot.
    pub desired: WorkerIntent,
    /// How long `desired` has stood. For a `Drain`, how long the master has
    /// been waiting for the process to leave.
    pub desired_age: Duration,
    /// The lifecycle operation acting on this slot, if one is.
    pub operation: Option<WorkerOperation>,
    /// Since the last heartbeat the master accepted from this process; `None`
    /// until its first beat.
    pub heartbeat_age: Option<Duration>,
    /// Why boot reconciliation refused this slot, while it is `ReconciliationBlocked`.
    pub block_reason: Option<WorkerBlockReason>,
}

impl WorkerStatus {
    /// The concise label, derived from `phase` and `desired` by the same rule
    /// the master applies. Not a field: it cannot disagree with them.
    pub const fn lifecycle(&self) -> WorkerLifecycle {
        WorkerLifecycle::from_observation(self.phase, self.desired)
    }

    pub fn pid(&self) -> Option<u32> {
        self.process.map(ProcessIdentity::pid)
    }
}

/// The operation acting on a slot. Precedence follows authority: the fleet
/// shutdown outranks a fleet-wide operation, which outranks the slot's own —
/// so once shutdown has latched, a slot whose stop or restart is still running
/// reports the shutdown, which is what the master has told the worker too.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WorkerOperation {
    pub id: OperationId,
    pub kind: OperationKind,
    pub origin: OperationOrigin,
    /// How long the operation has been running.
    pub age: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerOwnership {
    /// This master has no live process handle for the slot.
    Unowned,
    /// This master spawned the worker and owns its child relationship.
    Spawned,
    /// The worker accepted this master after being spawned by an earlier one.
    Adopted,
}

impl WorkerOwnership {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unowned => "unowned",
            Self::Spawned => "spawned",
            Self::Adopted => "adopted",
        }
    }

    const fn code(self) -> u8 {
        match self {
            Self::Unowned => 1,
            Self::Spawned => 2,
            Self::Adopted => 3,
        }
    }

    fn from_code(code: u8) -> Result<Self, String> {
        match code {
            1 => Ok(Self::Unowned),
            2 => Ok(Self::Spawned),
            3 => Ok(Self::Adopted),
            _ => Err(format!("unknown worker ownership {code}")),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerLifecycle {
    Starting,
    Running,
    Reloading,
    Stopping,
    Stopped,
    Exited,
    Crashed,
    /// The slot exists but this master did not spawn it: its node lane is
    /// still being published to by a process from an earlier master.
    Unclaimed,
}

impl WorkerLifecycle {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Reloading => "reloading",
            Self::Stopping => "stopping",
            Self::Stopped => "stopped",
            Self::Exited => "exited",
            Self::Crashed => "crashed",
            Self::Unclaimed => "unclaimed",
        }
    }
}

// Wire codes for the lifecycle enums. Zero is never a value, so a zero byte
// where one is required is a malformed row, not a default.

const fn phase_code(phase: WorkerObservedPhase) -> u8 {
    match phase {
        WorkerObservedPhase::Unclaimed => 1,
        WorkerObservedPhase::OwnershipPending => 2,
        WorkerObservedPhase::ReconciliationBlocked => 3,
        WorkerObservedPhase::Starting => 4,
        WorkerObservedPhase::Running => 5,
        WorkerObservedPhase::Reloading => 6,
        WorkerObservedPhase::DrainAcknowledged => 7,
        WorkerObservedPhase::TeardownCompleted => 8,
        WorkerObservedPhase::Exited(ExitOutcome::Planned) => 9,
        WorkerObservedPhase::Exited(ExitOutcome::Failed) => 10,
    }
}

fn phase_from_code(code: u8) -> Result<WorkerObservedPhase, String> {
    Ok(match code {
        1 => WorkerObservedPhase::Unclaimed,
        2 => WorkerObservedPhase::OwnershipPending,
        3 => WorkerObservedPhase::ReconciliationBlocked,
        4 => WorkerObservedPhase::Starting,
        5 => WorkerObservedPhase::Running,
        6 => WorkerObservedPhase::Reloading,
        7 => WorkerObservedPhase::DrainAcknowledged,
        8 => WorkerObservedPhase::TeardownCompleted,
        9 => WorkerObservedPhase::Exited(ExitOutcome::Planned),
        10 => WorkerObservedPhase::Exited(ExitOutcome::Failed),
        _ => return Err(format!("unknown worker phase {code}")),
    })
}

/// `(kind, drain reason, exit disposition)`; the last two are zero unless the
/// intent is a drain.
const fn intent_codes(intent: WorkerIntent) -> (u8, u8, u8) {
    match intent {
        WorkerIntent::EnsureRunning => (1, 0, 0),
        WorkerIntent::Stopped => (2, 0, 0),
        WorkerIntent::Drain { reason, after_exit } => {
            (3, reason.code(), exit_disposition_code(after_exit))
        }
    }
}

fn intent_from_codes(
    kind: u8,
    reason: u8,
    after_exit: u8,
) -> Result<WorkerIntent, String> {
    match (kind, reason, after_exit) {
        (1, 0, 0) => Ok(WorkerIntent::EnsureRunning),
        (2, 0, 0) => Ok(WorkerIntent::Stopped),
        (3, reason, after_exit) => Ok(WorkerIntent::Drain {
            reason: DrainReason::from_code(reason)
                .ok_or_else(|| format!("unknown drain reason {reason}"))?,
            after_exit: exit_disposition_from_code(after_exit)?,
        }),
        _ => Err(format!(
            "invalid worker intent codes {kind}/{reason}/{after_exit}"
        )),
    }
}

const fn exit_disposition_code(disposition: ExitDisposition) -> u8 {
    match disposition {
        ExitDisposition::StayStopped => 1,
        ExitDisposition::Replace => 2,
        ExitDisposition::RemoveSlot => 3,
        ExitDisposition::FinishFleetShutdown => 4,
    }
}

fn exit_disposition_from_code(code: u8) -> Result<ExitDisposition, String> {
    Ok(match code {
        1 => ExitDisposition::StayStopped,
        2 => ExitDisposition::Replace,
        3 => ExitDisposition::RemoveSlot,
        4 => ExitDisposition::FinishFleetShutdown,
        _ => return Err(format!("unknown exit disposition {code}")),
    })
}

/// `(kind, resize from, resize to)`; the counts are zero unless the kind is a resize.
const fn operation_codes(kind: OperationKind) -> (u8, u8, u8) {
    match kind {
        OperationKind::Start => (1, 0, 0),
        OperationKind::Stop => (2, 0, 0),
        OperationKind::Restart => (3, 0, 0),
        OperationKind::Reload => (4, 0, 0),
        OperationKind::Rotation => (5, 0, 0),
        OperationKind::HeartbeatRecovery => (6, 0, 0),
        OperationKind::CrashRecovery => (7, 0, 0),
        OperationKind::ReloadAll => (8, 0, 0),
        OperationKind::RestartAll => (9, 0, 0),
        OperationKind::Resize { from, to } => (10, from, to),
        OperationKind::ConfigReload => (11, 0, 0),
        OperationKind::FleetShutdown => (12, 0, 0),
    }
}

fn operation_from_codes(
    kind: u8,
    from: u8,
    to: u8,
) -> Result<OperationKind, String> {
    Ok(match kind {
        1 => OperationKind::Start,
        2 => OperationKind::Stop,
        3 => OperationKind::Restart,
        4 => OperationKind::Reload,
        5 => OperationKind::Rotation,
        6 => OperationKind::HeartbeatRecovery,
        7 => OperationKind::CrashRecovery,
        8 => OperationKind::ReloadAll,
        9 => OperationKind::RestartAll,
        10 => OperationKind::Resize { from, to },
        11 => OperationKind::ConfigReload,
        12 => OperationKind::FleetShutdown,
        _ => return Err(format!("unknown operation kind {kind}")),
    })
}

const fn block_reason_code(reason: WorkerBlockReason) -> u8 {
    match reason {
        WorkerBlockReason::StalePresence => 1,
        WorkerBlockReason::WorkerNotRunning => 2,
        WorkerBlockReason::WorkerBusy => 3,
        WorkerBlockReason::UnknownMaster => 4,
        WorkerBlockReason::MasterStillAlive => 5,
        WorkerBlockReason::MasterNotOlder => 6,
        WorkerBlockReason::IdentityChanged => 7,
    }
}

fn block_reason_from_code(code: u8) -> Result<WorkerBlockReason, String> {
    Ok(match code {
        1 => WorkerBlockReason::StalePresence,
        2 => WorkerBlockReason::WorkerNotRunning,
        3 => WorkerBlockReason::WorkerBusy,
        4 => WorkerBlockReason::UnknownMaster,
        5 => WorkerBlockReason::MasterStillAlive,
        6 => WorkerBlockReason::MasterNotOlder,
        7 => WorkerBlockReason::IdentityChanged,
        _ => return Err(format!("unknown block reason {code}")),
    })
}

/// `(origin, via worker, command key)`; the worker and key are zero unless
/// the origin is a management command.
const fn origin_codes(origin: OperationOrigin) -> (u8, u8, u64) {
    match origin {
        OperationOrigin::Management { via_worker, command } => (
            1,
            match via_worker {
                Some(worker) => worker.raw(),
                None => 0,
            },
            command.raw(),
        ),
        OperationOrigin::Supervisor => (2, 0, 0),
        OperationOrigin::Signal => (3, 0, 0),
    }
}

fn origin_from_codes(
    origin: u8,
    via_worker: u8,
    command: u64,
) -> Result<OperationOrigin, String> {
    match origin {
        1 => Ok(OperationOrigin::Management {
            via_worker: (via_worker != 0)
                .then(|| claviron_runtime::WorkerId::new(via_worker)),
            command: CommandKey::from_raw(command).ok_or_else(|| {
                "management operation without a command key".to_string()
            })?,
        }),
        2 => Ok(OperationOrigin::Supervisor),
        3 => Ok(OperationOrigin::Signal),
        _ => Err(format!("unknown operation origin {origin}")),
    }
}

const fn fleet_mode_code(mode: FleetMode) -> u8 {
    match mode {
        FleetMode::Running => 1,
        FleetMode::ShuttingDown => 2,
    }
}

fn fleet_mode_from_code(code: u8) -> Result<FleetMode, String> {
    Ok(match code {
        1 => FleetMode::Running,
        2 => FleetMode::ShuttingDown,
        _ => return Err(format!("unknown fleet mode {code}")),
    })
}

/// Orbit ring marker for the master-published management snapshot batch.
/// Keeps storage geometry separate from the decoded public read-model types.
#[derive(Clone)]
pub(crate) struct WorkerSnapshotRecord;

impl OrbitTyped for WorkerSnapshotRecord {
    const KIND: u8 = WORKER_SNAPSHOT_RING_KIND;
    const RING_SPEC: RingSpec = RingSpec::per_node(
        WORKER_SNAPSHOT_RING_CAPACITY,
        WORKER_SNAPSHOT_PAYLOAD_CAPACITY,
    );
}

/// One row of the batch. The header fields (everything down to `row_index`)
/// are repeated on every row so a reader can prove the rows it reads belong to
/// one capture.
struct WireStatus {
    captured_at_ms: u64,
    max_age_ms: u64,
    build_revision: u32,
    flags: u8,
    master_pid: u32,
    master_started_at: u64,
    master_generation: u64,
    master_runtime_started_at_ms: u64,
    master_runtime_uptime_ms: u64,
    master_reload_revision: u64,
    fleet_mode: u8,
    workers_expected: u8,
    row_count: u16,
    row_index: u16,
    worker_id: u8,
    ownership: u8,
    pid: u32,
    process_started_at: u64,
    uptime_ms: u64,
    recovery_attempts: u32,
    incarnation: u64,
    reload_revision: u64,
    phase: u8,
    phase_age_ms: u64,
    intent_kind: u8,
    drain_reason: u8,
    exit_disposition: u8,
    intent_age_ms: u64,
    operation_id: u64,
    operation_kind: u8,
    operation_from: u8,
    operation_to: u8,
    operation_origin: u8,
    operation_via_worker: u8,
    operation_command: u64,
    operation_age_ms: u64,
    heartbeat_age_ms: u64,
    block_reason: u8,
}

/// Capture identity and fleet-wide metadata repeated across a snapshot batch.
/// Lets readers reject mixed captures and represent an empty fleet explicitly.
struct WireHeader {
    captured_at_ms: u64,
    max_age_ms: u64,
    build_revision: u32,
    flags: u8,
    master_pid: u32,
    master_started_at: u64,
    master_generation: u64,
    master_runtime_started_at_ms: u64,
    master_runtime_uptime_ms: u64,
    master_reload_revision: u64,
    fleet_mode: u8,
    workers_expected: u8,
    row_count: u16,
}

impl WireHeader {
    fn of(
        snapshot: &WorkerListSnapshot,
        max_age: Duration,
        row_count: u16,
    ) -> Self {
        let mut flags = 0;
        if snapshot.build.dirty() {
            flags |= FLAG_BUILD_DIRTY;
        }
        if snapshot.master.is_some() {
            flags |= FLAG_MASTER;
        }
        if snapshot.master_runtime.is_some() {
            flags |= FLAG_MASTER_RUNTIME;
        }
        Self {
            captured_at_ms: unix_millis(snapshot.captured_at),
            max_age_ms: duration_millis(max_age),
            build_revision: snapshot.build.revision().unwrap_or(0),
            flags,
            master_pid: snapshot
                .master
                .map_or(0, |master| master.process().pid()),
            master_started_at: snapshot
                .master
                .map_or(0, |master| master.process().started_at()),
            master_generation: snapshot
                .master
                .map_or(0, MasterIdentity::generation),
            master_runtime_started_at_ms: snapshot
                .master_runtime
                .map_or(0, |runtime| runtime.started_at_unix_ms),
            master_runtime_uptime_ms: snapshot
                .master_runtime
                .map_or(0, |runtime| duration_millis(runtime.uptime)),
            master_reload_revision: snapshot
                .master_runtime
                .map_or(0, |runtime| runtime.reload_revision),
            fleet_mode: fleet_mode_code(snapshot.fleet_mode),
            workers_expected: snapshot.workers_expected,
            row_count,
        }
    }

    fn row(
        &self,
        row_index: u16,
        worker: &WorkerStatus,
    ) -> WireStatus {
        let mut flags = self.flags;
        if worker.process.is_some() {
            flags |= FLAG_PROCESS;
        }
        if worker.operation.is_some() {
            flags |= FLAG_OPERATION;
        }
        if worker.heartbeat_age.is_some() {
            flags |= FLAG_HEARTBEAT;
        }
        if worker.block_reason.is_some() {
            flags |= FLAG_BLOCK_REASON;
        }
        let (intent_kind, drain_reason, exit_disposition) =
            intent_codes(worker.desired);
        let (operation_kind, operation_from, operation_to) = worker
            .operation
            .map_or((0, 0, 0), |operation| operation_codes(operation.kind));
        let (operation_origin, operation_via_worker, operation_command) =
            worker
                .operation
                .map_or((0, 0, 0), |operation| origin_codes(operation.origin));
        if operation_command != 0 {
            flags |= FLAG_COMMAND;
        }
        WireStatus {
            captured_at_ms: self.captured_at_ms,
            max_age_ms: self.max_age_ms,
            build_revision: self.build_revision,
            flags,
            master_pid: self.master_pid,
            master_started_at: self.master_started_at,
            master_generation: self.master_generation,
            master_runtime_started_at_ms: self.master_runtime_started_at_ms,
            master_runtime_uptime_ms: self.master_runtime_uptime_ms,
            master_reload_revision: self.master_reload_revision,
            fleet_mode: self.fleet_mode,
            workers_expected: self.workers_expected,
            row_count: self.row_count,
            row_index,
            worker_id: worker.worker_id,
            ownership: worker.ownership.code(),
            pid: worker.process.map_or(0, ProcessIdentity::pid),
            process_started_at: worker
                .process
                .map_or(0, ProcessIdentity::started_at),
            uptime_ms: worker.uptime.map(duration_millis).unwrap_or(0),
            recovery_attempts: worker.recovery_attempts,
            incarnation: worker.incarnation,
            reload_revision: worker.reload_revision,
            phase: phase_code(worker.phase),
            phase_age_ms: duration_millis(worker.phase_age),
            intent_kind,
            drain_reason,
            exit_disposition,
            intent_age_ms: duration_millis(worker.desired_age),
            operation_id: worker
                .operation
                .map_or(0, |operation| operation.id.raw()),
            operation_kind,
            operation_from,
            operation_to,
            operation_origin,
            operation_via_worker,
            operation_command,
            operation_age_ms: worker
                .operation
                .map_or(0, |operation| duration_millis(operation.age)),
            heartbeat_age_ms: worker
                .heartbeat_age
                .map(duration_millis)
                .unwrap_or(0),
            block_reason: worker.block_reason.map_or(0, block_reason_code),
        }
    }

    /// The marker row of an empty snapshot: header only, every slot field zero.
    fn empty_marker(&self) -> WireStatus {
        WireStatus {
            captured_at_ms: self.captured_at_ms,
            max_age_ms: self.max_age_ms,
            build_revision: self.build_revision,
            flags: self.flags,
            master_pid: self.master_pid,
            master_started_at: self.master_started_at,
            master_generation: self.master_generation,
            master_runtime_started_at_ms: self.master_runtime_started_at_ms,
            master_runtime_uptime_ms: self.master_runtime_uptime_ms,
            master_reload_revision: self.master_reload_revision,
            fleet_mode: self.fleet_mode,
            workers_expected: self.workers_expected,
            row_count: 0,
            row_index: 0,
            worker_id: 0,
            ownership: 0,
            pid: 0,
            process_started_at: 0,
            uptime_ms: 0,
            recovery_attempts: 0,
            incarnation: 0,
            reload_revision: 0,
            phase: 0,
            phase_age_ms: 0,
            intent_kind: 0,
            drain_reason: 0,
            exit_disposition: 0,
            intent_age_ms: 0,
            operation_id: 0,
            operation_kind: 0,
            operation_from: 0,
            operation_to: 0,
            operation_origin: 0,
            operation_via_worker: 0,
            operation_command: 0,
            operation_age_ms: 0,
            heartbeat_age_ms: 0,
            block_reason: 0,
        }
    }
}

pub(crate) fn publish(
    fleet: &Fleet,
    snapshot: &WorkerListSnapshot,
    max_age: Duration,
) -> Result<(), String> {
    if snapshot.workers.len() > WORKER_SNAPSHOT_RING_CAPACITY {
        return Err(format!(
            "worker snapshot has {} rows, max {WORKER_SNAPSHOT_RING_CAPACITY}",
            snapshot.workers.len()
        ));
    }
    let row_count = u16::try_from(snapshot.workers.len()).map_err(|_| {
        "worker snapshot row count does not fit u16".to_string()
    })?;
    let header = WireHeader::of(snapshot, max_age, row_count);

    let payloads = if snapshot.workers.is_empty() {
        vec![Bytes::from(header.empty_marker().encode())]
    } else {
        snapshot
            .workers
            .iter()
            .enumerate()
            .map(|(index, worker)| {
                Bytes::from(header.row(index as u16, worker).encode())
            })
            .collect()
    };

    fleet.publish_batch::<WorkerSnapshotRecord>(
        WORKER_SNAPSHOT_FRAME_KIND,
        header.captured_at_ms,
        payloads,
    );
    Ok(())
}

pub(crate) fn collect(
    fleet: &Fleet,
    now: SystemTime,
) -> Result<WorkerListSnapshot, String> {
    let node = NodeId::new(0);
    let head = fleet.lane_head::<WorkerSnapshotRecord>(node);
    let last_counter = head
        .checked_sub(1)
        .ok_or_else(|| "no worker snapshot published".to_string())?;
    let last_frame = fleet
        .read_lane_at::<WorkerSnapshotRecord>(node, last_counter)
        .ok_or_else(|| "worker snapshot head is unavailable".to_string())?;
    if last_frame.kind != WORKER_SNAPSHOT_FRAME_KIND {
        return Err(
            "worker snapshot head has an unexpected frame kind".to_string()
        );
    }
    let last = WireStatus::decode(&last_frame.payload)?;
    let now_ms = unix_millis(now);
    if last_frame.ver != last.captured_at_ms {
        return Err("worker snapshot version does not match its capture time"
            .to_string());
    }
    if now_ms.saturating_sub(last.captured_at_ms) > last.max_age_ms {
        return Err(format!(
            "worker snapshot is stale: age={}ms, max={}ms",
            now_ms.saturating_sub(last.captured_at_ms),
            last.max_age_ms
        ));
    }
    if last.row_count == 0 {
        if last.row_index != 0 || last.worker_id != 0 {
            return Err("empty worker snapshot marker is invalid".to_string());
        }
    } else if last.row_index + 1 != last.row_count
        || usize::from(last.row_count) > WORKER_SNAPSHOT_RING_CAPACITY
    {
        return Err("worker snapshot tail is incomplete".to_string());
    }

    let physical_rows = usize::from(last.row_count.max(1));
    let first_counter = last_counter
        .checked_add(1)
        .and_then(|counter| counter.checked_sub(physical_rows as u64))
        .ok_or_else(|| "worker snapshot batch is incomplete".to_string())?;
    let mut rows = Vec::with_capacity(usize::from(last.row_count));
    for offset in 0..physical_rows {
        let frame = fleet
            .read_lane_at::<WorkerSnapshotRecord>(
                node,
                first_counter + offset as u64,
            )
            .ok_or_else(|| "worker snapshot batch is incomplete".to_string())?;
        if frame.kind != WORKER_SNAPSHOT_FRAME_KIND
            || frame.ver != last.captured_at_ms
        {
            return Err("worker snapshot batch is inconsistent".to_string());
        }
        let row = WireStatus::decode(&frame.payload)?;
        if !row.same_header_as(&last) || usize::from(row.row_index) != offset {
            return Err(
                "worker snapshot rows do not share one header".to_string()
            );
        }
        if row.row_count != 0 {
            rows.push(row.into_status()?);
        }
    }
    rows.sort_by_key(|worker| worker.worker_id);

    Ok(WorkerListSnapshot {
        captured_at: UNIX_EPOCH + Duration::from_millis(last.captured_at_ms),
        workers_expected: last.workers_expected,
        build: BuildIdentity::from_wire(
            last.build_revision,
            last.flags & FLAG_BUILD_DIRTY != 0,
        ),
        master: (last.flags & FLAG_MASTER != 0).then(|| {
            MasterIdentity::new(
                ProcessIdentity::new(last.master_pid, last.master_started_at),
                last.master_generation,
            )
        }),
        master_runtime: last.master_runtime(),
        fleet_mode: fleet_mode_from_code(last.fleet_mode)?,
        workers: rows,
    })
}

impl WireStatus {
    /// The header bits every row of one batch must agree on. Per-row flag bits
    /// are masked out of the comparison.
    fn same_header_as(
        &self,
        other: &Self,
    ) -> bool {
        const HEADER_FLAGS: u8 =
            FLAG_BUILD_DIRTY | FLAG_MASTER | FLAG_MASTER_RUNTIME;
        self.captured_at_ms == other.captured_at_ms
            && self.max_age_ms == other.max_age_ms
            && self.build_revision == other.build_revision
            && self.flags & HEADER_FLAGS == other.flags & HEADER_FLAGS
            && self.master_pid == other.master_pid
            && self.master_started_at == other.master_started_at
            && self.master_generation == other.master_generation
            && self.master_runtime_started_at_ms
                == other.master_runtime_started_at_ms
            && self.master_runtime_uptime_ms == other.master_runtime_uptime_ms
            && self.master_reload_revision == other.master_reload_revision
            && self.fleet_mode == other.fleet_mode
            && self.workers_expected == other.workers_expected
            && self.row_count == other.row_count
    }

    fn master_runtime(&self) -> Option<MasterRuntimeSnapshot> {
        (self.flags & FLAG_MASTER_RUNTIME != 0).then_some(
            MasterRuntimeSnapshot {
                started_at_unix_ms: self.master_runtime_started_at_ms,
                uptime: Duration::from_millis(self.master_runtime_uptime_ms),
                reload_revision: self.master_reload_revision,
            },
        )
    }

    fn into_status(self) -> Result<WorkerStatus, String> {
        let phase = phase_from_code(self.phase)?;
        let desired = intent_from_codes(
            self.intent_kind,
            self.drain_reason,
            self.exit_disposition,
        )?;
        let has_process = self.flags & FLAG_PROCESS != 0;
        if has_process && self.pid == 0 {
            return Err(
                "worker snapshot row claims a process with pid 0".to_string()
            );
        }
        let operation = if self.flags & FLAG_OPERATION != 0 {
            let command = if self.flags & FLAG_COMMAND != 0 {
                self.operation_command
            } else {
                0
            };
            Some(WorkerOperation {
                id: OperationId::from_raw(self.operation_id).ok_or_else(
                    || "worker snapshot row claims operation 0".to_string(),
                )?,
                kind: operation_from_codes(
                    self.operation_kind,
                    self.operation_from,
                    self.operation_to,
                )?,
                origin: origin_from_codes(
                    self.operation_origin,
                    self.operation_via_worker,
                    command,
                )?,
                age: Duration::from_millis(self.operation_age_ms),
            })
        } else {
            None
        };
        let block_reason = if self.flags & FLAG_BLOCK_REASON != 0 {
            Some(block_reason_from_code(self.block_reason)?)
        } else {
            None
        };
        Ok(WorkerStatus {
            worker_id: self.worker_id,
            ownership: WorkerOwnership::from_code(self.ownership)?,
            process: has_process.then(|| {
                ProcessIdentity::new(self.pid, self.process_started_at)
            }),
            uptime: has_process
                .then_some(Duration::from_millis(self.uptime_ms)),
            recovery_attempts: self.recovery_attempts,
            incarnation: self.incarnation,
            reload_revision: self.reload_revision,
            phase,
            phase_age: Duration::from_millis(self.phase_age_ms),
            desired,
            desired_age: Duration::from_millis(self.intent_age_ms),
            operation,
            heartbeat_age: (self.flags & FLAG_HEARTBEAT != 0)
                .then_some(Duration::from_millis(self.heartbeat_age_ms)),
            block_reason,
        })
    }

    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(WIRE_BYTES);
        bytes.extend_from_slice(&self.captured_at_ms.to_be_bytes());
        bytes.extend_from_slice(&self.max_age_ms.to_be_bytes());
        bytes.extend_from_slice(&self.build_revision.to_be_bytes());
        bytes.push(self.flags);
        bytes.extend_from_slice(&self.master_pid.to_be_bytes());
        bytes.extend_from_slice(&self.master_started_at.to_be_bytes());
        bytes.extend_from_slice(&self.master_generation.to_be_bytes());
        bytes.extend_from_slice(
            &self.master_runtime_started_at_ms.to_be_bytes(),
        );
        bytes.extend_from_slice(&self.master_runtime_uptime_ms.to_be_bytes());
        bytes.extend_from_slice(&self.master_reload_revision.to_be_bytes());
        bytes.push(self.fleet_mode);
        bytes.push(self.workers_expected);
        bytes.extend_from_slice(&self.row_count.to_be_bytes());
        bytes.extend_from_slice(&self.row_index.to_be_bytes());
        bytes.push(self.worker_id);
        bytes.push(self.ownership);
        bytes.extend_from_slice(&self.pid.to_be_bytes());
        bytes.extend_from_slice(&self.process_started_at.to_be_bytes());
        bytes.extend_from_slice(&self.uptime_ms.to_be_bytes());
        bytes.extend_from_slice(&self.recovery_attempts.to_be_bytes());
        bytes.extend_from_slice(&self.incarnation.to_be_bytes());
        bytes.extend_from_slice(&self.reload_revision.to_be_bytes());
        bytes.push(self.phase);
        bytes.extend_from_slice(&self.phase_age_ms.to_be_bytes());
        bytes.push(self.intent_kind);
        bytes.push(self.drain_reason);
        bytes.push(self.exit_disposition);
        bytes.extend_from_slice(&self.intent_age_ms.to_be_bytes());
        bytes.extend_from_slice(&self.operation_id.to_be_bytes());
        bytes.push(self.operation_kind);
        bytes.push(self.operation_from);
        bytes.push(self.operation_to);
        bytes.push(self.operation_origin);
        bytes.push(self.operation_via_worker);
        bytes.extend_from_slice(&self.operation_command.to_be_bytes());
        bytes.extend_from_slice(&self.operation_age_ms.to_be_bytes());
        bytes.extend_from_slice(&self.heartbeat_age_ms.to_be_bytes());
        bytes.push(self.block_reason);
        debug_assert_eq!(bytes.len(), WIRE_BYTES);
        bytes
    }

    fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() != WIRE_BYTES {
            return Err(format!(
                "invalid worker snapshot row length: expected {WIRE_BYTES}, got {}",
                bytes.len()
            ));
        }
        let mut offset = 0;
        fn take<const N: usize>(
            bytes: &[u8],
            offset: &mut usize,
        ) -> [u8; N] {
            let mut value = [0; N];
            value.copy_from_slice(&bytes[*offset..*offset + N]);
            *offset += N;
            value
        }
        fn byte(
            bytes: &[u8],
            offset: &mut usize,
        ) -> u8 {
            take::<1>(bytes, offset)[0]
        }
        Ok(Self {
            captured_at_ms: u64::from_be_bytes(take(bytes, &mut offset)),
            max_age_ms: u64::from_be_bytes(take(bytes, &mut offset)),
            build_revision: u32::from_be_bytes(take(bytes, &mut offset)),
            flags: byte(bytes, &mut offset),
            master_pid: u32::from_be_bytes(take(bytes, &mut offset)),
            master_started_at: u64::from_be_bytes(take(bytes, &mut offset)),
            master_generation: u64::from_be_bytes(take(bytes, &mut offset)),
            master_runtime_started_at_ms: u64::from_be_bytes(take(
                bytes,
                &mut offset,
            )),
            master_runtime_uptime_ms: u64::from_be_bytes(take(
                bytes,
                &mut offset,
            )),
            master_reload_revision: u64::from_be_bytes(take(
                bytes,
                &mut offset,
            )),
            fleet_mode: byte(bytes, &mut offset),
            workers_expected: byte(bytes, &mut offset),
            row_count: u16::from_be_bytes(take(bytes, &mut offset)),
            row_index: u16::from_be_bytes(take(bytes, &mut offset)),
            worker_id: byte(bytes, &mut offset),
            ownership: byte(bytes, &mut offset),
            pid: u32::from_be_bytes(take(bytes, &mut offset)),
            process_started_at: u64::from_be_bytes(take(bytes, &mut offset)),
            uptime_ms: u64::from_be_bytes(take(bytes, &mut offset)),
            recovery_attempts: u32::from_be_bytes(take(bytes, &mut offset)),
            incarnation: u64::from_be_bytes(take(bytes, &mut offset)),
            reload_revision: u64::from_be_bytes(take(bytes, &mut offset)),
            phase: byte(bytes, &mut offset),
            phase_age_ms: u64::from_be_bytes(take(bytes, &mut offset)),
            intent_kind: byte(bytes, &mut offset),
            drain_reason: byte(bytes, &mut offset),
            exit_disposition: byte(bytes, &mut offset),
            intent_age_ms: u64::from_be_bytes(take(bytes, &mut offset)),
            operation_id: u64::from_be_bytes(take(bytes, &mut offset)),
            operation_kind: byte(bytes, &mut offset),
            operation_from: byte(bytes, &mut offset),
            operation_to: byte(bytes, &mut offset),
            operation_origin: byte(bytes, &mut offset),
            operation_via_worker: byte(bytes, &mut offset),
            operation_command: u64::from_be_bytes(take(bytes, &mut offset)),
            operation_age_ms: u64::from_be_bytes(take(bytes, &mut offset)),
            heartbeat_age_ms: u64::from_be_bytes(take(bytes, &mut offset)),
            block_reason: byte(bytes, &mut offset),
        })
    }
}

pub(super) fn unix_millis(time: SystemTime) -> u64 {
    duration_millis(time.duration_since(UNIX_EPOCH).unwrap_or_default())
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
#[path = "../../tests/observation/snapshot.rs"]
mod tests;
