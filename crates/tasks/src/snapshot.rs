//! Transport-neutral task status snapshots and their Orbit wire shape.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use claviron_helpers::sync::LockUnpoisoned;
use claviron_orbit::{Fleet, NodeId, OrbitTyped, RingSpec};

use crate::config::{TaskTable, TaskTiming};
use crate::registry::{TaskDispatch, TaskScope};

const TASK_SNAPSHOT_RING_KIND: u8 = 226;
const TASK_SNAPSHOT_FRAME_KIND: u8 = 2;
const TASK_SNAPSHOT_RING_CAPACITY: usize = 1_024;
const TASK_SNAPSHOT_PAYLOAD_CAPACITY: usize = 512;
const TASK_SNAPSHOT_FIXED_BYTES: usize = 44;
pub(crate) const TASK_SNAPSHOT_MAX_NAME_BYTES: usize =
    TASK_SNAPSHOT_PAYLOAD_CAPACITY - TASK_SNAPSHOT_FIXED_BYTES;

/// One fleet-wide observation returned by [`crate::TaskManager::list`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskListSnapshot {
    pub captured_at: SystemTime,
    pub members_expected: u16,
    pub members_reporting: u16,
    pub tasks: Vec<TaskStatus>,
}

/// One fleet-wide observation narrowed to a named logical task.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskStatusSnapshot {
    pub captured_at: SystemTime,
    pub members_expected: u16,
    pub members_reporting: u16,
    pub task: TaskStatus,
}

/// Config identity plus the freshest runtime placements for one logical task.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskStatus {
    pub name: String,
    pub kind: TaskKind,
    pub scope: TaskScope,
    pub dispatch: TaskDispatch,
    pub timing: TaskTimingSummary,
    pub concurrency: u8,
    pub placements: Vec<TaskPlacementStatus>,
}

/// The body implementation selected by the configured task.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskKind {
    Builtin,
    Command,
    Php,
    Js,
    Lua,
}

/// Operator-relevant timing information without exposing the whole config.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TaskTimingSummary {
    Manual { execution_timeout: Duration },
    Daemon { enabled: bool },
    Interval { enabled: bool, every: Duration, execution_timeout: Duration },
    Cron { enabled: bool, expression: String, execution_timeout: Duration },
}

impl From<&TaskTiming> for TaskTimingSummary {
    fn from(timing: &TaskTiming) -> Self {
        match timing {
            TaskTiming::Manual { execution_timeout } => {
                Self::Manual { execution_timeout: *execution_timeout }
            }
            TaskTiming::Daemon { enabled } => {
                Self::Daemon { enabled: *enabled }
            }
            TaskTiming::Interval {
                enabled, every, execution_timeout, ..
            } => Self::Interval {
                enabled: *enabled,
                every: *every,
                execution_timeout: *execution_timeout,
            },
            TaskTiming::Cron {
                enabled, expression, execution_timeout, ..
            } => Self::Cron {
                enabled: *enabled,
                expression: expression.clone(),
                execution_timeout: *execution_timeout,
            },
        }
    }
}

/// One fleet member's runtime view of a logical task.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskPlacementStatus {
    pub member: TaskMember,
    pub lifecycle: TaskLifecycle,
    /// Monotonic process-local automatic supervisor/controller generation.
    /// Explicit runs do not change this value.
    pub generation: u64,
    /// Attempts whose task body is executing now. Scheduler and backoff waits
    /// deliberately do not count as in-flight execution.
    pub in_flight: usize,
    /// Fleet-arbitrated slots this member currently holds.
    ///
    /// Separate from `in_flight` because owning a task and running it are
    /// different questions, and for a scheduled task they are different nearly
    /// all of the time: a cron `one` task holds its slot for the whole minute
    /// and executes for a fraction of a second, so `in_flight` is zero at every
    /// moment an operator is likely to look. Without this, such a task reads as
    /// if nobody had it.
    ///
    /// Zero for `each`, which arbitrates nothing and is therefore owned by
    /// every eligible member at once.
    pub owned_slots: usize,
    pub last_execution: Option<TaskExecutionOutcome>,
    pub observed_at: SystemTime,
}

/// Stable Orbit lane identity plus the process role occupying it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct TaskMember {
    pub node_id: u16,
    pub role: TaskMemberRole,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum TaskMemberRole {
    Master,
    Worker,
    Standalone,
}

/// Service ownership state, independent from the last body outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskLifecycle {
    Ineligible,
    Disabled,
    Inactive,
    Active,
    Stopping,
    Finished,
    MissingRunner,
}

/// Terminal result of the most recently completed task execution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskExecutionOutcome {
    pub result: TaskExecutionResult,
    pub finished_at: SystemTime,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskExecutionResult {
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
    GaveUp,
}

/// Generation-independent local execution state shared by replacement runners.
#[derive(Default)]
pub(crate) struct TaskRuntimeState {
    generation: AtomicU64,
    in_flight: AtomicUsize,
    owned_slots: AtomicUsize,
    last_execution: Mutex<Option<TaskExecutionOutcome>>,
}

impl TaskRuntimeState {
    pub(crate) fn begin_generation(&self) -> u64 {
        self.generation
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |generation| {
                Some(generation.saturating_add(1))
            })
            .expect("task generation update is infallible")
            .saturating_add(1)
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    pub(crate) fn begin_attempt(&self) {
        self.in_flight.fetch_add(1, Ordering::SeqCst);
    }

    pub(crate) fn finish_attempt(&self) {
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
    }

    pub(crate) fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::SeqCst)
    }

    /// Publish how many controller slots this runtime holds. Called by the
    /// pool controller on every reconciliation, so a lost lease shows up as
    /// promptly as an acquired one.
    pub(crate) fn set_owned_slots(
        &self,
        slots: usize,
    ) {
        self.owned_slots.store(slots, Ordering::SeqCst);
    }

    pub(crate) fn owned_slots(&self) -> usize {
        self.owned_slots.load(Ordering::SeqCst)
    }

    pub(crate) fn record(
        &self,
        result: TaskExecutionResult,
    ) {
        *self.last_execution.lock_unpoisoned() = Some(TaskExecutionOutcome {
            result,
            finished_at: SystemTime::now(),
        });
    }

    pub(crate) fn last_execution(&self) -> Option<TaskExecutionOutcome> {
        self.last_execution.lock_unpoisoned().clone()
    }
}

#[derive(Clone)]
pub(crate) struct TaskSnapshotRecord;

impl OrbitTyped for TaskSnapshotRecord {
    const KIND: u8 = TASK_SNAPSHOT_RING_KIND;
    const RING_SPEC: RingSpec = RingSpec::per_node(
        TASK_SNAPSHOT_RING_CAPACITY,
        TASK_SNAPSHOT_PAYLOAD_CAPACITY,
    );
}

struct WirePlacement {
    node_id: u16,
    captured_at: u64,
    role: TaskMemberRole,
    row_count: u16,
    row_index: u16,
    task: Option<String>,
    lifecycle: TaskLifecycle,
    generation: u64,
    in_flight: usize,
    owned_slots: usize,
    last_execution: Option<TaskExecutionOutcome>,
}

pub(crate) struct CollectedPlacements {
    pub(crate) members_reporting: u16,
    pub(crate) by_task: HashMap<String, Vec<TaskPlacementStatus>>,
}

pub(crate) fn validate_task_table(tasks: &TaskTable) -> Result<(), String> {
    if tasks.len() > TASK_SNAPSHOT_RING_CAPACITY {
        return Err(format!(
            "task table has {} rows, management snapshots support at most {TASK_SNAPSHOT_RING_CAPACITY}",
            tasks.len()
        ));
    }
    if let Some(name) =
        tasks.keys().find(|name| name.len() > TASK_SNAPSHOT_MAX_NAME_BYTES)
    {
        return Err(format!(
            "task name '{name}' is too long for management snapshots: {} bytes, max {TASK_SNAPSHOT_MAX_NAME_BYTES}",
            name.len()
        ));
    }
    Ok(())
}

/// Publish one complete member snapshot. `publish_batch` makes its lane head
/// visible only after every row has committed, so readers never see a partial
/// task list for that member.
pub(crate) fn publish(
    fleet: &Fleet,
    role: TaskMemberRole,
    snapshot: &TaskListSnapshot,
) -> Result<(), String> {
    if snapshot.tasks.len() > TASK_SNAPSHOT_RING_CAPACITY {
        return Err(format!(
            "task snapshot has {} rows, max {TASK_SNAPSHOT_RING_CAPACITY}",
            snapshot.tasks.len()
        ));
    }
    let row_count = u16::try_from(snapshot.tasks.len())
        .map_err(|_| "task snapshot row count does not fit u16".to_string())?;
    let captured_at = unix_millis(snapshot.captured_at);

    let payloads = if snapshot.tasks.is_empty() {
        vec![Bytes::from(
            WirePlacement {
                node_id: fleet.node_id().get(),
                captured_at,
                role,
                row_count: 0,
                row_index: 0,
                task: None,
                lifecycle: TaskLifecycle::Inactive,
                generation: 0,
                in_flight: 0,
                owned_slots: 0,
                last_execution: None,
            }
            .encode()?,
        )]
    } else {
        snapshot
            .tasks
            .iter()
            .enumerate()
            .map(|(index, task)| {
                let placement = task.placements.first().ok_or_else(|| {
                    format!("local task '{}' has no placement", task.name)
                })?;
                if placement.member.node_id != fleet.node_id().get() {
                    return Err(format!(
                        "local task '{}' belongs to node {}, expected {}",
                        task.name,
                        placement.member.node_id,
                        fleet.node_id().get()
                    ));
                }
                WirePlacement {
                    node_id: placement.member.node_id,
                    captured_at,
                    role: placement.member.role,
                    row_count,
                    row_index: index as u16,
                    task: Some(task.name.clone()),
                    lifecycle: placement.lifecycle,
                    generation: placement.generation,
                    in_flight: placement.in_flight,
                    owned_slots: placement.owned_slots,
                    last_execution: placement.last_execution.clone(),
                }
                .encode()
                .map(Bytes::from)
            })
            .collect::<Result<Vec<_>, _>>()?
    };

    fleet.publish_batch::<TaskSnapshotRecord>(
        TASK_SNAPSHOT_FRAME_KIND,
        captured_at,
        payloads,
    );
    Ok(())
}

pub(crate) fn collect(
    fleet: &Fleet,
    now: SystemTime,
    max_age: Duration,
    members: &[claviron_orbit::service::FleetMember],
) -> CollectedPlacements {
    let now = unix_millis(now);
    let mut members_reporting = 0u16;
    let mut by_task = HashMap::<String, Vec<TaskPlacementStatus>>::new();

    for member in members {
        let node = NodeId::new(member.node_id);
        let Some(rows) = read_latest_member(fleet, node, now, max_age) else {
            continue;
        };
        members_reporting = members_reporting.saturating_add(1);
        for row in rows {
            let Some(task) = row.task else {
                continue;
            };
            by_task.entry(task).or_default().push(TaskPlacementStatus {
                member: TaskMember { node_id: row.node_id, role: row.role },
                lifecycle: row.lifecycle,
                generation: row.generation,
                in_flight: row.in_flight,
                owned_slots: row.owned_slots,
                last_execution: row.last_execution,
                observed_at: UNIX_EPOCH
                    + Duration::from_millis(row.captured_at),
            });
        }
    }
    for placements in by_task.values_mut() {
        placements.sort_by_key(|placement| placement.member);
    }

    CollectedPlacements { members_reporting, by_task }
}

fn read_latest_member(
    fleet: &Fleet,
    node: NodeId,
    now: u64,
    max_age: Duration,
) -> Option<Vec<WirePlacement>> {
    let head = fleet.lane_head::<TaskSnapshotRecord>(node);
    let last_counter = head.checked_sub(1)?;
    let last_frame =
        fleet.read_lane_at::<TaskSnapshotRecord>(node, last_counter)?;
    if last_frame.kind != TASK_SNAPSHOT_FRAME_KIND {
        return None;
    }
    let last = WirePlacement::decode(&last_frame.payload).ok()?;
    let max_age_ms = max_age.as_millis().min(u128::from(u64::MAX)) as u64;
    if last_frame.ver != last.captured_at
        || last.node_id != node.get()
        || now.saturating_sub(last.captured_at) > max_age_ms
    {
        return None;
    }
    if last.row_count == 0 {
        return (last.task.is_none() && last.row_index == 0).then(Vec::new);
    }
    if last.row_index + 1 != last.row_count {
        return None;
    }

    let count = u64::from(last.row_count);
    if count > TASK_SNAPSHOT_RING_CAPACITY as u64 || head < count {
        return None;
    }
    let start = head - count;
    let mut rows = Vec::with_capacity(count as usize);
    for index in 0..count {
        let frame =
            fleet.read_lane_at::<TaskSnapshotRecord>(node, start + index)?;
        if frame.kind != TASK_SNAPSHOT_FRAME_KIND
            || frame.ver != last.captured_at
        {
            return None;
        }
        let row = WirePlacement::decode(&frame.payload).ok()?;
        if row.node_id != last.node_id
            || row.captured_at != last.captured_at
            || row.role != last.role
            || row.row_count != last.row_count
            || u64::from(row.row_index) != index
            || row.task.is_none()
        {
            return None;
        }
        rows.push(row);
    }
    Some(rows)
}

impl WirePlacement {
    fn encode(&self) -> Result<Vec<u8>, String> {
        let task = self.task.as_deref().unwrap_or_default().as_bytes();
        if task.len() > TASK_SNAPSHOT_MAX_NAME_BYTES {
            return Err(format!(
                "task snapshot name is too long: {} bytes, max {TASK_SNAPSHOT_MAX_NAME_BYTES}",
                task.len()
            ));
        }
        let mut out =
            Vec::with_capacity(TASK_SNAPSHOT_FIXED_BYTES + task.len());
        out.extend_from_slice(&self.node_id.to_le_bytes());
        out.extend_from_slice(&self.captured_at.to_le_bytes());
        out.push(self.role.code());
        out.extend_from_slice(&self.row_count.to_le_bytes());
        out.extend_from_slice(&self.row_index.to_le_bytes());
        out.push(self.lifecycle.code());
        out.extend_from_slice(&self.generation.to_le_bytes());
        out.extend_from_slice(&(self.in_flight as u64).to_le_bytes());
        out.push(u8::try_from(self.owned_slots).unwrap_or(u8::MAX));
        match &self.last_execution {
            Some(last) => {
                out.push(last.result.code());
                out.extend_from_slice(
                    &unix_millis(last.finished_at).to_le_bytes(),
                );
            }
            None => {
                out.push(0);
                out.extend_from_slice(&0u64.to_le_bytes());
            }
        }
        out.extend_from_slice(&(task.len() as u16).to_le_bytes());
        out.extend_from_slice(task);
        Ok(out)
    }

    fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < TASK_SNAPSHOT_FIXED_BYTES {
            return Err(format!(
                "task snapshot payload is too short: {}",
                bytes.len()
            ));
        }
        let mut offset = 0;
        let node_id = read_u16(bytes, &mut offset)?;
        let captured_at = read_u64(bytes, &mut offset)?;
        let role = TaskMemberRole::from_code(read_u8(bytes, &mut offset)?)?;
        let row_count = read_u16(bytes, &mut offset)?;
        let row_index = read_u16(bytes, &mut offset)?;
        let lifecycle = TaskLifecycle::from_code(read_u8(bytes, &mut offset)?)?;
        let generation = read_u64(bytes, &mut offset)?;
        let in_flight = usize::try_from(read_u64(bytes, &mut offset)?)
            .map_err(|_| {
                "task snapshot in-flight count does not fit usize".to_string()
            })?;
        let owned_slots = usize::from(read_u8(bytes, &mut offset)?);
        let result = read_u8(bytes, &mut offset)?;
        let finished_at = read_u64(bytes, &mut offset)?;
        let name_len = usize::from(read_u16(bytes, &mut offset)?);
        if bytes.len() != offset + name_len {
            return Err("task snapshot name length is invalid".to_string());
        }
        let task = if name_len == 0 {
            None
        } else {
            Some(
                std::str::from_utf8(&bytes[offset..])
                    .map_err(|error| {
                        format!("task snapshot name is not UTF-8: {error}")
                    })?
                    .to_owned(),
            )
        };
        let last_execution = match result {
            0 if finished_at == 0 => None,
            0 => {
                return Err(
                    "task snapshot outcome timestamp has no result".to_string()
                );
            }
            code => Some(TaskExecutionOutcome {
                result: TaskExecutionResult::from_code(code)?,
                finished_at: UNIX_EPOCH + Duration::from_millis(finished_at),
            }),
        };
        Ok(Self {
            node_id,
            captured_at,
            role,
            row_count,
            row_index,
            task,
            lifecycle,
            generation,
            in_flight,
            owned_slots,
            last_execution,
        })
    }
}

impl TaskMemberRole {
    const fn code(self) -> u8 {
        match self {
            Self::Master => 1,
            Self::Worker => 2,
            Self::Standalone => 3,
        }
    }

    fn from_code(code: u8) -> Result<Self, String> {
        match code {
            1 => Ok(Self::Master),
            2 => Ok(Self::Worker),
            3 => Ok(Self::Standalone),
            _ => Err(format!("unknown task snapshot member role {code}")),
        }
    }
}

impl TaskLifecycle {
    const fn code(self) -> u8 {
        match self {
            Self::Ineligible => 1,
            Self::Disabled => 2,
            Self::Inactive => 4,
            Self::Active => 5,
            Self::Stopping => 6,
            Self::Finished => 7,
            Self::MissingRunner => 8,
        }
    }

    fn from_code(code: u8) -> Result<Self, String> {
        match code {
            1 => Ok(Self::Ineligible),
            2 => Ok(Self::Disabled),
            4 => Ok(Self::Inactive),
            5 => Ok(Self::Active),
            6 => Ok(Self::Stopping),
            7 => Ok(Self::Finished),
            8 => Ok(Self::MissingRunner),
            _ => Err(format!("unknown task snapshot lifecycle {code}")),
        }
    }
}

impl TaskExecutionResult {
    const fn code(self) -> u8 {
        match self {
            Self::Succeeded => 1,
            Self::Failed => 2,
            Self::Cancelled => 3,
            Self::TimedOut => 4,
            Self::GaveUp => 5,
        }
    }

    fn from_code(code: u8) -> Result<Self, String> {
        match code {
            1 => Ok(Self::Succeeded),
            2 => Ok(Self::Failed),
            3 => Ok(Self::Cancelled),
            4 => Ok(Self::TimedOut),
            5 => Ok(Self::GaveUp),
            _ => Err(format!("unknown task snapshot execution result {code}")),
        }
    }
}

fn unix_millis(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn read_u8(
    bytes: &[u8],
    offset: &mut usize,
) -> Result<u8, String> {
    let value = *bytes
        .get(*offset)
        .ok_or_else(|| "truncated task snapshot u8".to_string())?;
    *offset += 1;
    Ok(value)
}

fn read_u16(
    bytes: &[u8],
    offset: &mut usize,
) -> Result<u16, String> {
    let end = *offset + 2;
    let value = u16::from_le_bytes(
        bytes
            .get(*offset..end)
            .ok_or_else(|| "truncated task snapshot u16".to_string())?
            .try_into()
            .expect("u16 slice"),
    );
    *offset = end;
    Ok(value)
}

fn read_u64(
    bytes: &[u8],
    offset: &mut usize,
) -> Result<u64, String> {
    let end = *offset + 8;
    let value = u64::from_le_bytes(
        bytes
            .get(*offset..end)
            .ok_or_else(|| "truncated task snapshot u64".to_string())?
            .try_into()
            .expect("u64 slice"),
    );
    *offset = end;
    Ok(value)
}

#[cfg(test)]
#[path = "../tests/snapshot.rs"]
mod tests;
