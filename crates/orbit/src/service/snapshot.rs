//! Complete per-process service observations carried through one Orbit ring.

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::placement::Dispatch;
use bytes::Bytes;
use claviron_runtime::{
    AppState, RunMode, ServiceSnapshot, ServiceStartPolicy, ServiceStatus,
};
use orbitive::{Fleet, NodeId, OrbitTyped, RingSpec};

// Keep this allocation distinct from Orbitive's lock state/event kinds (228/229).
// A future stable OrbitTypeId should replace the shared u8 allocation space.
// Kind 232 carries the retired controller-only shape. Never resize retained SHM.
const SERVICE_SNAPSHOT_RING_KIND: u8 = 191;
const SERVICE_SNAPSHOT_FRAME_KIND: u8 = 1;
const SERVICE_SNAPSHOT_RING_CAPACITY: usize = 512;
const SERVICE_SNAPSHOT_PAYLOAD_CAPACITY: usize = 512;
const SERVICE_SNAPSHOT_FIXED_BYTES: usize = 49;
const SERVICE_SNAPSHOT_MAX_NAME_BYTES: usize = 160;
const SERVICE_SNAPSHOT_MAX_ERROR_BYTES: usize = 150;

const FLAG_RELOADABLE: u8 = 1 << 0;
const FLAG_LAST_ERROR_PRESENT: u8 = 1 << 1;
const FLAG_LAST_ERROR_TRUNCATED: u8 = 1 << 2;
const FLAG_RELOAD_ERROR_PRESENT: u8 = 1 << 3;
const FLAG_RELOAD_ERROR_TRUNCATED: u8 = 1 << 4;

/// One complete observation assembled from fresh per-process batches.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetServiceListSnapshot {
    pub captured_at: SystemTime,
    pub members_expected: u16,
    pub members_reporting: u16,
    pub services: Vec<FleetServiceStatus>,
}

/// One fleet-wide observation narrowed to a named runnable service.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetServiceStatusSnapshot {
    pub captured_at: SystemTime,
    pub members_expected: u16,
    pub members_reporting: u16,
    pub service: FleetServiceStatus,
}

/// One logical runnable and the processes where it is registered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetServiceStatus {
    pub name: String,
    pub placements: Vec<FleetServicePlacement>,
}

/// Process-local Continuo state for one registered runnable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetServicePlacement {
    pub member: FleetMember,
    pub status: ServiceStatus,
    pub dispatch: Dispatch,
    pub active_executions: u64,
    pub start_policy: ServiceStartPolicy,
    pub reloadable: bool,
    pub generation: u64,
    pub reload_revision: u64,
    pub last_error: Option<String>,
    pub last_error_truncated: bool,
    pub last_reload_error: Option<String>,
    pub last_reload_error_truncated: bool,
    pub observed_at: SystemTime,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct FleetMember {
    pub node_id: u16,
    pub role: FleetMemberRole,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum FleetMemberRole {
    Master,
    Worker,
    Standalone,
}

#[derive(Clone)]
pub(super) struct ServiceSnapshotRecord;

impl OrbitTyped for ServiceSnapshotRecord {
    const KIND: u8 = SERVICE_SNAPSHOT_RING_KIND;
    const RING_SPEC: RingSpec = RingSpec::per_node(
        SERVICE_SNAPSHOT_RING_CAPACITY,
        SERVICE_SNAPSHOT_PAYLOAD_CAPACITY,
    );
}

struct WirePlacement {
    node_id: u16,
    captured_at: u64,
    role: FleetMemberRole,
    row_count: u16,
    row_index: u16,
    service: Option<String>,
    status: ServiceStatus,
    dispatch: Dispatch,
    active_executions: u64,
    start_policy: ServiceStartPolicy,
    reloadable: bool,
    generation: u64,
    reload_revision: u64,
    last_error: Option<String>,
    last_error_truncated: bool,
    last_reload_error: Option<String>,
    last_reload_error_truncated: bool,
}

pub(super) fn local(
    state: &AppState,
    snapshots: Vec<ServiceSnapshot>,
) -> FleetServiceListSnapshot {
    let captured_at = SystemTime::now();
    let member = FleetMember {
        node_id: state.fleet_node_id().raw(),
        role: match state.run_mode() {
            RunMode::Master => FleetMemberRole::Master,
            RunMode::Worker(_) => FleetMemberRole::Worker,
            RunMode::Standalone => FleetMemberRole::Standalone,
        },
    };
    let services = snapshots
        .into_iter()
        .map(|service| FleetServiceStatus {
            name: service.name().to_owned(),
            placements: vec![FleetServicePlacement {
                member,
                status: service.status(),
                dispatch: service.dispatch(),
                active_executions: service.active_executions(),
                start_policy: service.start_policy(),
                reloadable: service.is_reloadable(),
                generation: service.generation(),
                reload_revision: service.reload_revision(),
                last_error: service.last_error().map(ToOwned::to_owned),
                last_error_truncated: false,
                last_reload_error: service
                    .last_reload_error()
                    .map(ToOwned::to_owned),
                last_reload_error_truncated: false,
                observed_at: captured_at,
            }],
        })
        .collect();
    FleetServiceListSnapshot {
        captured_at,
        members_expected: 1,
        members_reporting: 1,
        services,
    }
}

/// Publish a complete member batch. The lane head becomes visible only after
/// all service rows commit, so absence from a fresh batch means that service is
/// not registered in that process rather than that its row was partially read.
pub(super) fn publish(
    fleet: &Fleet,
    snapshot: &FleetServiceListSnapshot,
) -> Result<(), String> {
    if snapshot.services.len() > SERVICE_SNAPSHOT_RING_CAPACITY {
        return Err(format!(
            "service snapshot has {} rows, max {SERVICE_SNAPSHOT_RING_CAPACITY}",
            snapshot.services.len()
        ));
    }
    let row_count = u16::try_from(snapshot.services.len()).map_err(|_| {
        "service snapshot row count does not fit u16".to_owned()
    })?;
    let captured_at = unix_millis(snapshot.captured_at);

    let payloads = if snapshot.services.is_empty() {
        vec![Bytes::from(
            WirePlacement {
                node_id: fleet.node_id().get(),
                captured_at,
                role: FleetMemberRole::Standalone,
                row_count: 0,
                row_index: 0,
                service: None,
                status: ServiceStatus::Stopped,
                dispatch: Dispatch::Each,
                active_executions: 0,
                start_policy: ServiceStartPolicy::Unavailable,
                reloadable: false,
                generation: 0,
                reload_revision: 0,
                last_error: None,
                last_error_truncated: false,
                last_reload_error: None,
                last_reload_error_truncated: false,
            }
            .encode()?,
        )]
    } else {
        snapshot
            .services
            .iter()
            .enumerate()
            .map(|(index, service)| {
                let placement =
                    service.placements.first().ok_or_else(|| {
                        format!(
                            "local service '{}' has no placement",
                            service.name
                        )
                    })?;
                if placement.member.node_id != fleet.node_id().get() {
                    return Err(format!(
                        "local service '{}' belongs to node {}, expected {}",
                        service.name,
                        placement.member.node_id,
                        fleet.node_id().get()
                    ));
                }
                let (last_error, last_error_truncated) = bounded(
                    placement.last_error.as_deref(),
                    SERVICE_SNAPSHOT_MAX_ERROR_BYTES,
                );
                let (last_reload_error, last_reload_error_truncated) = bounded(
                    placement.last_reload_error.as_deref(),
                    SERVICE_SNAPSHOT_MAX_ERROR_BYTES,
                );
                WirePlacement {
                    node_id: placement.member.node_id,
                    captured_at,
                    role: placement.member.role,
                    row_count,
                    row_index: index as u16,
                    service: Some(service.name.clone()),
                    status: placement.status,
                    dispatch: placement.dispatch,
                    active_executions: placement.active_executions,
                    start_policy: placement.start_policy,
                    reloadable: placement.reloadable,
                    generation: placement.generation,
                    reload_revision: placement.reload_revision,
                    last_error,
                    last_error_truncated,
                    last_reload_error,
                    last_reload_error_truncated,
                }
                .encode()
                .map(Bytes::from)
            })
            .collect::<Result<Vec<_>, _>>()?
    };

    fleet.publish_batch::<ServiceSnapshotRecord>(
        SERVICE_SNAPSHOT_FRAME_KIND,
        captured_at,
        payloads,
    );
    Ok(())
}

pub(super) fn collect(
    fleet: &Fleet,
    now: SystemTime,
    members: &[FleetMember],
    max_age: Duration,
) -> FleetServiceListSnapshot {
    let now_ms = unix_millis(now);
    let mut members_reporting = 0u16;
    let mut by_service = HashMap::<String, Vec<FleetServicePlacement>>::new();

    for member in members {
        let node = NodeId::new(member.node_id);
        let Some(rows) = read_latest_member(fleet, node, now_ms, max_age)
        else {
            continue;
        };
        if rows.iter().any(|row| row.role != member.role) {
            continue;
        }
        members_reporting = members_reporting.saturating_add(1);
        for row in rows {
            let Some(service) = row.service else {
                continue;
            };
            by_service.entry(service).or_default().push(
                FleetServicePlacement {
                    member: FleetMember {
                        node_id: row.node_id,
                        role: row.role,
                    },
                    status: row.status,
                    dispatch: row.dispatch,
                    active_executions: row.active_executions,
                    start_policy: row.start_policy,
                    reloadable: row.reloadable,
                    generation: row.generation,
                    reload_revision: row.reload_revision,
                    last_error: row.last_error,
                    last_error_truncated: row.last_error_truncated,
                    last_reload_error: row.last_reload_error,
                    last_reload_error_truncated: row
                        .last_reload_error_truncated,
                    observed_at: UNIX_EPOCH
                        + Duration::from_millis(row.captured_at),
                },
            );
        }
    }

    let mut services = by_service
        .into_iter()
        .map(|(name, mut placements)| {
            placements.sort_by_key(|placement| placement.member);
            FleetServiceStatus { name, placements }
        })
        .collect::<Vec<_>>();
    services.sort_by(|left, right| left.name.cmp(&right.name));

    FleetServiceListSnapshot {
        captured_at: now,
        members_expected: members.len() as u16,
        members_reporting,
        services,
    }
}

fn read_latest_member(
    fleet: &Fleet,
    node: NodeId,
    now: u64,
    max_age: Duration,
) -> Option<Vec<WirePlacement>> {
    let head = fleet.lane_head::<ServiceSnapshotRecord>(node);
    let last_counter = head.checked_sub(1)?;
    let last_frame =
        fleet.read_lane_at::<ServiceSnapshotRecord>(node, last_counter)?;
    if last_frame.kind != SERVICE_SNAPSHOT_FRAME_KIND {
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
        return (last.service.is_none() && last.row_index == 0).then(Vec::new);
    }
    if last.row_index + 1 != last.row_count {
        return None;
    }

    let count = u64::from(last.row_count);
    if count > SERVICE_SNAPSHOT_RING_CAPACITY as u64 || head < count {
        return None;
    }
    let start = head - count;
    let mut rows = Vec::with_capacity(count as usize);
    for index in 0..count {
        let frame =
            fleet.read_lane_at::<ServiceSnapshotRecord>(node, start + index)?;
        if frame.kind != SERVICE_SNAPSHOT_FRAME_KIND
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
            || row.service.is_none()
        {
            return None;
        }
        rows.push(row);
    }
    Some(rows)
}

impl WirePlacement {
    fn encode(&self) -> Result<Vec<u8>, String> {
        let service = self.service.as_deref().unwrap_or_default().as_bytes();
        if service.len() > SERVICE_SNAPSHOT_MAX_NAME_BYTES {
            return Err(format!(
                "service snapshot name is too long: {} bytes, max {SERVICE_SNAPSHOT_MAX_NAME_BYTES}",
                service.len()
            ));
        }
        let last_error =
            self.last_error.as_deref().unwrap_or_default().as_bytes();
        let reload_error =
            self.last_reload_error.as_deref().unwrap_or_default().as_bytes();
        let total = SERVICE_SNAPSHOT_FIXED_BYTES
            + service.len()
            + last_error.len()
            + reload_error.len();
        if total > SERVICE_SNAPSHOT_PAYLOAD_CAPACITY {
            return Err(format!(
                "service snapshot payload is too large: {total} bytes, max {SERVICE_SNAPSHOT_PAYLOAD_CAPACITY}"
            ));
        }

        let mut flags = 0u8;
        if self.reloadable {
            flags |= FLAG_RELOADABLE;
        }
        if self.last_error.is_some() {
            flags |= FLAG_LAST_ERROR_PRESENT;
        }
        if self.last_error_truncated {
            flags |= FLAG_LAST_ERROR_TRUNCATED;
        }
        if self.last_reload_error.is_some() {
            flags |= FLAG_RELOAD_ERROR_PRESENT;
        }
        if self.last_reload_error_truncated {
            flags |= FLAG_RELOAD_ERROR_TRUNCATED;
        }

        let mut out = Vec::with_capacity(total);
        out.extend_from_slice(&self.node_id.to_le_bytes());
        out.extend_from_slice(&self.captured_at.to_le_bytes());
        out.push(self.role.code());
        out.extend_from_slice(&self.row_count.to_le_bytes());
        out.extend_from_slice(&self.row_index.to_le_bytes());
        out.push(status_code(self.status));
        out.push(start_policy_code(self.start_policy));
        out.push(flags);
        out.extend_from_slice(&self.generation.to_le_bytes());
        out.extend_from_slice(&self.reload_revision.to_le_bytes());
        out.push(match self.dispatch {
            Dispatch::Each => 1,
            Dispatch::One => 2,
            Dispatch::Pool => 3,
        });
        out.extend_from_slice(&self.active_executions.to_le_bytes());
        out.extend_from_slice(&(service.len() as u16).to_le_bytes());
        out.extend_from_slice(&(last_error.len() as u16).to_le_bytes());
        out.extend_from_slice(&(reload_error.len() as u16).to_le_bytes());
        out.extend_from_slice(service);
        out.extend_from_slice(last_error);
        out.extend_from_slice(reload_error);
        Ok(out)
    }

    fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < SERVICE_SNAPSHOT_FIXED_BYTES {
            return Err(format!(
                "service snapshot payload is too short: {}",
                bytes.len()
            ));
        }
        let mut offset = 0;
        let node_id = read_u16(bytes, &mut offset)?;
        let captured_at = read_u64(bytes, &mut offset)?;
        let role = FleetMemberRole::from_code(read_u8(bytes, &mut offset)?)?;
        let row_count = read_u16(bytes, &mut offset)?;
        let row_index = read_u16(bytes, &mut offset)?;
        let status = status_from_code(read_u8(bytes, &mut offset)?)?;
        let start_policy =
            start_policy_from_code(read_u8(bytes, &mut offset)?)?;
        let flags = read_u8(bytes, &mut offset)?;
        let generation = read_u64(bytes, &mut offset)?;
        let reload_revision = read_u64(bytes, &mut offset)?;
        let dispatch = match read_u8(bytes, &mut offset)? {
            1 => Dispatch::Each,
            2 => Dispatch::One,
            3 => Dispatch::Pool,
            code => {
                return Err(format!(
                    "unknown service snapshot dispatch {code}"
                ));
            }
        };
        let active_executions = read_u64(bytes, &mut offset)?;
        let name_len = usize::from(read_u16(bytes, &mut offset)?);
        let last_error_len = usize::from(read_u16(bytes, &mut offset)?);
        let reload_error_len = usize::from(read_u16(bytes, &mut offset)?);
        if bytes.len() != offset + name_len + last_error_len + reload_error_len
        {
            return Err(
                "service snapshot string lengths are invalid".to_owned()
            );
        }
        let service = read_string(bytes, &mut offset, name_len, "name")?;
        let last_error =
            read_string(bytes, &mut offset, last_error_len, "last error")?;
        let last_reload_error = read_string(
            bytes,
            &mut offset,
            reload_error_len,
            "last reload error",
        )?;
        if service.is_none() && row_count != 0 {
            return Err("non-empty service snapshot row has no name".to_owned());
        }
        if last_error.is_some() != (flags & FLAG_LAST_ERROR_PRESENT != 0) {
            return Err("service snapshot last-error presence flag is invalid"
                .to_owned());
        }
        if last_reload_error.is_some()
            != (flags & FLAG_RELOAD_ERROR_PRESENT != 0)
        {
            return Err(
                "service snapshot reload-error presence flag is invalid"
                    .to_owned(),
            );
        }
        Ok(Self {
            node_id,
            captured_at,
            role,
            row_count,
            row_index,
            service,
            status,
            dispatch,
            active_executions,
            start_policy,
            reloadable: flags & FLAG_RELOADABLE != 0,
            generation,
            reload_revision,
            last_error,
            last_error_truncated: flags & FLAG_LAST_ERROR_TRUNCATED != 0,
            last_reload_error,
            last_reload_error_truncated: flags & FLAG_RELOAD_ERROR_TRUNCATED
                != 0,
        })
    }
}

fn bounded(
    value: Option<&str>,
    max_bytes: usize,
) -> (Option<String>, bool) {
    let Some(value) = value else {
        return (None, false);
    };
    if value.len() <= max_bytes {
        return (Some(value.to_owned()), false);
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    (Some(value[..end].to_owned()), true)
}

fn read_string(
    bytes: &[u8],
    offset: &mut usize,
    len: usize,
    field: &str,
) -> Result<Option<String>, String> {
    if len == 0 {
        return Ok(None);
    }
    let end = offset
        .checked_add(len)
        .ok_or_else(|| format!("service snapshot {field} length overflow"))?;
    let value = bytes
        .get(*offset..end)
        .ok_or_else(|| format!("truncated service snapshot {field}"))?;
    *offset = end;
    Ok(Some(
        std::str::from_utf8(value)
            .map_err(|error| {
                format!("service snapshot {field} is not UTF-8: {error}")
            })?
            .to_owned(),
    ))
}

impl FleetMemberRole {
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
            _ => Err(format!("unknown service snapshot member role {code}")),
        }
    }
}

const fn status_code(status: ServiceStatus) -> u8 {
    match status {
        ServiceStatus::Running => 1,
        ServiceStatus::Stopping => 2,
        ServiceStatus::Stopped => 3,
        ServiceStatus::Failed => 4,
    }
}

fn status_from_code(code: u8) -> Result<ServiceStatus, String> {
    match code {
        1 => Ok(ServiceStatus::Running),
        2 => Ok(ServiceStatus::Stopping),
        3 => Ok(ServiceStatus::Stopped),
        4 => Ok(ServiceStatus::Failed),
        _ => Err(format!("unknown service snapshot status {code}")),
    }
}

const fn start_policy_code(policy: ServiceStartPolicy) -> u8 {
    match policy {
        ServiceStartPolicy::Automatic => 1,
        ServiceStartPolicy::Manual => 2,
        ServiceStartPolicy::Unavailable => 3,
    }
}

fn start_policy_from_code(code: u8) -> Result<ServiceStartPolicy, String> {
    match code {
        1 => Ok(ServiceStartPolicy::Automatic),
        2 => Ok(ServiceStartPolicy::Manual),
        3 => Ok(ServiceStartPolicy::Unavailable),
        _ => Err(format!("unknown service snapshot start policy {code}")),
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
        .ok_or_else(|| "truncated service snapshot u8".to_owned())?;
    *offset += 1;
    Ok(value)
}

fn read_u16(
    bytes: &[u8],
    offset: &mut usize,
) -> Result<u16, String> {
    let end = offset
        .checked_add(2)
        .ok_or_else(|| "service snapshot u16 offset overflow".to_owned())?;
    let raw = bytes
        .get(*offset..end)
        .ok_or_else(|| "truncated service snapshot u16".to_owned())?;
    *offset = end;
    Ok(u16::from_le_bytes([raw[0], raw[1]]))
}

fn read_u64(
    bytes: &[u8],
    offset: &mut usize,
) -> Result<u64, String> {
    let end = offset
        .checked_add(8)
        .ok_or_else(|| "service snapshot u64 offset overflow".to_owned())?;
    let raw = bytes
        .get(*offset..end)
        .ok_or_else(|| "truncated service snapshot u64".to_owned())?;
    *offset = end;
    Ok(u64::from_le_bytes(raw.try_into().expect("u64 slice length")))
}

#[cfg(test)]
#[path = "../../tests/service/snapshot.rs"]
mod tests;
