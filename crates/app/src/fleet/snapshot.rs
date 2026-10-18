//! Complete per-process application observations carried through Orbit.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use claviron_orbit::service::{FleetMember, FleetMemberRole};
use claviron_orbit::{Fleet, NodeId, OrbitTyped, RingSpec};
use claviron_runtime::{AppState, RunMode};

use crate::{
    ApplicationDescription, ApplicationId, ApplicationLifecycle,
    ApplicationName, ApplicationPoolStatus, ApplicationRuntime,
    ApplicationSnapshot,
};

// 233 is the retired PHP application snapshot allocation; 234 remains the
// PHP daemon snapshot transport. Keep the common application ring distinct.
// The descriptive row is incompatible with retired kind 235; retained SHM is never resized.
const APPLICATION_SNAPSHOT_RING_KIND: u8 = 192;
const APPLICATION_SNAPSHOT_FRAME_KIND: u8 = 1;
const APPLICATION_SNAPSHOT_RING_CAPACITY: usize = 512;
const APPLICATION_SNAPSHOT_PAYLOAD_CAPACITY: usize = 512;
const APPLICATION_SNAPSHOT_FIXED_BYTES: usize = 67;
// Version 2 adds boot-prepared descriptions to both fleet and Link reports.
const APPLICATION_SNAPSHOT_ROW_VERSION: u8 = 2;
const TRUNCATED_SUFFIX: &str = " [truncated]";

const FLAG_ERROR_PRESENT: u8 = 1 << 0;
const FLAG_ERROR_TRUNCATED: u8 = 1 << 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FleetApplicationListSnapshot {
    pub captured_at: SystemTime,
    pub members_expected: u16,
    pub members_reporting: u16,
    pub applications: Vec<FleetApplicationStatus>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FleetApplicationStatusSnapshot {
    pub captured_at: SystemTime,
    pub members_expected: u16,
    pub members_reporting: u16,
    pub application: FleetApplicationStatus,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FleetApplicationStatus {
    pub id: ApplicationId,
    pub placements: Vec<FleetApplicationPlacement>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FleetApplicationPlacement {
    pub member: FleetMember,
    pub description: ApplicationDescription,
    pub lifecycle: ApplicationLifecycle,
    pub enabled: bool,
    pub generation: u64,
    pub pool: Option<ApplicationPoolStatus>,
    pub last_error: Option<String>,
    pub last_error_truncated: bool,
    pub observed_at: SystemTime,
}

#[derive(Clone)]
pub(super) struct ApplicationSnapshotRecord;

impl OrbitTyped for ApplicationSnapshotRecord {
    const KIND: u8 = APPLICATION_SNAPSHOT_RING_KIND;
    const RING_SPEC: RingSpec = RingSpec::per_node(
        APPLICATION_SNAPSHOT_RING_CAPACITY,
        APPLICATION_SNAPSHOT_PAYLOAD_CAPACITY,
    );
}

#[derive(Clone)]
struct WirePlacement {
    node_id: u16,
    captured_at: u64,
    role: FleetMemberRole,
    row_count: u16,
    row_index: u16,
    id: Option<ApplicationId>,
    description: ApplicationDescription,
    lifecycle: ApplicationLifecycle,
    enabled: bool,
    generation: u64,
    pool: Option<ApplicationPoolStatus>,
    last_error: Option<String>,
    last_error_truncated: bool,
}

pub(super) fn local(
    state: &AppState,
    applications: Vec<ApplicationSnapshot>,
) -> FleetApplicationListSnapshot {
    let captured_at = SystemTime::now();
    let member = FleetMember {
        node_id: state.fleet_node_id().raw(),
        role: match state.run_mode() {
            RunMode::Master => FleetMemberRole::Master,
            RunMode::Worker(_) => FleetMemberRole::Worker,
            RunMode::Standalone => FleetMemberRole::Standalone,
        },
    };
    FleetApplicationListSnapshot {
        captured_at,
        members_expected: 1,
        members_reporting: 1,
        applications: applications
            .into_iter()
            .map(|application| FleetApplicationStatus {
                id: application.id,
                placements: vec![FleetApplicationPlacement {
                    member,
                    description: application.description,
                    lifecycle: application.lifecycle,
                    enabled: application.enabled,
                    generation: application.generation,
                    pool: application.pool,
                    last_error: application.last_error,
                    last_error_truncated: false,
                    observed_at: captured_at,
                }],
            })
            .collect(),
    }
}

pub(super) fn publish(
    fleet: &Fleet,
    snapshot: &FleetApplicationListSnapshot,
    member: FleetMember,
) -> Result<(), String> {
    if member.node_id != fleet.node_id().get() {
        return Err(format!(
            "local application snapshot belongs to node {}, expected {}",
            member.node_id,
            fleet.node_id().get()
        ));
    }
    let payloads = encode_local_rows(snapshot, member)?
        .into_iter()
        .map(Bytes::from)
        .collect();
    fleet.publish_batch::<ApplicationSnapshotRecord>(
        APPLICATION_SNAPSHOT_FRAME_KIND,
        unix_millis(snapshot.captured_at),
        payloads,
    );
    Ok(())
}

pub(super) fn encode_local_rows(
    snapshot: &FleetApplicationListSnapshot,
    member: FleetMember,
) -> Result<Vec<Vec<u8>>, String> {
    if snapshot.applications.len() > APPLICATION_SNAPSHOT_RING_CAPACITY {
        return Err(format!(
            "application snapshot has {} rows, max {APPLICATION_SNAPSHOT_RING_CAPACITY}",
            snapshot.applications.len()
        ));
    }
    let row_count =
        u16::try_from(snapshot.applications.len()).map_err(|_| {
            "application snapshot row count does not fit u16".to_owned()
        })?;
    let captured_at = unix_millis(snapshot.captured_at);
    let payloads = if snapshot.applications.is_empty() {
        vec![
            WirePlacement::empty(member.node_id, member.role, captured_at)
                .encode()?,
        ]
    } else {
        snapshot
            .applications
            .iter()
            .enumerate()
            .map(|(index, application)| {
                let placement =
                    application.placements.first().ok_or_else(|| {
                        format!(
                            "local application '{}' has no placement",
                            application.id
                        )
                    })?;
                if placement.member != member {
                    return Err(format!(
                        "local application '{}' belongs to {:?}, expected {:?}",
                        application.id, placement.member, member
                    ));
                }
                WirePlacement::from_status(
                    placement.member,
                    captured_at,
                    row_count,
                    index as u16,
                    application,
                )
                .encode()
            })
            .collect::<Result<Vec<_>, _>>()?
    };
    Ok(payloads)
}

pub(super) fn decode_link_rows(
    payloads: &[Vec<u8>]
) -> Result<FleetApplicationListSnapshot, String> {
    let Some(first) = payloads.first() else {
        return Err("linked application snapshot has no rows".to_owned());
    };
    let first = WirePlacement::decode(first)?;
    let count = usize::from(first.row_count);
    if count > APPLICATION_SNAPSHOT_RING_CAPACITY
        || payloads.len() != count.max(1)
    {
        return Err(
            "linked application snapshot row count is invalid".to_owned()
        );
    }
    let mut applications = Vec::with_capacity(count);
    for (index, payload) in payloads.iter().enumerate() {
        let row = WirePlacement::decode(payload)?;
        if row.node_id != first.node_id
            || row.role != first.role
            || row.captured_at != first.captured_at
            || row.row_count != first.row_count
            || usize::from(row.row_index) != index
        {
            return Err("linked application snapshot rows disagree".to_owned());
        }
        if count == 0 {
            if row.id.is_some() || index != 0 {
                return Err(
                    "linked empty application snapshot has an identity"
                        .to_owned(),
                );
            }
        } else {
            let id = row.id.clone().ok_or_else(|| {
                "linked application snapshot is missing an identity".to_owned()
            })?;
            applications.push(FleetApplicationStatus {
                id,
                placements: vec![row.into_placement()],
            });
        }
    }
    Ok(FleetApplicationListSnapshot {
        captured_at: UNIX_EPOCH + Duration::from_millis(first.captured_at),
        members_expected: 1,
        members_reporting: 1,
        applications,
    })
}

pub(super) fn collect(
    fleet: &Fleet,
    now: SystemTime,
    members: &[FleetMember],
    max_age: Duration,
) -> FleetApplicationListSnapshot {
    let mut reporting = 0u16;
    let mut applications =
        BTreeMap::<ApplicationId, Vec<FleetApplicationPlacement>>::new();
    for member in members {
        let Some(rows) = read_latest(fleet, *member, now, max_age) else {
            continue;
        };
        reporting = reporting.saturating_add(1);
        for row in rows {
            let Some(id) = row.id.clone() else { continue };
            applications.entry(id).or_default().push(row.into_placement());
        }
    }
    FleetApplicationListSnapshot {
        captured_at: now,
        members_expected: u16::try_from(members.len()).unwrap_or(u16::MAX),
        members_reporting: reporting,
        applications: applications
            .into_iter()
            .map(|(id, mut placements)| {
                placements.sort_by_key(|placement| placement.member);
                FleetApplicationStatus { id, placements }
            })
            .collect(),
    }
}

impl WirePlacement {
    fn empty(
        node_id: u16,
        role: FleetMemberRole,
        captured_at: u64,
    ) -> Self {
        Self {
            node_id,
            captured_at,
            role,
            row_count: 0,
            row_index: 0,
            id: None,
            description: ApplicationDescription::default(),
            lifecycle: ApplicationLifecycle::Stopped,
            enabled: false,
            generation: 0,
            pool: None,
            last_error: None,
            last_error_truncated: false,
        }
    }

    fn from_status(
        member: FleetMember,
        captured_at: u64,
        row_count: u16,
        row_index: u16,
        status: &FleetApplicationStatus,
    ) -> Self {
        let placement =
            status.placements.first().expect("validated local placement");
        Self {
            node_id: member.node_id,
            captured_at,
            role: member.role,
            row_count,
            row_index,
            id: Some(status.id.clone()),
            description: placement.description.clone(),
            lifecycle: placement.lifecycle,
            enabled: placement.enabled,
            generation: placement.generation,
            pool: placement.pool.clone(),
            last_error: placement.last_error.clone(),
            last_error_truncated: placement.last_error_truncated,
        }
    }

    fn encode(&self) -> Result<Vec<u8>, String> {
        let description = serde_json::to_vec(&self.description)
            .map_err(|error| format!("application description: {error}"))?;
        let runtime = self
            .id
            .as_ref()
            .map_or(&[][..], |id| id.runtime().as_str().as_bytes());
        let name = self
            .id
            .as_ref()
            .map_or(&[][..], |id| id.name().as_str().as_bytes());
        if runtime.len() + name.len() + description.len()
            > APPLICATION_SNAPSHOT_PAYLOAD_CAPACITY
                - APPLICATION_SNAPSHOT_FIXED_BYTES
        {
            return Err(
                "application snapshot identity/description is too long"
                    .to_owned(),
            );
        }
        let error_limit = APPLICATION_SNAPSHOT_PAYLOAD_CAPACITY
            - APPLICATION_SNAPSHOT_FIXED_BYTES
            - runtime.len()
            - name.len()
            - description.len();
        let (error, truncated) =
            bounded_error(self.last_error.as_deref(), error_limit);
        let mut flags = 0;
        if self.last_error.is_some() {
            flags |= FLAG_ERROR_PRESENT;
        }
        if self.last_error_truncated || truncated {
            flags |= FLAG_ERROR_TRUNCATED;
        }

        let mut out = Vec::with_capacity(
            APPLICATION_SNAPSHOT_FIXED_BYTES
                + runtime.len()
                + name.len()
                + description.len()
                + error.len(),
        );
        out.push(APPLICATION_SNAPSHOT_ROW_VERSION);
        out.extend_from_slice(&self.node_id.to_le_bytes());
        out.extend_from_slice(&self.captured_at.to_le_bytes());
        out.push(role_code(self.role));
        out.extend_from_slice(&self.row_count.to_le_bytes());
        out.extend_from_slice(&self.row_index.to_le_bytes());
        out.push(self.id.is_some().into());
        out.push(lifecycle_code(self.lifecycle));
        out.push(self.enabled.into());
        out.extend_from_slice(&self.generation.to_le_bytes());
        out.push(self.pool.is_some().into());
        encode_optional_bool(
            &mut out,
            self.pool.as_ref().and_then(|pool| pool.accepting),
        );
        out.extend_from_slice(
            &self.pool.as_ref().map_or(0, |pool| pool.instances).to_le_bytes(),
        );
        for value in self.pool.as_ref().map_or([None; 5], |pool| {
            [
                pool.ready,
                pool.busy,
                pool.retiring,
                pool.backing_off,
                pool.exhausted,
            ]
        }) {
            encode_optional_u32(&mut out, value);
        }
        out.push(flags);
        out.extend_from_slice(&(runtime.len() as u16).to_le_bytes());
        out.extend_from_slice(&(name.len() as u16).to_le_bytes());
        out.extend_from_slice(&(error.len() as u16).to_le_bytes());
        out.extend_from_slice(&(description.len() as u16).to_le_bytes());
        out.extend_from_slice(runtime);
        out.extend_from_slice(name);
        out.extend_from_slice(&error);
        out.extend_from_slice(&description);
        debug_assert_eq!(
            out.len(),
            APPLICATION_SNAPSHOT_FIXED_BYTES
                + runtime.len()
                + name.len()
                + description.len()
                + error.len()
        );
        Ok(out)
    }

    fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < APPLICATION_SNAPSHOT_FIXED_BYTES {
            return Err("application snapshot payload is too short".to_owned());
        }
        let mut offset = 0;
        if read_u8(bytes, &mut offset)? != APPLICATION_SNAPSHOT_ROW_VERSION {
            return Err(
                "unsupported application snapshot row version".to_owned()
            );
        }
        let node_id = read_u16(bytes, &mut offset)?;
        let captured_at = read_u64(bytes, &mut offset)?;
        let role = role_from_code(read_u8(bytes, &mut offset)?)?;
        let row_count = read_u16(bytes, &mut offset)?;
        let row_index = read_u16(bytes, &mut offset)?;
        let has_id = read_bool(bytes, &mut offset)?;
        let lifecycle = lifecycle_from_code(read_u8(bytes, &mut offset)?)?;
        let enabled = read_bool(bytes, &mut offset)?;
        let generation = read_u64(bytes, &mut offset)?;
        let has_pool = read_bool(bytes, &mut offset)?;
        let accepting = read_optional_bool(bytes, &mut offset)?;
        let instances = read_u32(bytes, &mut offset)?;
        let ready = read_optional_u32(bytes, &mut offset)?;
        let busy = read_optional_u32(bytes, &mut offset)?;
        let retiring = read_optional_u32(bytes, &mut offset)?;
        let backing_off = read_optional_u32(bytes, &mut offset)?;
        let exhausted = read_optional_u32(bytes, &mut offset)?;
        let flags = read_u8(bytes, &mut offset)?;
        if flags & !(FLAG_ERROR_PRESENT | FLAG_ERROR_TRUNCATED) != 0 {
            return Err("application snapshot has unknown flags".to_owned());
        }
        let runtime_len = usize::from(read_u16(bytes, &mut offset)?);
        let name_len = usize::from(read_u16(bytes, &mut offset)?);
        let error_len = usize::from(read_u16(bytes, &mut offset)?);
        let description_len = usize::from(read_u16(bytes, &mut offset)?);
        if bytes.len()
            != offset + runtime_len + name_len + error_len + description_len
        {
            return Err(
                "application snapshot string lengths are invalid".to_owned()
            );
        }
        let runtime = read_string(bytes, &mut offset, runtime_len)?;
        let name = read_string(bytes, &mut offset, name_len)?;
        let error = read_string(bytes, &mut offset, error_len)?;
        let description =
            serde_json::from_slice::<ApplicationDescription>(&bytes[offset..])
                .map_err(|error| {
                    format!("invalid application description: {error}")
                })?;
        let id = if has_id {
            Some(ApplicationId::new(
                ApplicationRuntime::new(runtime)
                    .map_err(|error| error.to_string())?,
                ApplicationName::new(name)
                    .map_err(|error| error.to_string())?,
            ))
        } else if runtime.is_empty() && name.is_empty() {
            None
        } else {
            return Err(
                "empty application snapshot marker has an identity".to_owned()
            );
        };
        let pool = has_pool.then_some(ApplicationPoolStatus {
            accepting,
            instances,
            ready,
            busy,
            retiring,
            backing_off,
            exhausted,
        });
        if !has_pool
            && (accepting.is_some()
                || instances != 0
                || [ready, busy, retiring, backing_off, exhausted]
                    .into_iter()
                    .any(|value| value.is_some()))
        {
            return Err(
                "application snapshot has counters without a pool".to_owned()
            );
        }
        let error_present = flags & FLAG_ERROR_PRESENT != 0;
        if !error_present && !error.is_empty() {
            return Err(
                "application snapshot error flag is inconsistent".to_owned()
            );
        }
        Ok(Self {
            node_id,
            captured_at,
            role,
            row_count,
            row_index,
            id,
            description,
            lifecycle,
            enabled,
            generation,
            pool,
            last_error: error_present.then_some(error),
            last_error_truncated: flags & FLAG_ERROR_TRUNCATED != 0,
        })
    }

    fn into_placement(self) -> FleetApplicationPlacement {
        FleetApplicationPlacement {
            member: FleetMember { node_id: self.node_id, role: self.role },
            description: self.description,
            lifecycle: self.lifecycle,
            enabled: self.enabled,
            generation: self.generation,
            pool: self.pool,
            last_error: self.last_error,
            last_error_truncated: self.last_error_truncated,
            observed_at: UNIX_EPOCH + Duration::from_millis(self.captured_at),
        }
    }
}

fn read_latest(
    fleet: &Fleet,
    member: FleetMember,
    now: SystemTime,
    max_age: Duration,
) -> Option<Vec<WirePlacement>> {
    let node = NodeId::new(member.node_id);
    let head = fleet.lane_head::<ApplicationSnapshotRecord>(node);
    let last = fleet.read_lane_at::<ApplicationSnapshotRecord>(
        node,
        head.checked_sub(1)?,
    )?;
    if last.kind != APPLICATION_SNAPSHOT_FRAME_KIND {
        return None;
    }
    let tail = WirePlacement::decode(&last.payload).ok()?;
    let now = unix_millis(now);
    let max_age = max_age.as_millis().min(u128::from(u64::MAX)) as u64;
    if last.ver != tail.captured_at
        || tail.node_id != member.node_id
        || tail.role != member.role
        || now.saturating_sub(tail.captured_at) > max_age
    {
        return None;
    }
    if tail.row_count == 0 {
        return (tail.id.is_none() && tail.row_index == 0).then(Vec::new);
    }
    if tail.row_index + 1 != tail.row_count
        || usize::from(tail.row_count) > APPLICATION_SNAPSHOT_RING_CAPACITY
        || head < u64::from(tail.row_count)
    {
        return None;
    }
    let count = u64::from(tail.row_count);
    let mut rows = Vec::with_capacity(count as usize);
    for index in 0..count {
        let frame = fleet.read_lane_at::<ApplicationSnapshotRecord>(
            node,
            head - count + index,
        )?;
        let row = WirePlacement::decode(&frame.payload).ok()?;
        if frame.kind != APPLICATION_SNAPSHOT_FRAME_KIND
            || frame.ver != tail.captured_at
            || row.node_id != tail.node_id
            || row.role != tail.role
            || row.captured_at != tail.captured_at
            || row.row_count != tail.row_count
            || u64::from(row.row_index) != index
            || row.id.is_none()
        {
            return None;
        }
        rows.push(row);
    }
    Some(rows)
}

fn bounded_error(
    value: Option<&str>,
    max: usize,
) -> (Vec<u8>, bool) {
    let Some(value) = value else { return (Vec::new(), false) };
    if value.len() <= max {
        return (value.as_bytes().to_vec(), false);
    }
    if max <= TRUNCATED_SUFFIX.len() {
        return (TRUNCATED_SUFFIX.as_bytes()[..max].to_vec(), true);
    }
    let mut end = max - TRUNCATED_SUFFIX.len();
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    (format!("{}{}", &value[..end], TRUNCATED_SUFFIX).into_bytes(), true)
}

fn role_code(role: FleetMemberRole) -> u8 {
    match role {
        FleetMemberRole::Master => 1,
        FleetMemberRole::Worker => 2,
        FleetMemberRole::Standalone => 3,
    }
}

fn role_from_code(code: u8) -> Result<FleetMemberRole, String> {
    match code {
        1 => Ok(FleetMemberRole::Master),
        2 => Ok(FleetMemberRole::Worker),
        3 => Ok(FleetMemberRole::Standalone),
        _ => Err(format!("unknown application snapshot member role {code}")),
    }
}

fn lifecycle_code(lifecycle: ApplicationLifecycle) -> u8 {
    match lifecycle {
        ApplicationLifecycle::Starting => 1,
        ApplicationLifecycle::Running => 2,
        ApplicationLifecycle::Stopping => 3,
        ApplicationLifecycle::Stopped => 4,
        ApplicationLifecycle::Failed => 5,
    }
}

fn lifecycle_from_code(code: u8) -> Result<ApplicationLifecycle, String> {
    match code {
        1 => Ok(ApplicationLifecycle::Starting),
        2 => Ok(ApplicationLifecycle::Running),
        3 => Ok(ApplicationLifecycle::Stopping),
        4 => Ok(ApplicationLifecycle::Stopped),
        5 => Ok(ApplicationLifecycle::Failed),
        _ => Err(format!("unknown application snapshot lifecycle {code}")),
    }
}

fn encode_optional_bool(
    out: &mut Vec<u8>,
    value: Option<bool>,
) {
    out.push(match value {
        None => 0,
        Some(false) => 1,
        Some(true) => 2,
    });
}

fn read_optional_bool(
    bytes: &[u8],
    offset: &mut usize,
) -> Result<Option<bool>, String> {
    match read_u8(bytes, offset)? {
        0 => Ok(None),
        1 => Ok(Some(false)),
        2 => Ok(Some(true)),
        value => Err(format!(
            "invalid optional application snapshot boolean {value}"
        )),
    }
}

fn encode_optional_u32(
    out: &mut Vec<u8>,
    value: Option<u32>,
) {
    out.push(value.is_some().into());
    out.extend_from_slice(&value.unwrap_or_default().to_le_bytes());
}

fn read_optional_u32(
    bytes: &[u8],
    offset: &mut usize,
) -> Result<Option<u32>, String> {
    let present = read_bool(bytes, offset)?;
    let value = read_u32(bytes, offset)?;
    if !present && value != 0 {
        return Err(
            "absent application snapshot counter is non-zero".to_owned()
        );
    }
    Ok(present.then_some(value))
}

fn unix_millis(value: SystemTime) -> u64 {
    value
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn read_string(
    bytes: &[u8],
    offset: &mut usize,
    len: usize,
) -> Result<String, String> {
    let end = offset
        .checked_add(len)
        .ok_or("application snapshot offset overflow")?;
    let value =
        bytes.get(*offset..end).ok_or("application snapshot is truncated")?;
    *offset = end;
    std::str::from_utf8(value).map(ToOwned::to_owned).map_err(|error| {
        format!("application snapshot string is not UTF-8: {error}")
    })
}

fn read_bool(
    bytes: &[u8],
    offset: &mut usize,
) -> Result<bool, String> {
    match read_u8(bytes, offset)? {
        0 => Ok(false),
        1 => Ok(true),
        value => Err(format!("invalid application snapshot boolean {value}")),
    }
}

fn read_u8(
    bytes: &[u8],
    offset: &mut usize,
) -> Result<u8, String> {
    let value =
        *bytes.get(*offset).ok_or("application snapshot is truncated")?;
    *offset += 1;
    Ok(value)
}

fn read_u16(
    bytes: &[u8],
    offset: &mut usize,
) -> Result<u16, String> {
    let end =
        offset.checked_add(2).ok_or("application snapshot offset overflow")?;
    let value =
        bytes.get(*offset..end).ok_or("application snapshot is truncated")?;
    *offset = end;
    Ok(u16::from_le_bytes(value.try_into().unwrap()))
}

fn read_u32(
    bytes: &[u8],
    offset: &mut usize,
) -> Result<u32, String> {
    let end =
        offset.checked_add(4).ok_or("application snapshot offset overflow")?;
    let value =
        bytes.get(*offset..end).ok_or("application snapshot is truncated")?;
    *offset = end;
    Ok(u32::from_le_bytes(value.try_into().unwrap()))
}

fn read_u64(
    bytes: &[u8],
    offset: &mut usize,
) -> Result<u64, String> {
    let end =
        offset.checked_add(8).ok_or("application snapshot offset overflow")?;
    let value =
        bytes.get(*offset..end).ok_or("application snapshot is truncated")?;
    *offset = end;
    Ok(u64::from_le_bytes(value.try_into().unwrap()))
}

#[cfg(test)]
#[path = "../../tests/fleet/snapshot.rs"]
mod tests;
