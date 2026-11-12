//! Link geometry is frozen at startup; receiver admission is a live policy.

use std::sync::{Arc, Mutex, Weak};

use claviron_runtime::registry;
use serde::{Deserialize, Serialize};

use crate::link::application::{
    Access, ApplicationSegment, Joined, LINK_BUFFER_BYTES, LINK_FLEET_CAPACITY,
    LINK_INBOX_CAPACITY, LINK_INBOX_PAYLOAD_CAPACITY, LINK_LANE_CAPACITY,
    LINK_SPEC,
};

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(from = "LinkFields")]
pub struct LinkConfig {
    /// The exchanges that run at once. An engine may declare its own bound.
    pub max_in_flight: Option<usize>,
    /// The exchanges that can wait at the receiver. Zero disables the queue.
    pub max_queued: Option<usize>,
    pub geometry: LinkGeometry,
    /// Opt into SHM-backed streaming HTTP responses for cooperating callers.
    pub shared_response: bool,
    /// The owner's exact count or growth bounds. Absent leaves process counts
    /// to the external owner and publishes no growth policy.
    pub workers: Option<Workers>,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct LinkFields {
    max_in_flight: Option<usize>,
    max_queued: Option<usize>,
    geometry: LinkGeometry,
    shared_response: bool,
    workers: Option<Workers>,
    #[serde(rename = "max_waiting", deserialize_with = "reject_max_waiting")]
    _retired: (),
}

fn reject_max_waiting<'de, D: serde::Deserializer<'de>>(
    _: D
) -> Result<(), D::Error> {
    Err(serde::de::Error::custom(
        "orbit.link.max_waiting was removed; max_queued counts only waiting exchanges",
    ))
}

impl From<LinkFields> for LinkConfig {
    fn from(fields: LinkFields) -> Self {
        Self {
            max_in_flight: fields.max_in_flight,
            max_queued: fields.max_queued,
            geometry: fields.geometry,
            shared_response: fields.shared_response,
            workers: fields.workers,
        }
    }
}

/// The worker owner declares either an exact count (`static`) or a growth
/// policy (`dynamic`, PHP-FPM's word; `on_pressure` is still accepted).
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(try_from = "WorkerConfig", into = "WorkerConfig")]
pub enum Workers {
    Static { count: u16 },
    Dynamic(GrowthPolicy),
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GrowthPolicy {
    pub min: u16,
    pub max: WorkerMax,
    pub step: u16,
    pub retire_idle: RetireIdle,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(tag = "policy", rename_all = "snake_case", deny_unknown_fields)]
enum WorkerConfig {
    Static {
        count: u16,
        #[serde(default)]
        retire_idle: StaticRetirement,
    },
    #[serde(alias = "on_pressure")]
    Dynamic(GrowthPolicy),
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum StaticRetirement {
    #[default]
    Off,
}

impl TryFrom<WorkerConfig> for Workers {
    type Error = String;
    fn try_from(value: WorkerConfig) -> Result<Self, Self::Error> {
        let workers = match value {
            WorkerConfig::Static { count, .. } => Self::Static { count },
            WorkerConfig::Dynamic(policy) => Self::Dynamic(policy),
        };
        workers.validate().map_err(|error| error.to_string())?;
        Ok(workers)
    }
}

impl From<Workers> for WorkerConfig {
    fn from(value: Workers) -> Self {
        match value {
            Workers::Static { count } => {
                Self::Static { count, retire_idle: StaticRetirement::Off }
            }
            Workers::Dynamic(policy) => Self::Dynamic(policy),
        }
    }
}

/// The upper bound of a growth policy, written as a number or by name.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum WorkerMax {
    Count(u16),
    Named(WorkerMaxName),
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkerMaxName {
    /// The supervisor's worker capacity: the compiled fleet capacity less the
    /// master. Not a CPU count. Resolved by the master at boot.
    Available,
}

impl WorkerMax {
    /// The bound as a number, given the supervisor's worker capacity.
    pub fn resolve(
        self,
        worker_capacity: u16,
    ) -> u16 {
        match self {
            Self::Count(count) => count,
            Self::Named(WorkerMaxName::Available) => worker_capacity,
        }
    }
}

/// Idle retirement is off unless the operator declares a positive duration.
#[derive(
    Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq,
)]
#[serde(from = "RetireConfig", into = "RetireConfig")]
pub enum RetireIdle {
    #[default]
    Off,
    After {
        #[serde(with = "humantime_serde")]
        idle_timeout: std::time::Duration,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(untagged)]
enum RetireConfig {
    Off(StaticRetirement),
    After(IdleTimeout),
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct IdleTimeout {
    #[serde(with = "humantime_serde")]
    idle_timeout: std::time::Duration,
}
impl From<RetireConfig> for RetireIdle {
    fn from(value: RetireConfig) -> Self {
        match value {
            RetireConfig::Off(_) => Self::Off,
            RetireConfig::After(value) => {
                Self::After { idle_timeout: value.idle_timeout }
            }
        }
    }
}
impl From<RetireIdle> for RetireConfig {
    fn from(value: RetireIdle) -> Self {
        match value {
            RetireIdle::Off => Self::Off(StaticRetirement::Off),
            RetireIdle::After { idle_timeout } => {
                Self::After(IdleTimeout { idle_timeout })
            }
        }
    }
}

impl RetireIdle {
    pub fn timeout(self) -> Option<std::time::Duration> {
        match self {
            Self::Off => None,
            Self::After { idle_timeout } => Some(idle_timeout),
        }
    }
}

impl Workers {
    pub fn initial_count(self) -> u16 {
        match self {
            Self::Static { count } => count,
            Self::Dynamic(policy) => policy.min,
        }
    }
    pub fn maximum(
        self,
        capacity: u16,
    ) -> u16 {
        match self {
            Self::Static { count } => count,
            Self::Dynamic(policy) => policy.max.resolve(capacity),
        }
    }
    /// The policy as the log states it: `static N` or `dynamic min..=max`,
    /// the maximum resolved against `capacity` as [`Self::maximum`] does.
    pub fn describe(
        self,
        capacity: u16,
    ) -> WorkerPolicy {
        WorkerPolicy { workers: self, capacity }
    }

    pub fn retire_idle(self) -> RetireIdle {
        match self {
            Self::Static { .. } => RetireIdle::Off,
            Self::Dynamic(policy) => policy.retire_idle,
        }
    }
    pub fn validate(&self) -> registry::Result<()> {
        if self.initial_count() == 0 {
            return Err(registry::Error::msg(
                "workers count/min must be positive",
            ));
        }
        if let Self::Dynamic(policy) = self {
            if matches!(policy.max, WorkerMax::Count(max) if max < policy.min) {
                return Err(registry::Error::msg("workers need min <= max"));
            }
            if policy.step != 1 {
                return Err(registry::Error::msg(
                    "workers.step must be 1: one worker per pressure episode",
                ));
            }
            if policy
                .retire_idle
                .timeout()
                .is_some_and(|timeout| timeout.is_zero())
            {
                return Err(registry::Error::msg(
                    "retire_idle.idle_timeout must be positive",
                ));
            }
        }
        Ok(())
    }
    pub fn for_owner(
        &self,
        worker_capacity: u16,
        fleet_capacity: usize,
        count: u16,
    ) -> registry::Result<Option<orbitive::link::GrowthPolicy>> {
        self.validate()?;
        let max = self.maximum(worker_capacity);
        let control_lanes =
            1 + usize::from(self.retire_idle().timeout().is_some());
        if max > worker_capacity
            || usize::from(max) + control_lanes > fleet_capacity
        {
            return Err(registry::Error::msg(
                "workers exceed owner/fleet capacity, including caller and retirement lanes",
            ));
        }
        if count < self.initial_count() || count > max {
            return Err(registry::Error::msg(
                "owner worker count is outside the declared worker range",
            ));
        }
        Ok(self.growth(worker_capacity))
    }
    pub fn growth(
        &self,
        worker_capacity: u16,
    ) -> Option<orbitive::link::GrowthPolicy> {
        match self {
            Self::Static { .. } => None,
            Self::Dynamic(policy) => Some(orbitive::link::GrowthPolicy {
                min: policy.min,
                max: policy.max.resolve(worker_capacity),
                step: policy.step,
            }),
        }
    }
}

/// A [`Workers`] policy written for the log, without allocating.
#[derive(Clone, Copy, Debug)]
pub struct WorkerPolicy {
    workers: Workers,
    capacity: u16,
}

impl std::fmt::Display for WorkerPolicy {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        match self.workers {
            Workers::Static { count } => write!(f, "static {count}"),
            Workers::Dynamic(policy) => write!(
                f,
                "dynamic {}..={}",
                policy.min,
                policy.max.resolve(self.capacity)
            ),
        }
    }
}

/// Actual physical shape, bounded by the binary's compiled profile.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct LinkGeometry {
    pub fleet_capacity: usize,
    pub inbox_capacity: usize,
    pub inbox_payload_capacity: usize,
    pub lane_capacity: usize,
    pub buffer_bytes: usize,
}

impl Default for LinkGeometry {
    fn default() -> Self {
        Self {
            fleet_capacity: LINK_FLEET_CAPACITY,
            inbox_capacity: LINK_INBOX_CAPACITY,
            inbox_payload_capacity: LINK_INBOX_PAYLOAD_CAPACITY,
            lane_capacity: LINK_LANE_CAPACITY,
            buffer_bytes: LINK_BUFFER_BYTES,
        }
    }
}

/// The shape as the log states it, every field by its configuration name.
impl std::fmt::Display for LinkGeometry {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(
            f,
            "fleet_capacity {}, inbox_capacity {}, inbox_payload_capacity {}, lane_capacity {}, buffer_bytes {}",
            self.fleet_capacity,
            self.inbox_capacity,
            self.inbox_payload_capacity,
            self.lane_capacity,
            self.buffer_bytes
        )
    }
}

impl LinkGeometry {
    /// The shape an existing fleet was created with, as its owner declared it.
    pub fn of(spec: orbitive::link::LinkSpec) -> Self {
        Self {
            fleet_capacity: usize::from(spec.fleet_capacity),
            inbox_capacity: spec.inbox.capacity,
            inbox_payload_capacity: spec.inbox.payload_capacity,
            lane_capacity: spec.streams.lane_capacity,
            buffer_bytes: spec.streams.buffer_bytes,
        }
    }

    pub fn validate(self) -> registry::Result<()> {
        for (name, value, ceiling) in [
            ("fleet_capacity", self.fleet_capacity, LINK_FLEET_CAPACITY),
            ("inbox_capacity", self.inbox_capacity, LINK_INBOX_CAPACITY),
            (
                "inbox_payload_capacity",
                self.inbox_payload_capacity,
                LINK_INBOX_PAYLOAD_CAPACITY,
            ),
            ("lane_capacity", self.lane_capacity, LINK_LANE_CAPACITY),
            ("buffer_bytes", self.buffer_bytes, LINK_BUFFER_BYTES),
        ] {
            if value == 0 || value > ceiling {
                return Err(registry::Error::msg(format!(
                    "Link {name} must be positive and no greater than compiled ceiling {ceiling}"
                )));
            }
        }
        if !self.inbox_capacity.is_power_of_two()
            || !self.lane_capacity.is_power_of_two()
            || !self.buffer_bytes.is_power_of_two()
        {
            return Err(registry::Error::msg(
                "Link inbox_capacity and buffer_bytes must be powers of two",
            ));
        }
        if !crate::link::application::lane_fits_inbox(self.spec()) {
            return Err(registry::Error::msg(format!(
                "Link lane_capacity {} must be below inbox_capacity {}: a sender's inbox takes the replies to all its open exchanges and control frames beside them",
                self.lane_capacity, self.inbox_capacity
            )));
        }
        Ok(())
    }

    /// The inbox also carries control frames, so the admission window must
    /// leave space beyond the running and queued exchanges.
    pub fn admits(
        self,
        max_in_flight: usize,
        max_queued: usize,
    ) -> bool {
        max_in_flight != 0
            && max_in_flight
                .checked_add(max_queued)
                .is_some_and(|window| window < self.inbox_capacity)
    }

    fn check_admission(
        self,
        max_in_flight: usize,
        max_queued: usize,
    ) -> registry::Result<()> {
        if self.admits(max_in_flight, max_queued) {
            return Ok(());
        }
        Err(registry::Error::msg(format!(
            "Link needs positive max_in_flight ({max_in_flight}) and max_in_flight + max_queued ({max_queued}) below inbox_capacity ({}): the inbox also carries control frames",
            self.inbox_capacity
        )))
    }

    pub fn spec(self) -> orbitive::link::LinkSpec {
        orbitive::link::LinkSpec::new(
            self.fleet_capacity as u16,
            LINK_SPEC.inbox_kind,
            orbitive::link::InboxGeometry::new(
                self.inbox_capacity,
                self.inbox_payload_capacity,
            ),
            orbitive::stream::StreamSpec::new(
                LINK_SPEC.streams.kind,
                self.lane_capacity,
                self.buffer_bytes,
            ),
        )
    }
}

impl LinkConfig {
    pub fn validate(&self) -> registry::Result<()> {
        self.geometry.validate()?;
        if let Some(limit) = self.max_in_flight {
            self.check_bound(Some(limit))?;
        }
        if let Some(workers) = &self.workers {
            workers.validate()?;
            // The fleet's lanes hold every worker and at least one caller.
            if workers.maximum(u16::MAX) != u16::MAX
                && usize::from(workers.maximum(u16::MAX))
                    >= self.geometry.fleet_capacity
            {
                return Err(registry::Error::msg(format!(
                    "Link workers maximum must leave a lane for a caller in fleet_capacity {}",
                    self.geometry.fleet_capacity
                )));
            }
        }
        Ok(())
    }

    pub(super) fn execution_bound(
        &self,
        engine: Option<(usize, &'static str)>,
    ) -> registry::Result<Option<usize>> {
        if let Some((_, key)) = engine
            && self.max_in_flight.is_some()
        {
            return Err(registry::Error::msg(format!(
                "orbit.link.max_in_flight is set by {key}; remove orbit.link.max_in_flight",
            )));
        }
        let limit = engine.map(|(limit, _)| limit).or(self.max_in_flight);
        self.check_bound(limit)?;
        if limit.is_none()
            && self.workers.is_some_and(|workers| {
                workers.retire_idle().timeout().is_some()
            })
        {
            return Err(registry::Error::msg(
                "idle retirement requires tracked receiver admission (max_in_flight/max_queued)",
            ));
        }
        Ok(limit)
    }

    fn check_bound(
        &self,
        limit: Option<usize>,
    ) -> registry::Result<()> {
        match (limit, self.max_queued) {
            (Some(limit), Some(queued)) => {
                self.geometry.check_admission(limit, queued)
            }
            (Some(_), None) => Err(registry::Error::msg(
                "a bounded Link receiver needs orbit.link.max_queued: max_queued counts only waiting exchanges",
            )),
            (None, Some(_)) => Err(registry::Error::msg(
                "orbit.link.max_queued needs a receiver execution bound",
            )),
            (None, None) => Ok(()),
        }
    }
}

struct Active {
    segment: Weak<ApplicationSegment>,
    joined: Weak<Joined>,
}

struct State {
    limit: Option<usize>,
    max_queued: Option<usize>,
    engine_key: Option<&'static str>,
    geometry: LinkGeometry,
    shared_response: bool,
    workers: Option<Workers>,
    active: Option<Active>,
}

/// Cold-path control of a receiver. It retains no lane or exchange ownership.
pub struct LinkCapacity {
    state: Mutex<State>,
    execution_changed: tokio::sync::Notify,
}

impl LinkCapacity {
    /// A programmatic bound with no queue admits only what it runs.
    pub fn new(
        limit: Option<usize>,
        max_queued: Option<usize>,
    ) -> Self {
        Self {
            state: Mutex::new(State {
                limit,
                max_queued,
                engine_key: None,
                geometry: LinkGeometry::default(),
                shared_response: false,
                workers: None,
                active: None,
            }),
            execution_changed: tokio::sync::Notify::new(),
        }
    }

    pub fn set_max_in_flight(
        &self,
        limit: usize,
    ) -> registry::Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| registry::Error::msg("Link capacity lock poisoned"))?;
        if let Some(key) = state.engine_key
            && state.limit != Some(limit)
        {
            return Err(registry::Error::msg(format!(
                "orbit.link.max_in_flight is set by {key}",
            )));
        }
        let queued = state.max_queued.unwrap_or(0);
        state.geometry.check_admission(limit, queued)?;
        if let Some(active) = state.active.as_ref()
            && let (Some(segment), Some(joined)) =
                (active.segment.upgrade(), active.joined.upgrade())
        {
            let update_window =
                || segment.update_work_capacity(&joined, limit + queued);
            let update_execution =
                || segment.set_work_execution(joined.lane, limit);
            // Keep the execution bound inside the window through both writes.
            let updated = if queued == 0 {
                update_window()
            } else if state.limit.is_some_and(|previous| limit < previous) {
                update_execution().and_then(|()| update_window())
            } else {
                update_window().and_then(|()| update_execution())
            };
            updated.map_err(|e| registry::Error::msg(e.to_string()))?;
        }
        if queued != 0 && state.limit.is_some_and(|previous| limit > previous) {
            self.execution_changed.notify_one();
        }
        state.limit = Some(limit);
        Ok(())
    }

    /// Wake the receiver when an increase can start work it already holds.
    pub async fn execution_changed(&self) {
        self.execution_changed.notified().await;
    }

    /// Configure before serving; a live receiver's shape cannot change.
    pub fn prepare(
        &self,
        config: &LinkConfig,
        engine: Option<(usize, &'static str)>,
    ) -> registry::Result<()> {
        config.validate()?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| registry::Error::msg("Link capacity lock poisoned"))?;
        if state
            .active
            .as_ref()
            .is_some_and(|active| active.joined.strong_count() != 0)
        {
            return Err(registry::Error::msg(
                "Link geometry is already in use",
            ));
        }
        state.limit = config.execution_bound(engine)?;
        state.max_queued = config.max_queued;
        state.engine_key = engine.map(|(_, key)| key);
        state.geometry = config.geometry;
        state.shared_response = config.shared_response;
        state.workers = config.workers;
        Ok(())
    }

    /// The owner's declared count policy, for its fleet and its master.
    pub fn workers(&self) -> registry::Result<Option<Workers>> {
        self.state
            .lock()
            .map(|state| state.workers)
            .map_err(|_| registry::Error::msg("Link capacity lock poisoned"))
    }

    /// The startup queue bound, counting waiting exchanges only.
    pub fn max_queued(&self) -> registry::Result<Option<usize>> {
        self.state
            .lock()
            .map(|state| state.max_queued)
            .map_err(|_| registry::Error::msg("Link capacity lock poisoned"))
    }

    /// The exchanges the receiver runs at once; `None` is no bound.
    pub fn max_in_flight(&self) -> registry::Result<Option<usize>> {
        self.limit()
    }

    pub(super) fn limit(&self) -> registry::Result<Option<usize>> {
        self.state
            .lock()
            .map(|state| state.limit)
            .map_err(|_| registry::Error::msg("Link capacity lock poisoned"))
    }

    pub fn geometry(&self) -> registry::Result<LinkGeometry> {
        self.state
            .lock()
            .map(|state| state.geometry)
            .map_err(|_| registry::Error::msg("Link capacity lock poisoned"))
    }

    pub fn shared_response(&self) -> registry::Result<bool> {
        self.state
            .lock()
            .map(|state| state.shared_response)
            .map_err(|_| registry::Error::msg("Link capacity lock poisoned"))
    }

    pub fn join(
        &self,
        fleet: &str,
        application: &str,
        runtime: &str,
        incarnation: u64,
    ) -> registry::Result<(Arc<ApplicationSegment>, Arc<Joined>)> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| registry::Error::msg("Link capacity lock poisoned"))?;
        let segment = Arc::new(
            ApplicationSegment::for_fleet_with_spec(
                fleet,
                state.geometry.spec(),
                &Access::OwnerOnly,
            )
            .map_err(|e| registry::Error::msg(e.to_string()))?,
        );
        let queued = state.max_queued.unwrap_or(0);
        let window = state
            .limit
            .map(|limit| {
                state.geometry.check_admission(limit, queued)?;
                Ok::<_, registry::Error>(limit + queued)
            })
            .transpose()?;
        let joined = Arc::new(
            segment
                .join_with_capacity(application, runtime, incarnation, window)
                .map_err(|e| registry::Error::msg(e.to_string()))?,
        );
        // The window is published; how much of it runs at once goes with it.
        if let Some(limit) = state.limit {
            segment
                .set_work_execution(joined.lane, limit)
                .map_err(|e| registry::Error::msg(e.to_string()))?;
        }
        state.active = Some(Active {
            segment: Arc::downgrade(&segment),
            joined: Arc::downgrade(&joined),
        });
        Ok((segment, joined))
    }
}

#[cfg(test)]
#[path = "../../tests/config/link.rs"]
mod tests;
