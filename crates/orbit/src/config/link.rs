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
#[serde(default, deny_unknown_fields)]
pub struct LinkConfig {
    /// Receiver admission bound; absent uses the engine's declared default,
    /// or leaves admission unbounded when the engine declares none.
    pub max_in_flight: Option<usize>,
    pub geometry: LinkGeometry,
    /// How the owner grows under unmet demand. Absent is static: nothing is
    /// published and no sender reports pressure.
    pub workers: Option<LinkWorkers>,
}

/// The owner's growth policy, declared beside its shape and bound and
/// published in its fleet. Every field is written when the block is.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LinkWorkers {
    pub policy: WorkerPolicy,
    /// Never fewer workers.
    pub min: u16,
    /// Never more workers: a count, or `available` for the supervisor's
    /// worker capacity. Within that capacity and the fleet's lanes either way.
    pub max: WorkerMax,
    /// Workers added per pressure episode.
    pub step: u16,
    /// Automatic retirement of workers added under pressure.
    pub retire_idle: RetireIdle,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkerPolicy {
    /// Today's behaviour: the worker count is the launch's.
    Static,
    /// Add `step` workers when a sender finds every receiver full, up to `max`.
    OnPressure,
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

/// Only `off` is defined: what "idle" means without an invented duration is
/// still open, so nothing is retired automatically.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RetireIdle {
    Off,
}

impl LinkWorkers {
    pub fn validate(&self) -> registry::Result<()> {
        // A named maximum is checked once it is resolved, at the master.
        if self.min == 0
            || matches!(self.max, WorkerMax::Count(max) if self.min > max)
        {
            return Err(registry::Error::msg(format!(
                "Link workers need 0 < min <= max (min {}, max {:?})",
                self.min, self.max
            )));
        }
        // The supervisor resizes one worker at a time and only while the
        // fleet is stable; a larger step has nothing to run it yet.
        if self.step != 1 {
            return Err(registry::Error::msg(format!(
                "Link workers.step must be 1 (got {}): workers are added one per pressure episode",
                self.step
            )));
        }
        Ok(())
    }

    /// What the fleet publishes, with the maximum resolved: `None` while the
    /// policy is static.
    pub fn growth(
        &self,
        worker_capacity: u16,
    ) -> Option<orbitive::link::GrowthPolicy> {
        (self.policy == WorkerPolicy::OnPressure).then_some(
            orbitive::link::GrowthPolicy {
                min: self.min,
                max: self.max.resolve(worker_capacity),
                step: self.step,
            },
        )
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

impl LinkGeometry {
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

    /// A receiver's admission bound must leave its inbox room for the
    /// control frames that share it with admitted work (status, capacity,
    /// shutdown), so it is positive and below the inbox capacity.
    pub fn admits(
        self,
        max_in_flight: usize,
    ) -> bool {
        max_in_flight != 0 && max_in_flight < self.inbox_capacity
    }

    fn check_admission(
        self,
        max_in_flight: usize,
    ) -> registry::Result<()> {
        if self.admits(max_in_flight) {
            return Ok(());
        }
        Err(registry::Error::msg(format!(
            "Link max_in_flight {max_in_flight} must be positive and below inbox_capacity {}: the inbox also carries control frames",
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
            self.geometry.check_admission(limit)?;
        }
        if let Some(workers) = &self.workers {
            workers.validate()?;
            // The fleet's lanes hold every worker and at least one caller.
            if let WorkerMax::Count(max) = workers.max
                && usize::from(max) >= self.geometry.fleet_capacity
            {
                return Err(registry::Error::msg(format!(
                    "Link workers.max {max} must leave a lane for a caller in fleet_capacity {}",
                    self.geometry.fleet_capacity
                )));
            }
        }
        Ok(())
    }
}

struct Active {
    segment: Weak<ApplicationSegment>,
    joined: Weak<Joined>,
}

struct State {
    limit: Option<usize>,
    geometry: LinkGeometry,
    workers: Option<LinkWorkers>,
    active: Option<Active>,
}

/// Cold-path control of a receiver. It retains no lane or exchange ownership.
pub struct LinkCapacity(Mutex<State>);

impl LinkCapacity {
    pub fn new(limit: Option<usize>) -> Self {
        Self(Mutex::new(State {
            limit,
            geometry: LinkGeometry::default(),
            workers: None,
            active: None,
        }))
    }

    pub fn set_max_in_flight(
        &self,
        limit: usize,
    ) -> registry::Result<()> {
        let mut state = self
            .0
            .lock()
            .map_err(|_| registry::Error::msg("Link capacity lock poisoned"))?;
        state.geometry.check_admission(limit)?;
        if let Some(active) = state.active.as_ref()
            && let (Some(segment), Some(joined)) =
                (active.segment.upgrade(), active.joined.upgrade())
        {
            segment
                .update_work_capacity(&joined, limit)
                .map_err(|e| registry::Error::msg(e.to_string()))?;
        }
        state.limit = Some(limit);
        Ok(())
    }

    /// Configure before serving; a live receiver's shape cannot change.
    pub fn prepare(
        &self,
        config: &LinkConfig,
        fallback: Option<usize>,
    ) -> registry::Result<()> {
        config.validate()?;
        let mut state = self
            .0
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
        let limit = config.max_in_flight.or(fallback);
        if let Some(limit) = limit {
            config.geometry.check_admission(limit)?;
        }
        state.limit = limit;
        state.geometry = config.geometry;
        state.workers = config.workers;
        Ok(())
    }

    /// The owner's declared growth policy, for its fleet and its master.
    pub fn workers(&self) -> registry::Result<Option<LinkWorkers>> {
        self.0
            .lock()
            .map(|state| state.workers)
            .map_err(|_| registry::Error::msg("Link capacity lock poisoned"))
    }

    pub(super) fn limit(&self) -> registry::Result<Option<usize>> {
        self.0
            .lock()
            .map(|state| state.limit)
            .map_err(|_| registry::Error::msg("Link capacity lock poisoned"))
    }

    pub fn geometry(&self) -> registry::Result<LinkGeometry> {
        self.0
            .lock()
            .map(|state| state.geometry)
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
            .0
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
        let joined = Arc::new(
            segment
                .join_with_capacity(
                    application,
                    runtime,
                    incarnation,
                    state.limit,
                )
                .map_err(|e| registry::Error::msg(e.to_string()))?,
        );
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
