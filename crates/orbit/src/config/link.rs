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
        match self.max_in_flight {
            Some(limit) => self.geometry.check_admission(limit),
            None => Ok(()),
        }
    }
}

struct Active {
    segment: Weak<ApplicationSegment>,
    joined: Weak<Joined>,
}

struct State {
    limit: Option<usize>,
    geometry: LinkGeometry,
    active: Option<Active>,
}

/// Cold-path control of a receiver. It retains no lane or exchange ownership.
pub struct LinkCapacity(Mutex<State>);

impl LinkCapacity {
    pub fn new(limit: Option<usize>) -> Self {
        Self(Mutex::new(State {
            limit,
            geometry: LinkGeometry::default(),
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
        Ok(())
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
