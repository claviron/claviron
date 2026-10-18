//! Host-local wire snapshot: monotonic expiry, not std::Instant or wall time.
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use super::set::Slot;
use super::table::PublicationError;
use super::{ReservedOriginId, ResolutionGeneration, ResolvedSet};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolutionSnapshot {
    first: ReservedOriginId,
    slots: Vec<Option<Slot>>,
    desired: Vec<super::Address>,
    generation: ResolutionGeneration,
    expires_at_ms: Option<u64>,
    stale: bool,
    truncated: bool,
}
impl ResolvedSet {
    pub fn snapshot(&self) -> Result<ResolutionSnapshot, PublicationError> {
        // Sample host time first, process time second. Flooring the remaining
        // lifetime cannot extend cache expiry across a publication boundary.
        let host = u64::try_from(monotonic()?.as_millis())
            .map_err(|_| PublicationError::Invalid("host clock exceeds u64"))?;
        let now = Instant::now();
        let expires_at_ms = self
            .valid_until
            .map(|expiry| {
                host.checked_add(
                    u64::try_from(
                        expiry.saturating_duration_since(now).as_millis(),
                    )
                    .map_err(|_| {
                        PublicationError::Invalid("expiry exceeds u64")
                    })?,
                )
                .ok_or(PublicationError::Invalid("expiry exceeds host clock"))
            })
            .transpose()?;
        Ok(ResolutionSnapshot {
            first: self.first,
            slots: self.slots.clone(),
            desired: self.desired.clone(),
            generation: self.generation,
            expires_at_ms,
            stale: self.stale,
            truncated: self.truncated,
        })
    }
}
impl ResolutionSnapshot {
    pub fn generation(&self) -> ResolutionGeneration {
        self.generation
    }
    pub fn stale(&self) -> bool {
        self.stale
    }
    pub fn truncated(&self) -> bool {
        self.truncated
    }
    pub fn restore(&self) -> Result<ResolvedSet, PublicationError> {
        self.validate()?;
        let mut set =
            ResolvedSet::new(self.first, self.slots.len()).map_err(|_| {
                PublicationError::Invalid(
                    "invalid reserved ReservedOriginId range",
                )
            })?;
        // Sample local first, host second: a peer never grants a fresh TTL.
        let local = Instant::now();
        let host = monotonic()?;
        set.valid_until = self
            .expires_at_ms
            .map(|deadline| {
                local
                    .checked_add(remaining_host_lifetime(deadline, host)?)
                    .ok_or(PublicationError::Invalid(
                        "expiry exceeds process clock",
                    ))
            })
            .transpose()?;
        set.slots = self.slots.clone();
        set.desired = self.desired.clone();
        set.generation = self.generation;
        set.stale = self.stale;
        set.truncated = self.truncated;
        Ok(set)
    }
    pub(super) fn same_membership(
        &self,
        other: &Self,
    ) -> bool {
        self.first == other.first
            && self.slots == other.slots
            && self.desired == other.desired
            && self.generation == other.generation
    }
    pub(super) fn validate(&self) -> Result<(), PublicationError> {
        let invalid = |detail| PublicationError::Invalid(detail);
        if self.slots.is_empty()
            || self.slots.len() > usize::from(u16::MAX) + 1 - self.first.index()
            || self.desired.len() > self.slots.len()
        {
            return Err(invalid(
                "invalid reserved ReservedOriginId range or desired set length",
            ));
        }
        if self.desired.windows(2).any(|pair| pair[0].socket >= pair[1].socket)
            || self.desired.iter().any(|address| address.socket.port() == 0)
        {
            return Err(invalid(
                "desired endpoints must be sorted, unique and have ports",
            ));
        }
        let mut endpoints = std::collections::BTreeSet::new();
        for (index, slot) in
            self.slots.iter().enumerate().filter_map(|(index, slot)| {
                slot.as_ref().map(|slot| (index, slot))
            })
        {
            let assignment = slot.assignment;
            if assignment.id.index() != self.first.index() + index
                || assignment.installed.0 == 0
                || assignment.installed > self.generation
                || assignment.address.socket.port() == 0
                || !endpoints.insert(assignment.address.socket)
            {
                return Err(invalid("invalid or duplicate slot assignment"));
            }
            if !slot.retiring && !self.desired.contains(&assignment.address) {
                return Err(invalid(
                    "active assignment is absent from desired membership",
                ));
            }
        }
        if self.generation.0 == 0
            && (!endpoints.is_empty()
                || !self.desired.is_empty()
                || self.expires_at_ms.is_some())
        {
            return Err(invalid("generation zero cannot contain an answer"));
        }
        if self.generation.0 > 0 && self.expires_at_ms.is_none() {
            return Err(invalid(
                "an answered generation must retain its expiry",
            ));
        }
        // apply() eagerly fills every free slot with unblocked desired endpoints.
        // Restoring a gap would strand that endpoint: an unchanged answer does
        // not call activate() again. A retiring assignment may still block its
        // own desired socket, even when another slot is free.
        if self.slots.iter().any(Option::is_none)
            && self
                .desired
                .iter()
                .any(|address| !endpoints.contains(&address.socket))
        {
            return Err(invalid(
                "unblocked desired endpoint is missing from a free slot",
            ));
        }
        Ok(())
    }
}

pub(super) fn remaining_host_lifetime(
    deadline: u64,
    host: Duration,
) -> Result<Duration, PublicationError> {
    // The published deadline is floored. Round the reader's clock upward:
    // flooring both would grant a fresh fractional millisecond of cache life.
    let host = u64::try_from(host.as_nanos().div_ceil(1_000_000))
        .map_err(|_| PublicationError::Invalid("host clock exceeds u64"))?;
    Ok(Duration::from_millis(deadline.saturating_sub(host)))
}

#[cfg(unix)]
fn monotonic() -> Result<Duration, PublicationError> {
    let mut time = std::mem::MaybeUninit::<libc::timespec>::uninit();
    // SAFETY: time points to sufficient writable storage for clock_gettime.
    if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, time.as_mut_ptr()) }
        != 0
    {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: a successful clock_gettime initialized the complete timespec.
    let time = unsafe { time.assume_init() };
    let seconds = u64::try_from(time.tv_sec)
        .map_err(|_| PublicationError::Invalid("negative host clock"))?;
    let nanos = u32::try_from(time.tv_nsec)
        .map_err(|_| PublicationError::Invalid("negative host clock"))?;
    if nanos >= 1_000_000_000 {
        return Err(PublicationError::Invalid(
            "invalid host clock nanoseconds",
        ));
    }
    Ok(Duration::new(seconds, nanos))
}
#[cfg(not(unix))]
fn monotonic() -> Result<Duration, PublicationError> {
    // Non-POSIX backings are process-local; this origin only encodes that clock.
    static START: std::sync::LazyLock<Instant> =
        std::sync::LazyLock::new(Instant::now);
    Ok(START.elapsed())
}
