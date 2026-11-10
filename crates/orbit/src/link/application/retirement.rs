//! Controller-owned idle observation using existing member capacity interests.
//! No new SHM fields, language ABI or polling cadence is introduced.

use super::{
    ApplicationReceiver, ApplicationSegment, Joined, Received, Result,
    map_link_error,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

/// The process owner supplies its highest removable worker, not a metrics view.
#[async_trait::async_trait]
pub trait RetirementTarget: Send + Sync {
    async fn candidate_pid(&self) -> Option<u32>;
    /// Park until the owner's authoritative candidate changes.
    async fn changed(
        &self,
        previous: Option<u32>,
    );
}

pub struct RetirementSource(pub Arc<dyn RetirementTarget>);

#[derive(Default)]
struct IdleClock {
    generation: Option<u64>,
    deadline: Option<Instant>,
}

impl IdleClock {
    fn observe(
        &mut self,
        now: Instant,
        generation: u64,
        idle: bool,
        timeout: Duration,
    ) -> Option<Instant> {
        if self.generation != Some(generation) || !idle {
            self.deadline = None;
        }
        self.generation = Some(generation);
        if idle {
            Some(*self.deadline.get_or_insert(now + timeout))
        } else {
            None
        }
    }
}

/// One optional management lane, created only when idle retirement is enabled.
/// It serves no application name and owns the only reader of this lane.
pub struct IdleRetirement {
    segment: Arc<ApplicationSegment>,
    joined: Joined,
    receiver: ApplicationReceiver,
    candidate: Option<(u32, Vec<(usize, u64)>)>,
    clock: IdleClock,
}

impl IdleRetirement {
    pub fn new(
        segment: Arc<ApplicationSegment>,
        incarnation: u64,
    ) -> Result<Self> {
        let joined =
            segment.join("worker-retirement", "management", incarnation)?;
        let receiver = match segment.application_receiver(joined.lane) {
            Ok(receiver) => receiver,
            Err(error) => {
                let _ = segment.inbox().release_lane(joined.lane);
                return Err(error);
            }
        };
        Ok(Self {
            segment,
            joined,
            receiver,
            candidate: None,
            clock: IdleClock::default(),
        })
    }

    /// Wait for one exact owned worker to remain empty for the declared time.
    /// Watching only this candidate means another busy worker cannot postpone it.
    /// Reservations, queued work and running body/session owners all count busy.
    pub async fn wait(
        &mut self,
        pid: u32,
        timeout: Duration,
    ) -> Result<Option<u32>> {
        let inbox = self.segment.inbox();
        let directory = inbox.directory();
        let lanes: Vec<_> = (0..inbox.lanes())
            .filter(|&lane| {
                inbox.owner(lane).ok() == Some(Some(pid))
                    && inbox
                        .consumers(lane)
                        .ok()
                        .is_some_and(|count| count != 0)
            })
            .collect();
        if lanes.is_empty() {
            // A candidate may be owned but still joining. Park on the real
            // directory wake rather than spinning while the lane is absent.
            self.receiver.next(&mut Vec::new()).await?;
            return Ok(None);
        }
        let identities: Vec<_> = lanes
            .iter()
            .map(|&lane| {
                if inbox.work_capacity(lane).map_err(map_link_error)?.is_none()
                {
                    return Err(super::Error::Malformed(
                        "idle retirement requires tracked receiver admission",
                    ));
                }
                inbox.incarnation(lane).map_err(map_link_error)
            })
            .collect::<Result<_>>()?;
        if inbox.directory() != directory {
            return Ok(None);
        }
        let candidate = (
            pid,
            lanes.iter().copied().zip(identities.iter().copied()).collect(),
        );
        if self.candidate.as_ref() != Some(&candidate) {
            self.clock = IdleClock::default();
            self.candidate = Some(candidate);
        }
        let mut frame = Vec::new();
        loop {
            for &lane in &lanes {
                inbox
                    .watch_capacity(lane, self.joined.lane)
                    .map_err(map_link_error)?;
            }
            let generation = inbox
                .capacity_generation(self.joined.lane)
                .map_err(map_link_error)?;
            if !lanes.iter().zip(&identities).all(|(&lane, &incarnation)| {
                inbox.owner(lane).ok() == Some(Some(pid))
                    && inbox.incarnation(lane).ok() == Some(incarnation)
                    && inbox
                        .consumers(lane)
                        .ok()
                        .is_some_and(|count| count != 0)
            }) {
                self.receiver.next(&mut frame).await?;
                return Ok(None);
            }
            let idle =
                lanes.iter().all(|&lane| inbox.work_load(lane).ok() == Some(0));
            let deadline =
                self.clock.observe(Instant::now(), generation, idle, timeout);
            if inbox
                .capacity_generation(self.joined.lane)
                .map_err(map_link_error)?
                != generation
            {
                continue;
            }
            let expiry = async {
                match deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::select! {
                biased;
                received = self.receiver.next(&mut frame) => {
                    // A completed/cancelled claim resets the idle clock even
                    // if work arrived and finished between two observations.
                    match received? {
                        Received::Capacity(_) => self.clock = IdleClock::default(),
                        Received::Directory(_) => return Ok(None), // refresh all lanes of this PID
                        _ => {}
                    }
                }
                () = expiry => {
                    // Another serving lane may have joined this same process
                    // after the membership snapshot. Recollect before deciding.
                    if inbox.directory() != directory { return Ok(None); }
                    if inbox.capacity_generation(self.joined.lane).map_err(map_link_error)? == generation
                        && lanes.iter().zip(&identities).all(|(&lane, &incarnation)| {
                            inbox.owner(lane).ok() == Some(Some(pid)) && inbox.incarnation(lane).ok() == Some(incarnation)
                                && inbox.consumers(lane).ok().is_some_and(|count| count != 0)
                                && inbox.work_load(lane).ok() == Some(0)
                        }) {
                        return Ok(Some(pid));
                    }
                }
            }
        }
    }
}

impl Drop for IdleRetirement {
    fn drop(&mut self) {
        let _ = self.segment.inbox().release_lane(self.joined.lane);
    }
}

#[cfg(test)]
#[path = "../../../tests/link/application/retirement.rs"]
mod tests;
