//! Master-local heartbeat observations, not process ownership or restart policy.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use claviron_orbit::placement::{Scope, Scoped};
use claviron_orbit::{Fleet, NodeId, OrbitEpoch};
use claviron_runtime::registry::{self, Component};
use claviron_runtime::{AppState, WorkerId};
use tokio::sync::RwLock;

use crate::heartbeat::{self, HeartbeatRecord};

/// Tracks heartbeat evidence for worker incarnations validated by the supervisor.
///
/// The monitor samples this capability before evaluating liveness policy. It has
/// no independent ticker, child handles, signal delivery, or process-status table.
pub(crate) struct HeartbeatObserver {
    fleet: OnceLock<Arc<Fleet>>,
    workers: RwLock<HashMap<WorkerId, HeartbeatObservation>>,
}

/// PID-correlated heartbeat evidence for one tracked slot, including its grace
/// anchor and accepted lane counters. Keeps freshness distinct from both boot
/// completion and kernel process liveness; it never owns the worker process.
#[derive(Clone, Debug)]
pub(crate) struct HeartbeatObservation {
    pub(crate) pid: u32,
    // Initially the boot-confirmation time (grace anchor), then sample time.
    pub(crate) last_heartbeat: Instant,
    pub(crate) first_heartbeat: Option<Instant>,
    pub(crate) beats: u64,
    min_counter: u64,
    last_counter: Option<u64>,
}

impl HeartbeatObserver {
    pub(crate) fn new() -> Self {
        Self { fleet: OnceLock::new(), workers: RwLock::new(HashMap::new()) }
    }

    /// The master preparation barrier attaches the process-lifetime fleet
    /// before spawning or adopting workers. Observation does not own its boot.
    pub(crate) fn attach_fleet(
        &self,
        fleet: Arc<Fleet>,
    ) -> registry::Result<()> {
        self.fleet.set(fleet).map_err(|_| {
            registry::Error::msg(
                "heartbeat observation fleet is already attached",
            )
        })
    }

    /// Start the grace period only after the supervisor validates a boot event.
    /// Existing lane records cannot refresh this new incarnation's liveness.
    pub(crate) async fn track_worker(
        &self,
        worker_id: WorkerId,
        pid: u32,
    ) {
        let mut workers = self.workers.write().await;
        let min_counter = self.fleet.get().map_or(0, |fleet| {
            fleet.lane_head::<HeartbeatRecord>(NodeId::new(u16::from(
                worker_id.raw(),
            )))
        });
        workers.insert(
            worker_id,
            HeartbeatObservation {
                pid,
                last_heartbeat: Instant::now(),
                first_heartbeat: None,
                beats: 0,
                min_counter,
                last_counter: None,
            },
        );
    }

    /// Close only the matching incarnation's observation. Completion is not OS
    /// exit: the supervisor must still reap the child and enforce its deadline.
    pub(crate) async fn complete_worker_shutdown(
        &self,
        worker_id: WorkerId,
        pid: u32,
    ) -> bool {
        let mut workers = self.workers.write().await;
        if !workers.get(&worker_id).is_some_and(|worker| worker.pid == pid) {
            return false;
        }
        workers.remove(&worker_id);
        true
    }

    pub(crate) async fn clear(&self) {
        self.workers.write().await.clear();
    }

    /// How long ago something last published on this worker id's node lane, if
    /// that is recent enough for the publisher to still be alive.
    ///
    /// It cannot say *who*, because the heartbeat carries neither a pid nor a
    /// state — and in particular it cannot say *whether it was us*. These are
    /// this instance's own lanes, and the master that wrote the last beat in
    /// them is, on any ordinary restart, this master's own predecessor. A
    /// recent beat is therefore not evidence of a foreign claim; it is only
    /// evidence that the id was in use at some moment, most often by us.
    ///
    /// Which makes this a suspicion and never a verdict. Confirming it takes
    /// `lane_head`.
    pub(crate) fn lane_beat_age(
        &self,
        worker_id: WorkerId,
        max_age: Duration,
    ) -> Option<Duration> {
        let fleet = self.fleet.get()?;
        let latest = heartbeat::latest_for(
            fleet.as_ref(),
            NodeId::new(u16::from(worker_id.raw())),
        )?;
        let age = latest.age_at(OrbitEpoch::now());
        (age < max_age).then_some(age)
    }

    /// Where this worker id's node lane has been written up to.
    ///
    /// The one thing that separates a publisher from its residue: a process
    /// that is still alive advances this, a record left behind by one that is
    /// gone does not. Ages cannot make that distinction — the SHM segment
    /// survives master generations, so a master restarted inside
    /// `heartbeat_timeout` reads its own predecessor's final beats as fresh,
    /// and would refuse to start the workers it had itself just stopped.
    pub(crate) fn lane_head(
        &self,
        worker_id: WorkerId,
    ) -> Option<u64> {
        let fleet = self.fleet.get()?;
        Some(fleet.lane_head::<HeartbeatRecord>(NodeId::new(u16::from(
            worker_id.raw(),
        ))))
    }

    /// Read the ring before returning stale (worker id, PID) observations.
    /// The caller owns the timeout and must validate each PID against its child
    /// table before acting. This result is not evidence of OS process exit.
    pub(crate) async fn stale_workers(
        &self,
        timeout: Duration,
    ) -> Vec<(WorkerId, u32)> {
        self.observe_heartbeats(timeout).await;
        let workers = self.workers.read().await;
        let now = Instant::now();
        workers
            .iter()
            .filter_map(|(id, worker)| {
                (now.duration_since(worker.last_heartbeat) > timeout)
                    .then_some((*id, worker.pid))
            })
            .collect()
    }

    /// How long ago each tracked worker's last accepted heartbeat was sampled,
    /// after folding the ring once more. A worker that has not beaten since its
    /// boot was confirmed has no age yet: the grace anchor is not a heartbeat.
    pub(crate) async fn heartbeat_ages(
        &self,
        max_age: Duration,
    ) -> HashMap<WorkerId, Duration> {
        self.observe_heartbeats(max_age).await;
        let workers = self.workers.read().await;
        let now = Instant::now();
        workers
            .iter()
            .filter(|(_, worker)| worker.first_heartbeat.is_some())
            .map(|(id, worker)| {
                (*id, now.saturating_duration_since(worker.last_heartbeat))
            })
            .collect()
    }

    /// Fold each fresh, new lane record into the tracked incarnation. Re-reading
    /// a record does not renew liveness; neither does a record predating boot.
    async fn observe_heartbeats(
        &self,
        max_age: Duration,
    ) {
        let Some(fleet) = self.fleet.get() else {
            return;
        };
        let now_epoch = OrbitEpoch::now();
        let observed_at = Instant::now();
        let latest = heartbeat::latest_heartbeats(fleet.as_ref());
        let mut workers = self.workers.write().await;

        for heartbeat in latest {
            let Ok(node) = u8::try_from(heartbeat.node_id.get()) else {
                continue;
            };
            let sample_age = heartbeat.age_at(now_epoch);
            if node == 0 || sample_age > max_age {
                continue;
            }
            let Some(worker) = workers.get_mut(&WorkerId::new(node)) else {
                continue;
            };
            let counter = heartbeat.counter();
            if counter < worker.min_counter
                || worker.last_counter.is_some_and(|last| counter <= last)
            {
                continue;
            }

            let sample_at =
                observed_at.checked_sub(sample_age).unwrap_or(observed_at);
            worker.last_counter = Some(counter);
            worker.last_heartbeat = sample_at;
            worker.first_heartbeat.get_or_insert(sample_at);
            worker.beats = worker.beats.saturating_add(1);
        }
    }

    #[cfg(test)]
    pub(crate) async fn snapshot(
        &self
    ) -> HashMap<WorkerId, HeartbeatObservation> {
        self.workers.read().await.clone()
    }
}

impl Component<AppState> for HeartbeatObserver {
    type Deps = ();

    fn as_scoped(&self) -> Option<&dyn Scoped> {
        Some(self)
    }
    fn name(&self) -> &'static str {
        "HeartbeatObserver"
    }
}
impl Scoped for HeartbeatObserver {
    fn scope(&self) -> Scope {
        Scope::Master
    }
}

#[cfg(test)]
#[path = "../../tests/observer.rs"]
mod tests;
