use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use claviron_runtime::WorkerId;
use claviron_sys::process::ProcessIdentity;
use tokio::sync::RwLock;

use crate::lifecycle::{
    DrainReason, ExitDisposition, ExitOutcome, FleetMode, LifecycleOperation,
    MasterIdentity, OperationTable, OwnedProcess, WorkerIdentity, WorkerIntent,
    WorkerPhase,
};
use crate::snapshot::WorkerLifecycle;
use crate::{Error, Result};

pub(crate) const FLEET_RUNNING: u8 = 0;
pub(crate) const FLEET_SHUTTING_DOWN: u8 = 1;
/// The master-lifetime ownership aggregate. It has no runtime service lifecycle.
///
/// Keeps process handles, slot state, master identity and operation ownership
/// together so every lifecycle action consults the same authority. It outlives
/// individual service generations and owns no ticker, config loader or event
/// listener; monitoring and management act on it through named use cases.
pub struct WorkerFleet {
    pub(crate) startup_completed: AtomicBool,
    pub(crate) workers: Arc<RwLock<Vec<WorkerProcess>>>,
    pub(crate) master: OnceLock<MasterIdentity>,
    pub(crate) fleet_mode: AtomicU8,
    pub(crate) shutdown_driven: AtomicBool,
    pub(crate) operations: OperationTable,
}
impl Default for WorkerFleet {
    fn default() -> Self {
        Self {
            startup_completed: AtomicBool::new(false),
            workers: Arc::new(RwLock::new(Vec::new())),
            master: OnceLock::new(),
            fleet_mode: AtomicU8::new(FLEET_RUNNING),
            shutdown_driven: AtomicBool::new(false),
            operations: OperationTable::default(),
        }
    }
}
/// Authoritative state of one worker slot, including its optional exact process
/// handle, observed phase, desired intent, incarnation and recovery history.
///
/// Phase and intent own separate clocks updated only by their setters. The row
/// survives service restarts and can remain without a live process, preserving
/// operator intent and incarnation history for a later replacement.
pub(crate) struct WorkerProcess {
    pub(crate) id: WorkerId,
    pub(crate) process: Option<OwnedProcess>,
    /// What the master has observed of the process, and since when. See
    /// [`WorkerPhase`]. Private so the clock cannot drift from the fact it
    /// dates: every change goes through [`Self::set_phase`].
    observed: WorkerPhase,
    observed_since: Instant,
    /// What the master wants of the slot, and since when. See
    /// [`WorkerIntent`]. Changed only through [`Self::set_intent`] for the
    /// same reason.
    desired: WorkerIntent,
    desired_since: Instant,
    pub(crate) recovery_attempts: u32,
    pub(crate) last_restart: Option<Instant>,
    pub(crate) incarnation: u64,
    pub(crate) reload_revision: u64,
}

impl WorkerProcess {
    /// A fresh row: first incarnation, no recovery history, both clocks at now.
    pub(crate) fn new(
        id: WorkerId,
        process: Option<OwnedProcess>,
        phase: WorkerPhase,
        intent: WorkerIntent,
    ) -> Self {
        let now = Instant::now();
        Self {
            id,
            process,
            observed: phase,
            observed_since: now,
            desired: intent,
            desired_since: now,
            recovery_attempts: 0,
            last_restart: None,
            incarnation: 1,
            reload_revision: 0,
        }
    }

    pub(crate) fn with_incarnation(
        mut self,
        incarnation: u64,
    ) -> Self {
        self.incarnation = incarnation;
        self
    }

    pub(crate) fn with_reload_revision(
        mut self,
        reload_revision: u64,
    ) -> Self {
        self.reload_revision = reload_revision;
        self
    }

    pub(crate) fn with_recovery(
        mut self,
        recovery_attempts: u32,
        last_restart: Option<Instant>,
    ) -> Self {
        self.recovery_attempts = recovery_attempts;
        self.last_restart = last_restart;
        self
    }

    pub(crate) const fn phase(&self) -> WorkerPhase {
        self.observed
    }

    pub(crate) const fn intent(&self) -> WorkerIntent {
        self.desired
    }

    /// How long the slot has been in its current phase. For
    /// `DrainAcknowledged` that is the acknowledgement age; for
    /// `ReconciliationBlocked` how long the block has stood.
    pub(crate) fn phase_age(&self) -> Duration {
        self.observed_since.elapsed()
    }

    /// How long the current intent has stood. For a `Drain` that is how long
    /// the master has been waiting for the process to leave.
    pub(crate) fn intent_age(&self) -> Duration {
        self.desired_since.elapsed()
    }

    /// Record an observation. The clock restarts only when the phase changes
    /// as anyone outside this master would see it — same published state and
    /// same block reason keep the clock, so a blocked slot whose presence
    /// evidence refreshes on every heartbeat still shows how long it has been
    /// blocked, and re-recording the same fact does not make it look new.
    pub(crate) fn set_phase(
        &mut self,
        phase: WorkerPhase,
    ) {
        if !self.observed.same_observation(phase) {
            self.observed_since = Instant::now();
        }
        self.observed = phase;
    }

    /// Record what the master wants. Same clock rule as [`Self::set_phase`].
    pub(crate) fn set_intent(
        &mut self,
        intent: WorkerIntent,
    ) {
        if self.desired != intent {
            self.desired = intent;
            self.desired_since = Instant::now();
        }
    }

    /// Re-issue the ownership claim: the slot stays `OwnershipPending` — same
    /// phase, same clock — and only the request time moves. `false` when the
    /// slot is not waiting for a claim answer.
    pub(crate) fn renew_ownership_claim(&mut self) -> bool {
        match &mut self.observed {
            WorkerPhase::OwnershipPending { requested_at, .. } => {
                *requested_at = Instant::now();
                true
            }
            _ => false,
        }
    }

    pub(crate) fn pid(&self) -> Option<u32> {
        self.process.as_ref().map(OwnedProcess::pid)
    }

    /// The exact identity of this slot's process under `master`, if it has one.
    pub(crate) fn identity(
        &self,
        master: MasterIdentity,
    ) -> Option<WorkerIdentity> {
        self.process.as_ref().map(|process| {
            WorkerIdentity::new(
                self.id,
                process.identity(),
                self.incarnation,
                master,
            )
        })
    }

    /// The operator-facing label, derived from phase and intent every time.
    pub(crate) const fn lifecycle(&self) -> WorkerLifecycle {
        WorkerLifecycle::project(self.phase(), self.intent())
    }

    /// Running, and nothing has asked it to leave.
    pub(crate) const fn is_running(&self) -> bool {
        matches!(self.phase(), WorkerPhase::Running)
            && matches!(self.intent(), WorkerIntent::EnsureRunning)
    }

    /// Running or reloading, and wanted: a slot that carries the fleet's work.
    pub(crate) const fn is_serving(&self) -> bool {
        matches!(self.phase(), WorkerPhase::Running | WorkerPhase::Reloading)
            && matches!(self.intent(), WorkerIntent::EnsureRunning)
    }

    /// Live and wanted, boot confirmed or not. A draining process is not
    /// capacity even though it is still there.
    pub(crate) const fn is_capacity(&self) -> bool {
        self.phase().is_live()
            && matches!(self.intent(), WorkerIntent::EnsureRunning)
    }

    /// No process, and none wanted.
    pub(crate) const fn is_stopped(&self) -> bool {
        matches!(self.intent(), WorkerIntent::Stopped)
            && !self.phase().is_live()
    }

    /// Ask the current process to leave. The phase does not change — the
    /// process is still there — only the intent does. Returns the intent that
    /// stood before, for the caller that cannot prove the exit and must put
    /// it back.
    pub(crate) fn begin_drain(
        &mut self,
        reason: DrainReason,
        after_exit: ExitDisposition,
    ) -> WorkerIntent {
        let previous = self.desired;
        self.set_intent(WorkerIntent::Drain { reason, after_exit });
        previous
    }

    /// The process has been reaped. Classify the exit from the intent that
    /// stood at that moment and let the intent move on.
    pub(crate) fn record_exit(&mut self) -> ExitOutcome {
        let transition = self.desired.on_exit();
        self.set_phase(WorkerPhase::Exited { outcome: transition.outcome });
        self.set_intent(transition.next);
        transition.outcome
    }

    /// The slot has no usable process and is still wanted: recovery applies.
    pub(crate) fn mark_failed(&mut self) {
        self.set_phase(WorkerPhase::Exited { outcome: ExitOutcome::Failed });
        self.set_intent(WorkerIntent::EnsureRunning);
    }
}

/// Lightweight result of a process-health pass for internal reporting.
/// It copies the slot id, PID and projected lifecycle, but owns no process handle
/// and is not the wire-format management snapshot or an authority for signalling.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct WorkerProcessSnapshot {
    pub(crate) id: WorkerId,
    pub(crate) pid: u32,
    pub(crate) lifecycle: WorkerLifecycle,
}

impl WorkerProcessSnapshot {
    pub(crate) const fn id(self) -> WorkerId {
        self.id
    }

    const fn pid(self) -> u32 {
        self.pid
    }

    const fn state(self) -> &'static str {
        self.lifecycle.as_str()
    }
}

/// One live identity a blocked fleet shutdown is still waiting for: enough to
/// explain the wait without reading the worker's log.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BlockedWorker {
    pub(crate) worker_id: WorkerId,
    pub(crate) identity: ProcessIdentity,
    pub(crate) phase: WorkerPhase,
    pub(crate) intent: WorkerIntent,
    pub(crate) operation: Option<LifecycleOperation>,
    /// How long the master has owned this process.
    pub(crate) alive_for: Duration,
}

impl fmt::Display for BlockedWorker {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        write!(
            f,
            "worker #{} pid={} start_stamp={} phase={:?} intent={:?} alive_for={}",
            self.worker_id,
            self.identity.pid(),
            self.identity.started_at(),
            self.phase,
            self.intent,
            humantime::format_duration(Duration::from_secs(
                self.alive_for.as_secs()
            ))
        )?;
        match self.operation {
            Some(operation) => write!(f, " operation={operation}"),
            None => f.write_str(" operation=none"),
        }
    }
}

impl fmt::Debug for WorkerProcessSnapshot {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        f.debug_struct("WorkerProcess")
            .field("id", &self.id())
            .field("pid", &self.pid())
            .field("state", &self.state())
            .finish()
    }
}

#[cfg(test)]
#[path = "../tests/fleet/tests.rs"]
mod tests;

impl WorkerFleet {
    pub(crate) fn fleet_mode(&self) -> FleetMode {
        match self.fleet_mode.load(Ordering::Acquire) {
            FLEET_RUNNING => FleetMode::Running,
            _ => FleetMode::ShuttingDown,
        }
    }
    pub(crate) fn is_shutting_down(&self) -> bool {
        self.fleet_mode() == FleetMode::ShuttingDown
    }
    pub(crate) fn latch_shutdown(&self) -> bool {
        self.fleet_mode
            .compare_exchange(
                FLEET_RUNNING,
                FLEET_SHUTTING_DOWN,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }
    pub(crate) async fn begin_fleet_shutdown(&self) -> bool {
        if !self.latch_shutdown() {
            return false;
        }
        self.operations.begin_fleet_shutdown();
        let mut workers = self.workers.write().await;
        for worker in
            workers.iter_mut().filter(|worker| worker.phase().is_live())
        {
            match worker.intent() {
                WorkerIntent::Drain { reason, .. } => {
                    worker.set_intent(WorkerIntent::Drain {
                        reason,
                        after_exit: ExitDisposition::FinishFleetShutdown,
                    });
                }
                WorkerIntent::EnsureRunning | WorkerIntent::Stopped => {
                    worker.begin_drain(
                        DrainReason::Shutdown,
                        ExitDisposition::FinishFleetShutdown,
                    );
                }
            }
        }
        true
    }
    pub(crate) fn master(&self) -> Result<MasterIdentity> {
        self.master.get().copied().ok_or_else(|| {
            Error::service("master identity is not established before boot")
        })
    }
}

pub(crate) mod model;
pub(crate) mod operation;

#[cfg(test)]
#[path = "../tests/fleet/exit_tests.rs"]
mod exit_tests;
