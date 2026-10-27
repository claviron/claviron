use std::sync::Arc;
use std::time::{Duration, Instant};

use claviron_orbit::{OrbitEpoch, OrbitStateExt};
use claviron_runtime::{AppState, WorkerCount, WorkerId};
use tracing::{debug, info, trace, warn};

use crate::control::{WorkerControl, WorkerProcess};
use crate::lifecycle::{
    LaneWatch, MasterIdentity, OwnedProcess, WorkerBlockReason, WorkerIdentity,
    WorkerIntent, WorkerPhase, master_identity_is_known,
};
use crate::ownership::{
    PresenceDecision, WorkerOwnershipAccepted, WorkerOwnershipClaim,
    assess_presence,
};
use crate::presence::{WorkerPresence, latest_worker_presence};
use crate::{Error, Result};
/// A lane must hold still for one heartbeat interval plus this before it is
/// taken to have nobody behind it, so a beat due exactly at the end of the
/// interval still counts. See [`reconcile_unclaimed_workers`].
///
/// [`reconcile_unclaimed_workers`]: WorkerControl::reconcile_unclaimed_workers
pub(crate) const LANE_WATCH_MARGIN: Duration = Duration::from_millis(250);

/// Recover the logical topology before creating the master process table.
///
/// Exact live presence above the configured floor is enough to reserve those
/// contiguous slots, but not enough to adopt them. The ordinary ownership
/// reconciliation still decides whether each process may be claimed. This
/// split prevents a surviving higher slot from becoming invisible or being
/// overlapped while preserving presence's evidence-only boundary.
pub(crate) fn effective_worker_count_from_presence(
    configured: u8,
    capacity: u8,
    mut latest: impl FnMut(WorkerId) -> Option<WorkerPresence>,
) -> (u8, Vec<WorkerPresence>) {
    if configured >= capacity {
        return (configured, Vec::new());
    }

    let mut effective = configured;
    let mut survivors = Vec::new();
    for raw in configured + 1..=capacity {
        let Some(presence) = latest(WorkerId::new(raw)) else {
            continue;
        };
        if presence.exact_process_alive() {
            effective = raw;
            survivors.push(presence);
        }
    }
    (effective, survivors)
}

/// Restore a topology that a previous master expanded through management.
///
/// This runs during provider boot so every runnable observes the recovered
/// count, and again immediately before process-table construction as an
/// idempotent last check. It reserves live slots only; ownership remains a
/// separate reconciliation step.
pub(crate) fn recover_worker_topology(
    _control: &WorkerControl,
    state: &AppState,
    fleet: &claviron_orbit::Fleet,
) -> Result<u8> {
    let configured_worker_count = state.worker_count();
    let (worker_count, survivors) = effective_worker_count_from_presence(
        configured_worker_count,
        state.worker_capacity(),
        |id| latest_worker_presence(fleet, id),
    );
    if worker_count == configured_worker_count {
        return Ok(worker_count);
    }

    state
        .set_worker_count(WorkerCount::new(worker_count))
        .map_err(Error::service)?;
    warn!(
        configured_worker_count,
        effective_worker_count = worker_count,
        recovered_workers = survivors.len(),
        "live worker topology from the previous master exceeds configuration; preserving it for ownership reconciliation"
    );
    for presence in &survivors {
        warn!(
            worker_id = %presence.identity().worker_id(),
            pid = presence.identity().process().pid(),
            start_stamp = presence.identity().process().started_at(),
            previous_master_pid = presence.identity().master().process().pid(),
            previous_master_generation = presence.identity().master().generation(),
            parent_pid = presence.parent_pid(),
            "verified live worker reserves its existing slot; adoption still requires the ownership handshake"
        );
    }
    Ok(worker_count)
}

/// Spawn all worker processes
pub(crate) async fn spawn_all_workers(
    control: &WorkerControl,
    state: &AppState,
) -> Result<()> {
    let fleet = state.fleet().ok_or_else(|| {
        Error::service("Orbit fleet is unavailable for boot reconciliation")
    })?;
    let worker_count = crate::lifecycle::reconcile::recover_worker_topology(
        control, state, &fleet,
    )?;

    if worker_count == 0 {
        info!("👑 Master mode: No workers configured (running standalone)");
        return Ok(());
    }

    debug!("👑 Master mode: Spawning {} worker process(es)...", worker_count);

    let heartbeat_timeout = control.config().heartbeat.timeout;
    let mut workers = control.workers.write().await;

    // Invariant: WORKER_ID_NON_ZERO: Worker IDs are 1-based; worker #0 does not exist.
    for id in (1..=worker_count).map(WorkerId::new) {
        if workers.iter().any(|worker| worker.id == id) {
            continue;
        }
        // Typed presence wins over heartbeat age. A live exact process may
        // have missed enough pulses to make kind 221 stale; spawning before
        // inspecting its worker-written identity would overlap the slot.
        if let Some(presence) = latest_worker_presence(&fleet, id) {
            let lane_head = control.observer.lane_head(id).unwrap_or_default();
            debug!(
                proc_worker_id = %id,
                pid = presence.identity().process().pid(),
                start_stamp = presence.identity().process().started_at(),
                incarnation = presence.identity().incarnation(),
                lane_head,
                "worker slot #{id} not spawned: typed presence requires exact reconciliation"
            );
            workers.push(
                WorkerProcess::new(
                    id,
                    None,
                    WorkerPhase::Unclaimed {
                        watch: LaneWatch::starting_at(lane_head),
                    },
                    WorkerIntent::EnsureRunning,
                )
                .with_incarnation(0),
            );
            continue;
        }
        // A recent beat is not yet a reason to stand down — see
        // `reconcile_unclaimed_workers`, which decides by watching. Boot only
        // opens the watch; it never waits on it.
        if let Some(age) = control.observer.lane_beat_age(id, heartbeat_timeout)
        {
            let lane_head = control.observer.lane_head(id).unwrap_or_default();
            debug!(
                proc_worker_id = %id,
                last_beat_ms = age.as_millis(),
                lane_head,
                "worker slot #{id} not spawned yet: its node lane carries a recent \
                 heartbeat. Watching the lane to see whether anything is still publishing \
                 it before a second worker #{id} joins it"
            );
            workers.push(
                WorkerProcess::new(
                    id,
                    None,
                    WorkerPhase::Unclaimed {
                        watch: LaneWatch::starting_at(lane_head),
                    },
                    WorkerIntent::EnsureRunning,
                )
                .with_incarnation(0),
            );
            continue;
        }
        let worker = crate::process::spawn::spawn_worker(
            control,
            id,
            1,
            worker_count,
            state,
        )?;
        workers.push(worker);
    }

    Ok(())
}

/// How long a lane must hold still before this master takes its id — or
/// `None` when no slot is under watch, which is the ordinary boot and the
/// reason neither caller comes to depend on a loaded supervisor.yaml.
pub(crate) async fn unclaimed_watch_window(
    control: &WorkerControl
) -> Option<Duration> {
    control
        .workers
        .read()
        .await
        .iter()
        .any(|worker| matches!(worker.phase(), WorkerPhase::Unclaimed { .. }))
        .then(|| control.config().heartbeat.interval + LANE_WATCH_MARGIN)
}

/// When every slot now under watch will have been decided, one way or the
/// other. Boot waits for this rather than confirming a count it is still
/// in the middle of producing.
pub(crate) async fn unclaimed_watch_deadline(
    control: &WorkerControl
) -> Option<tokio::time::Instant> {
    Some(
        tokio::time::Instant::now()
            + crate::lifecycle::reconcile::unclaimed_watch_window(control)
                .await?,
    )
}

/// Resolve the slots boot could not spawn onto, and fill the ones that
/// turn out to be free. Costs one lane read each, waits for nothing, and
/// never touches the recovery budget: an unclaimed slot is not a failure,
/// it is a slot whose id may still be spoken for.
///
/// The lanes belong to this instance, so on any ordinary restart the beats
/// in them were written by this master's own previous generation. A recent
/// beat is therefore a suspicion, not a claim: the SHM segment outlives the
/// process, and a master restarted inside `heartbeat_timeout` finds every
/// slot apparently taken by the workers it had itself just stopped.
///
/// Age cannot separate that residue from a live orphan. Motion can: a
/// publisher that is still running advances its lane head within one
/// heartbeat interval, and residue never moves again. So the decision is
/// made by watching rather than by sleeping — each call compares the lane
/// against where it stood when the slot was last looked at, and the time
/// between calls is time the boot wait and the monitor tick were spending
/// anyway. A lane that has moved restarts its own watch; one that has held
/// still for longer than a heartbeat interval has nobody behind it.
pub(crate) async fn reconcile_unclaimed_workers(
    control: &WorkerControl,
    state: &AppState,
) -> Result<()> {
    let candidates = control
        .workers
        .read()
        .await
        .iter()
        .filter(|worker| worker.phase().is_unowned())
        .map(|worker| (worker.id, worker.phase()))
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return Ok(());
    }

    let fleet = state.fleet().ok_or_else(|| {
        Error::service("Orbit fleet is unavailable for boot reconciliation")
    })?;
    let master = control.master()?;
    let cfg = control.config();
    let max_age = cfg.heartbeat.timeout;
    let quiet_for = cfg.heartbeat.interval + LANE_WATCH_MARGIN;
    let now = OrbitEpoch::now();

    for (worker_id, phase) in candidates {
        let latest = latest_worker_presence(&fleet, worker_id);
        match phase {
            WorkerPhase::Unclaimed { watch } => {
                let Some(presence) = latest else {
                    let current = control
                        .observer
                        .lane_head(worker_id)
                        .unwrap_or(watch.head);
                    if current != watch.head {
                        let mut workers = control.workers.write().await;
                        if let Some(worker) =
                            workers.iter_mut().find(|worker| {
                                worker.id == worker_id
                                    && matches!(
                                        worker.phase(),
                                        WorkerPhase::Unclaimed { .. }
                                    )
                            })
                        {
                            worker.set_phase(WorkerPhase::Unclaimed {
                                watch: LaneWatch::starting_at(current),
                            });
                        }
                        continue;
                    }
                    if watch.since.elapsed() >= quiet_for {
                        crate::lifecycle::reconcile::spawn_reconciled_worker(
                            control, worker_id, 1, state,
                        )
                        .await?;
                    }
                    continue;
                };
                crate::lifecycle::reconcile::reconcile_presence(
                    control, worker_id, phase, presence, state,
                )
                .await?;
            }
            WorkerPhase::OwnershipPending { presence, requested_at } => {
                let Some(latest) = latest else {
                    if !presence.exact_process_alive() {
                        crate::lifecycle::reconcile::spawn_reconciled_worker(
                            control,
                            worker_id,
                            presence.identity().incarnation().saturating_add(1),
                            state,
                        )
                        .await?;
                    }
                    continue;
                };
                let previous = presence.identity();
                let observed = latest.identity();
                if observed.process() != previous.process()
                    || observed.incarnation() != previous.incarnation()
                {
                    crate::lifecycle::reconcile::block_reconciliation(
                        control,
                        worker_id,
                        phase,
                        latest,
                        WorkerBlockReason::IdentityChanged,
                    )
                    .await;
                    return Err(Error::service(format!(
                        "worker #{worker_id} PID {} changed identity while ownership was pending: expected start_stamp={} incarnation={}, observed PID {} start_stamp={} incarnation={}",
                        previous.process().pid(),
                        previous.process().started_at(),
                        previous.incarnation(),
                        observed.process().pid(),
                        observed.process().started_at(),
                        observed.incarnation()
                    )));
                }
                let known = master_identity_is_known(&fleet, observed.master());
                match assess_presence(latest, master, known, now, max_age) {
                    Ok(PresenceDecision::ReclaimDead) => {
                        crate::lifecycle::reconcile::spawn_reconciled_worker(
                            control,
                            worker_id,
                            previous.incarnation().saturating_add(1),
                            state,
                        )
                        .await?;
                    }
                    Ok(PresenceDecision::AdoptAccepted) => {
                        crate::lifecycle::reconcile::install_adopted_worker(
                            control, worker_id, observed, latest, state,
                        )
                        .await?;
                    }
                    Ok(PresenceDecision::Claim) => {
                        if requested_at.elapsed() >= cfg.heartbeat.interval {
                            crate::lifecycle::reconcile::publish_ownership_claim(
                                control, state, worker_id, presence, master
                            )
                            .await;
                        }
                    }
                    Err(refusal) => {
                        crate::lifecycle::reconcile::block_reconciliation(
                            control,
                            worker_id,
                            phase,
                            refusal.presence(),
                            refusal.block_reason(),
                        )
                        .await;
                        return Err(Error::service(refusal.to_string()));
                    }
                }
            }
            WorkerPhase::ReconciliationBlocked { presence, .. } => {
                let latest = latest.unwrap_or(presence);
                crate::lifecycle::reconcile::reconcile_presence(
                    control, worker_id, phase, latest, state,
                )
                .await?;
            }
            _ => {}
        }
    }
    Ok(())
}

pub(crate) async fn reconcile_presence(
    control: &WorkerControl,
    worker_id: WorkerId,
    expected_phase: WorkerPhase,
    presence: WorkerPresence,
    state: &AppState,
) -> Result<()> {
    let fleet = state.fleet().ok_or_else(|| {
        Error::service("Orbit fleet is unavailable for reconciliation")
    })?;
    let master = control.master()?;
    let known = master_identity_is_known(&fleet, presence.identity().master());
    let now = OrbitEpoch::now();
    let max_age = control.config().heartbeat.timeout;
    match assess_presence(presence, master, known, now, max_age) {
        Ok(PresenceDecision::ReclaimDead) => {
            crate::lifecycle::reconcile::spawn_reconciled_worker(
                control,
                worker_id,
                presence.identity().incarnation().saturating_add(1),
                state,
            )
            .await
        }
        Ok(PresenceDecision::AdoptAccepted) => {
            if crate::lifecycle::reconcile::install_adopted_worker(
                control,
                worker_id,
                presence.identity(),
                presence,
                state,
            )
            .await?
            {
                Ok(())
            } else {
                Err(Error::service(format!(
                    "worker #{worker_id} PID {} supplied accepted presence but its slot changed before adoption",
                    presence.identity().process().pid()
                )))
            }
        }
        Ok(PresenceDecision::Claim) => {
            let mut workers = control.workers.write().await;
            let Some(worker) =
                workers.iter_mut().find(|worker| worker.id == worker_id)
            else {
                return Err(Error::service(format!(
                    "worker #{worker_id} disappeared during boot reconciliation"
                )));
            };
            if worker.phase() != expected_phase || worker.process.is_some() {
                return Ok(());
            }
            worker.set_phase(WorkerPhase::OwnershipPending {
                presence,
                requested_at: Instant::now(),
            });
            drop(workers);
            state.events().emit(WorkerOwnershipClaim {
                target: presence.identity(),
                claimant: master,
            });
            info!(
                %worker_id,
                pid = presence.identity().process().pid(),
                old_master_pid = presence.identity().master().process().pid(),
                old_master_generation = presence.identity().master().generation(),
                new_master_pid = master.process().pid(),
                new_master_generation = master.generation(),
                "requested explicit ownership of live worker from dead master"
            );
            Ok(())
        }
        Err(refusal) => {
            crate::lifecycle::reconcile::block_reconciliation(
                control,
                worker_id,
                expected_phase,
                refusal.presence(),
                refusal.block_reason(),
            )
            .await;
            Err(Error::service(refusal.to_string()))
        }
    }
}

pub(crate) async fn block_reconciliation(
    control: &WorkerControl,
    worker_id: WorkerId,
    expected_phase: WorkerPhase,
    presence: WorkerPresence,
    reason: WorkerBlockReason,
) {
    let mut workers = control.workers.write().await;
    if let Some(worker) = workers.iter_mut().find(|worker| {
        worker.id == worker_id && worker.phase() == expected_phase
    }) {
        worker
            .set_phase(WorkerPhase::ReconciliationBlocked { presence, reason });
    }
}

pub(crate) async fn publish_ownership_claim(
    control: &WorkerControl,
    state: &AppState,
    worker_id: WorkerId,
    presence: WorkerPresence,
    master: MasterIdentity,
) {
    let mut workers = control.workers.write().await;
    let Some(worker) = workers.iter_mut().find(|worker| worker.id == worker_id)
    else {
        return;
    };
    if !worker.renew_ownership_claim() {
        return;
    }
    drop(workers);
    state.events().emit(WorkerOwnershipClaim {
        target: presence.identity(),
        claimant: master,
    });
}

pub(crate) async fn spawn_reconciled_worker(
    control: &WorkerControl,
    worker_id: WorkerId,
    incarnation: u64,
    state: &AppState,
) -> Result<()> {
    let mut workers = control.workers.write().await;
    let Some(slot) = workers
        .iter_mut()
        .find(|worker| worker.id == worker_id && worker.phase().is_unowned())
    else {
        return Ok(());
    };
    // Keep the slot locked across spawn and installation: an ownership ack
    // cannot win between those steps and leave the new child untracked.
    let spawned = crate::process::spawn::spawn_worker(
        control,
        worker_id,
        incarnation,
        state.worker_count(),
        state,
    )?;
    let pid = spawned.pid();
    *slot = spawned;
    control.changed.notify_waiters();
    trace!(
        %worker_id,
        pid,
        incarnation,
        "worker slot reclaimed only after the previous exact identity was proven dead"
    );
    Ok(())
}

/// Complete an ownership transfer only for the exact claim currently
/// pending on the slot. The worker has already rewritten its own identity;
/// this installs the non-child process handle in the authoritative table.
pub(crate) async fn ownership_accepted(
    control: &WorkerControl,
    accepted: WorkerOwnershipAccepted,
    state: &AppState,
) -> bool {
    if control.is_shutting_down() {
        return false;
    }
    let Ok(master) = control.master() else {
        return false;
    };
    if accepted.current != accepted.previous.with_master(master)
        || !accepted.current.process().is_alive()
    {
        return false;
    }
    let presence = {
        let workers = control.workers.read().await;
        let Some(worker) = workers
            .iter()
            .find(|worker| worker.id == accepted.current.worker_id())
        else {
            return false;
        };
        let WorkerPhase::OwnershipPending { presence, .. } = worker.phase()
        else {
            return false;
        };
        if presence.identity() != accepted.previous {
            return false;
        }
        presence
    };
    match crate::lifecycle::reconcile::install_adopted_worker(
        control,
        accepted.current.worker_id(),
        accepted.current,
        presence,
        state,
    )
    .await
    {
        Ok(adopted) => adopted,
        Err(error) => {
            warn!(
                worker_id = %accepted.current.worker_id(),
                pid = accepted.current.process().pid(),
                %error,
                "ownership acknowledgement could not install the adopted process handle"
            );
            false
        }
    }
}

/// [INCARNATION_MASKING]: retain the surviving process and slot; only the
/// explicitly accepted master ownership changes.
///
/// [INCARNATION_MASKING]: https://github.com/claviron/claviron/blob/main/docs/design/INCARNATION_MASKING.md
pub(crate) async fn install_adopted_worker(
    control: &WorkerControl,
    worker_id: WorkerId,
    accepted: WorkerIdentity,
    evidence: WorkerPresence,
    state: &AppState,
) -> Result<bool> {
    let master = control.master()?;
    if accepted != evidence.identity().with_master(master)
        || !accepted.process().is_alive()
    {
        return Ok(false);
    }
    let process = OwnedProcess::from_adopted(
        accepted.process(),
        evidence.runtime_started_at_unix_ms(),
        Arc::clone(&control.exited),
    )?
    .recover_orbit_on_exit(state, accepted.incarnation());
    let mut workers = control.workers.write().await;
    let Some(worker) = workers.iter_mut().find(|worker| worker.id == worker_id)
    else {
        return Ok(false);
    };
    if worker.phase() == WorkerPhase::Running
        && worker.identity(master).is_some_and(|identity| identity == accepted)
    {
        return Ok(true);
    }
    let matches_evidence = match worker.phase() {
        WorkerPhase::Unclaimed { .. } => evidence.identity() == accepted,
        WorkerPhase::OwnershipPending { presence, .. } => {
            presence.identity().process() == accepted.process()
                && presence.identity().incarnation() == accepted.incarnation()
        }
        WorkerPhase::ReconciliationBlocked { presence, .. } => {
            presence.identity().process() == accepted.process()
                && presence.identity().incarnation() == accepted.incarnation()
        }
        _ => false,
    };
    if !matches_evidence || worker.process.is_some() {
        return Ok(false);
    }
    worker.process = Some(process);
    worker.set_phase(WorkerPhase::Running);
    worker.set_intent(WorkerIntent::EnsureRunning);
    worker.incarnation = accepted.incarnation();
    drop(workers);
    control.observer.track_worker(worker_id, accepted.process().pid()).await;
    control.changed.notify_waiters();
    info!(
        %worker_id,
        pid = accepted.process().pid(),
        start_stamp = accepted.process().started_at(),
        incarnation = accepted.incarnation(),
        master_generation = accepted.master().generation(),
        runtime_started_at_unix_ms = evidence.runtime_started_at_unix_ms(),
        "adopted live worker after explicit ownership acceptance"
    );
    Ok(true)
}
