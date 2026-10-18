use std::sync::Arc;
use std::time::Duration;

use claviron_runtime::AppState;
use tracing::{debug, info, trace, warn};

use crate::control::WorkerControl;
use crate::{Error, Result};

/// Monitor workers and restart failed ones
/// This runs as a runnable provider task on master.
pub(crate) async fn monitor_workers(
    control: &Arc<WorkerControl>,
    state: AppState,
    cancellation: tokio_util::sync::CancellationToken,
) -> Result<()> {
    let mut monitor_interval = control.config().monitor.interval;
    // First tick fires after one full period so worker boot and the first
    // Orbit heartbeat complete before process policy is evaluated.
    let mut interval = tokio::time::interval_at(
        tokio::time::Instant::now() + monitor_interval,
        monitor_interval,
    );
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    info!(
        "👁️ Supervisor worker monitor started (interval: {:?})",
        monitor_interval
    );

    loop {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                debug!("🔸 WorkerMonitor stopped: service cancellation requested");
                break Ok(());
            }
            _ = interval.tick() => {}
            // A watched worker died. The tick would have found it too,
            // eventually; this is the difference between "eventually" and
            // "now" at N-1 capacity.
            () = control.exited.notified() => {}
        }

        if cancellation.is_cancelled() {
            debug!("🔸 WorkerMonitor stopped: service cancellation requested");
            break Ok(());
        }
        if control.is_shutting_down() {
            info!("🔸 WorkerMonitor stopped: worker fleet is shutting down");
            break Ok(());
        }

        // Initial convergence owns early exits. Monitoring must not race it
        // by starting crash recovery before initial boot is confirmed.
        if !control.startup_completed.load(std::sync::atomic::Ordering::Acquire)
        {
            continue;
        }

        // Check worker health
        // Before looking at what died, see whether a slot boot could not
        // claim has become claimable. Cheap — one lane read per pending
        // slot, and only while any are pending.
        let current_interval = control.config().monitor.interval;
        if current_interval != monitor_interval {
            monitor_interval = current_interval;
            interval = tokio::time::interval_at(
                tokio::time::Instant::now() + monitor_interval,
                monitor_interval,
            );
            interval.set_missed_tick_behavior(
                tokio::time::MissedTickBehavior::Skip,
            );
        }

        let (_alive, dead) =
            crate::process::health::check_worker_health(control).await;

        if !dead.is_empty() {
            // Get current worker count to avoid restarting workers that were intentionally scaled down
            let current_worker_count = state.worker_count();

            // Filter dead workers: only restart if worker_id <= current_worker_count
            let mut should_restart = Vec::new();
            let mut should_ignore = Vec::new();

            for worker_id in dead {
                if worker_id.raw() <= current_worker_count {
                    should_restart.push(worker_id);
                } else {
                    should_ignore.push(worker_id);
                }
            }

            if !should_ignore.is_empty() {
                debug!(
                    "🔍 Ignoring {} scaled-down worker(s): {:?}",
                    should_ignore.len(),
                    should_ignore
                );
            }

            if !should_restart.is_empty() {
                // Every configured slot dead and none with budget left is
                // the state the outer service manager must see: the
                // runnable fails instead of idling on a fleet it cannot
                // rebuild. Decided from the table, before any recovery
                // task is started for this tick.
                if _alive.is_empty()
                    && crate::lifecycle::recovery::recovery_exhausted(
                        control,
                        &should_restart,
                    )
                    .await
                {
                    break Err(Error::service(format!(
                        "all workers exhausted after restart failures; no alive workers remain: {should_restart:?}"
                    )));
                }
                warn!(
                    "🔍 Detected {} dead worker(s) that need restart: {:?}",
                    should_restart.len(),
                    should_restart
                );
                for worker_id in should_restart {
                    // One recovery per slot at a time; a tick that finds the
                    // same slot dead while its recovery is still backing off
                    // joins that recovery instead of starting another.
                    crate::management::submit::submit_crash_recovery(
                        control, worker_id, &state,
                    );
                }
            }
        } else {
            // #[cfg(debug_assertions)]
            // debug!(
            //     target: "claviron::supervisor",
            //     "🧬 {} worker(s) running: {:?}",
            //     _alive.len(),
            //     _alive
            // );

            // Hung detection via Orbit heartbeat.
            // Checking stale workers
            // Add a small skew to avoid race at the exact timeout boundary
            let heartbeat_timeout = control.config().heartbeat.timeout;
            // Jitter: max(750ms, 1/2 monitor interval)

            let mut stale =
                control.observer.stale_workers(heartbeat_timeout).await;
            if !stale.is_empty() {
                // Re-check once after a short delay to filter clock/latency blips
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => {
                        debug!("🔸 WorkerMonitor stopped: service cancellation requested");
                        return Ok(());
                    },
                    _ = tokio::time::sleep(Duration::from_millis(200)) => {},
                }
                stale = control.observer.stale_workers(heartbeat_timeout).await;
            }

            if !stale.is_empty() {
                for (id, pid) in stale {
                    // The previous incarnation may still have a liveness entry
                    // while its replacement is waiting for boot confirmation;
                    // `drain_stale_worker` matches the exact PID. The exit is
                    // recorded as a failure and the next health pass
                    // recovers the slot.
                    crate::management::submit::submit_heartbeat_recovery(
                        control,
                        &state,
                        id,
                        pid,
                        heartbeat_timeout,
                    );
                }
            } else {
                trace!("✅ No stale or dead workers detected");
            }

            let cfg = control.config();
            let rss_threshold = cfg.rotation.rss_threshold;
            let max_age = cfg.rotation.max_age;
            if rss_threshold.is_some() || max_age.is_some() {
                let workers = control.workers.read().await;
                let candidates = workers
                    .iter()
                    .filter(|worker| worker.is_running())
                    .filter_map(|worker| {
                        worker.process.as_ref().map(|process| {
                            (worker.id, process.pid(), process.age())
                        })
                    })
                    .collect::<Vec<_>>();
                drop(workers);

                for (worker_id, pid, age) in candidates {
                    let rss_bytes = claviron_sys::rss::read_rss(Some(pid));
                    let rotation_reason = rss_threshold
                        .as_ref()
                        .and_then(|threshold| {
                            rss_bytes
                                .filter(|rss| *rss > threshold.as_u64())
                                .map(|rss| {
                                    format!(
                                        "RSS {} > {}",
                                        byte_unit::Byte::from_u64(rss)
                                            .get_appropriate_unit(
                                                byte_unit::UnitType::Binary
                                            ),
                                        threshold.get_appropriate_unit(
                                            byte_unit::UnitType::Binary
                                        )
                                    )
                                })
                        })
                        .or_else(|| {
                            max_age.filter(|limit| age >= *limit).map(|limit| {
                                format!(
                                    "age {} >= {}",
                                    humantime::format_duration(age),
                                    humantime::format_duration(limit)
                                )
                            })
                        });

                    if rss_threshold.is_some() && rss_bytes.is_none() {
                        warn!(
                            "⚠️  Supervisor could not read RSS for worker #{} (PID {})",
                            worker_id, pid
                        );
                    }

                    if let Some(reason) = rotation_reason {
                        info!(
                            "🔄 Worker #{} rotation triggered: {}",
                            worker_id, reason
                        );
                        crate::management::submit::submit_rotation(
                            control, worker_id, &state,
                        );
                    }
                }
            }
        }
    }
}
