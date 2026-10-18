//! Periodic tasks for the PHP-FPM FCGI client.
//!
//! Reload-free by construction: every tick re-resolves `PhpFcgiService`
//! from the registry and snapshots its current `entries`, so pools that
//! were added / removed via `Reloadable::reload` are picked up on the
//! next sweep without any signaling between the service and the task.
//!
//! The task carries its own entry rather than needing a line in
//! `tasks.yaml`, and takes its cadence from `fpm.yaml` — the file that
//! also holds the `idle_timeout` the cadence has to stay under. See
//! [`crate::config::HousekeepConfig`].

use claviron_runtime::AppState;
use claviron_tasks::service::TaskService;
use claviron_tasks::{TaskConfig, TaskResult};
use futures::stream::{FuturesUnordered, StreamExt};
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn};

use crate::config::HousekeepConfig;

pub(crate) const TASK: &str = "fcgi_housekeep";

/// Install the sweep as a builtin task, with the cadence `fpm.yaml` asks
/// for.
///
/// Registration happens from `PhpFcgiService::configure`, with the cadence
/// Congif prepared: after every provider is registered and before
/// `TaskService` boots and folds builtin entries into the task table — a
/// registration after that point would be ignored, which is also why a
/// later change of the cadence takes effect with the next start.
///
/// A line under this name in `tasks.yaml` still replaces the entry whole,
/// which is the operator's override and the only reason to write one.
pub(crate) fn register(
    state: &AppState,
    housekeep: &HousekeepConfig,
) -> Result<(), String> {
    let Some(tasks) = state.registry_ref().resolve::<TaskService>() else {
        // A build with the FCGI runner but no task service is a valid
        // composition; there is nothing to install into.
        return Ok(());
    };

    let entry = format!(
        "timing: {{ kind: interval, enabled: {}, every: {}ms, skip_on_boot: true }}\n\
         scope: workers\nattempts: 1\nsilent: true\n",
        housekeep.enabled,
        housekeep.interval.as_millis()
    );
    let defaults =
        TaskConfig::from_yaml(&entry).map_err(|e| format!("{TASK}: {e}"))?;
    debug!(
        "{TASK}: sweeping every {:?} (enabled={})",
        housekeep.interval, housekeep.enabled
    );

    tasks.register_builtin(
        TASK,
        defaults,
        |state: AppState, token: CancellationToken| {
            fcgi_housekeep(state, token)
        },
    );
    Ok(())
}

use crate::pool::FcgiPool;
#[cfg(debug_assertions)]
use crate::pool::PoolStats;
use crate::service::PhpFcgiService;

/// Periodic kill-sweep over every Pooled entry in `PhpFcgiService`.
///
/// The actual algorithm lives in `FcgiPool::housekeeping` (single source
/// of truth, shared with `acquire`'s own staleness primitive). This task
/// just dispatches one sweep per pool, in parallel, on each tick.
///
/// Reload-free: every tick re-resolves `PhpFcgiService` from the registry
/// and snapshots its pooled entries. Pools added or removed by a recent
/// `Reloadable::reload` are picked up on the next tick — no signaling
/// between the service and this task.
///
/// `scope = "workers"` runs in worker processes and standalone (master is
/// excluded — it clears `entries` in boot, so there's nothing to sweep).
pub async fn fcgi_housekeep(
    state: AppState,
    _token: CancellationToken,
) -> TaskResult<()> {
    let Some(php) = state.registry_ref().resolve::<PhpFcgiService>() else {
        trace!("fcgi_housekeep: PhpFcgiService not registered; skipping");
        return Ok(());
    };

    let pools = php.pooled_entries().await;
    if pools.is_empty() {
        return Ok(());
    }

    // Sweeps are pure CPU + atomics (microsecond bound), but go through
    // `spawn_blocking` so the std::Mutex held briefly inside doesn't
    // park a tokio worker thread under load.
    let mut sweeps = FuturesUnordered::new();
    for (name, pool) in pools {
        sweeps
            .push(tokio::task::spawn_blocking(move || sweep_pool(name, pool)));
    }

    while let Some(res) = sweeps.next().await {
        handle_sweep_result(res);
    }

    Ok(())
}

#[cfg(debug_assertions)]
fn sweep_pool(
    name: String,
    pool: FcgiPool,
) -> (String, PoolStats) {
    pool.housekeeping();
    (name, pool.stats())
}

#[cfg(not(debug_assertions))]
fn sweep_pool(
    _name: String,
    pool: FcgiPool,
) {
    pool.housekeeping();
}

#[cfg(debug_assertions)]
fn handle_sweep_result(
    res: Result<(String, PoolStats), tokio::task::JoinError>
) {
    match res {
        Ok((name, stats)) => {
            if stats.live == 0 && stats.idle == 0 {
                trace!("fcgi_housekeep tick pool={name} live=0 idle=0");
            } else {
                trace!(
                    "fcgi_housekeep tick pool={name} live={} idle={}",
                    stats.live, stats.idle
                );
            }
        }
        Err(e) => warn!("fcgi_housekeep sweep panicked: {e}"),
    }
}

#[cfg(not(debug_assertions))]
fn handle_sweep_result(res: Result<(), tokio::task::JoinError>) {
    if let Err(e) = res {
        warn!("fcgi_housekeep sweep panicked: {e}");
    }
}
