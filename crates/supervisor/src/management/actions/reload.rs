use claviron_runtime::{AppState, WorkerId};
use tracing::{info, warn};

use crate::control::WorkerControl;
use crate::lifecycle::WorkerPhase;
use crate::{Error, Result, reload};

pub(crate) async fn reload_worker(
    control: &WorkerControl,
    worker_id: WorkerId,
    state: &AppState,
) -> Result<()> {
    {
        let mut workers = control.workers.write().await;
        let worker = workers
            .iter_mut()
            .find(|worker| worker.id == worker_id)
            .ok_or_else(|| {
            Error::service(format!("Worker #{worker_id} not found"))
        })?;
        if !worker.is_running() {
            return Err(Error::service(format!(
                "Worker #{worker_id} is {}, not running",
                worker.lifecycle().as_str()
            )));
        }
        worker.set_phase(WorkerPhase::Reloading);
    }

    let mut result = reload::reload_worker(
        state,
        worker_id,
        control.config().reload.timeout,
    )
    .await;
    let mut workers = control.workers.write().await;
    if let Some(worker) =
        workers.iter_mut().find(|worker| worker.id == worker_id)
    {
        let Some(process) = worker.process.as_mut() else {
            worker.mark_failed();
            result = Err(format!(
                "Worker #{worker_id} lost its process during reload"
            ));
            return result.map_err(Error::service);
        };
        match process.try_reap() {
            Ok(None) => {
                if worker.phase() == WorkerPhase::Reloading {
                    worker.set_phase(WorkerPhase::Running);
                }
                if result.is_ok() {
                    worker.reload_revision =
                        worker.reload_revision.saturating_add(1);
                }
            }
            Ok(Some(status)) => {
                worker.record_exit();
                result = Err(format!(
                    "Worker #{worker_id} exited during reload with status: {status}"
                ));
            }
            Err(error) => {
                worker.record_exit();
                result = Err(format!(
                    "failed to inspect Worker #{worker_id} after reload: {error}"
                ));
            }
        }
    }
    result.map_err(Error::service)
}

pub(crate) async fn record_reload_revisions(
    control: &WorkerControl,
    worker_ids: &[WorkerId],
) {
    let mut workers = control.workers.write().await;
    for worker in
        workers.iter_mut().filter(|worker| worker_ids.contains(&worker.id))
    {
        worker.reload_revision = worker.reload_revision.saturating_add(1);
    }
}

pub(crate) async fn reload_all_workers(
    control: &WorkerControl,
    state: &AppState,
) -> Result<()> {
    let worker_ids = {
        let mut workers = control.workers.write().await;
        let worker_ids = workers
            .iter()
            .filter(|worker| worker.is_running())
            .map(|worker| worker.id)
            .collect::<Vec<_>>();
        for worker in
            workers.iter_mut().filter(|worker| worker_ids.contains(&worker.id))
        {
            worker.set_phase(WorkerPhase::Reloading);
        }
        worker_ids
    };
    if worker_ids.is_empty() {
        return Err(Error::service(
            "no running workers are available to reload",
        ));
    }

    let outcome = reload::reload_workers(
        state,
        &worker_ids,
        Vec::new(),
        control.config().reload.timeout,
    )
    .await;
    let (reloaded, timed_out, unavailable, coordination_error) = match outcome {
        Ok(reload::ReloadOutcome::Complete { reloaded }) => {
            (reloaded, Vec::new(), Vec::new(), None)
        }
        Ok(reload::ReloadOutcome::Partial {
            reloaded,
            timed_out,
            unavailable,
        }) => (reloaded, timed_out, unavailable, None),
        Err(error) => (Vec::new(), Vec::new(), Vec::new(), Some(error)),
    };

    let mut workers = control.workers.write().await;
    let mut process_errors = Vec::new();
    for worker in
        workers.iter_mut().filter(|worker| worker_ids.contains(&worker.id))
    {
        let Some(process) = worker.process.as_mut() else {
            worker.mark_failed();
            process_errors.push(format!(
                "Worker #{} lost its process during reload",
                worker.id
            ));
            continue;
        };
        match process.try_reap() {
            Ok(None) => {
                if worker.phase() == WorkerPhase::Reloading {
                    worker.set_phase(WorkerPhase::Running);
                }
                if reloaded.contains(&worker.id) {
                    worker.reload_revision =
                        worker.reload_revision.saturating_add(1);
                }
            }
            Ok(Some(status)) => {
                worker.record_exit();
                process_errors.push(format!(
                    "Worker #{} exited during reload with status: {status}",
                    worker.id
                ));
            }
            Err(error) => {
                worker.record_exit();
                process_errors.push(format!(
                    "failed to inspect Worker #{} after reload: {error}",
                    worker.id
                ));
            }
        }
    }
    drop(workers);

    let mut errors = Vec::new();
    if let Some(error) = coordination_error {
        errors.push(error);
    }
    if !timed_out.is_empty() {
        errors.push(format!(
            "worker reload timed out for worker ids: {timed_out:?}"
        ));
    }
    if !unavailable.is_empty() {
        errors.push(format!("workers unavailable for reload: {unavailable:?}"));
    }
    errors.extend(process_errors);
    if errors.is_empty() {
        Ok(())
    } else {
        Err(Error::service(errors.join("; ")))
    }
}

/// Request worker config reload through Orbit events.
/// Only reloads workers with ID <= workers_to_reload (excludes newly spawned workers)
/// Returns true if all workers are still alive and successfully reloaded
pub(crate) async fn reload_workers(
    control: &WorkerControl,
    state: &AppState,
    workers_to_reload: u8,
) -> std::result::Result<reload::ReloadOutcome, String> {
    if workers_to_reload == 0 {
        return Ok(reload::ReloadOutcome::Complete { reloaded: Vec::new() });
    }

    let worker_ids = {
        let workers = control.workers.read().await;
        workers
            .iter()
            .filter(|worker| {
                worker.id.raw() <= workers_to_reload && worker.is_running()
            })
            .map(|worker| worker.id)
            .collect::<Vec<_>>()
    };

    let unavailable = (1..=workers_to_reload)
        .map(WorkerId::new)
        .filter(|worker_id| !worker_ids.contains(worker_id))
        .collect::<Vec<_>>();
    if !unavailable.is_empty() {
        warn!(
            expected = workers_to_reload,
            active = worker_ids.len(),
            ?unavailable,
            "worker reload will skip inactive or missing workers"
        );
    }

    if !worker_ids.is_empty() {
        info!(
            "🔄 Publishing one reload event for {} active worker(s)...",
            worker_ids.len()
        );
    }
    reload::reload_workers(
        state,
        &worker_ids,
        unavailable,
        control.config().reload.timeout,
    )
    .await
}
