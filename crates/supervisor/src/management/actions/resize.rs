//! Logical fleet-size changes within immutable Orbit capacity.
//!
//! Growth boot-confirms only the added slot; shrink gracefully retires only
//! the highest configured slot. Neither operation replaces retained workers.

use claviron_runtime::{AppState, WorkerCount, WorkerId};
use tracing::info;

use crate::control::WorkerControl;
use crate::lifecycle::{
    DrainReason, ExitDisposition, OperationId, WorkerIntent,
};
use crate::{Error, Result};

pub(crate) async fn resize_workers(
    control: &WorkerControl,
    state: &AppState,
    expected_worker_count: u8,
    target_worker_count: u8,
    operation: OperationId,
) -> Result<()> {
    let current_worker_count = state.worker_count();
    if current_worker_count != expected_worker_count {
        return Err(Error::service(format!(
            "fleet size changed before resize was applied: expected {expected_worker_count}, current {current_worker_count}"
        )));
    }
    if expected_worker_count.abs_diff(target_worker_count) != 1
        || target_worker_count == 0
    {
        return Err(Error::service(format!(
            "fleet resize must change a non-zero worker count by one: {expected_worker_count} -> {target_worker_count}"
        )));
    }
    crate::management::actions::resize::validate_resize_table(
        control,
        current_worker_count,
    )
    .await?;

    if target_worker_count > current_worker_count {
        if target_worker_count > state.worker_capacity() {
            return Err(Error::service(format!(
                "fleet worker capacity {} cannot admit worker #{target_worker_count}",
                state.worker_capacity()
            )));
        }
        crate::management::actions::resize::grow_fleet(
            control,
            state,
            current_worker_count,
            target_worker_count,
            operation,
        )
        .await
    } else {
        crate::management::actions::resize::shrink_fleet(
            control,
            state,
            current_worker_count,
            target_worker_count,
            operation,
        )
        .await
    }
}

pub(crate) async fn grow_fleet(
    control: &WorkerControl,
    state: &AppState,
    previous_worker_count: u8,
    target_worker_count: u8,
    operation: OperationId,
) -> Result<()> {
    let new_id = WorkerId::new(target_worker_count);
    // A slot removed earlier keeps its counters as a tombstone, so the
    // returning id continues its incarnation sequence instead of restarting it.
    let (incarnation, reload_revision) = {
        let workers = control.workers.read().await;
        workers
            .iter()
            .find(|worker| worker.id == new_id)
            .map(|worker| {
                if !worker.is_stopped() {
                    return Err(Error::service(format!(
                        "unconfigured worker #{target_worker_count} is {}, not stopped",
                        worker.lifecycle().as_str()
                    )));
                }
                Ok((worker.incarnation.saturating_add(1), worker.reload_revision))
            })
            .transpose()?
            .unwrap_or((1, 0))
    };
    let mut workers = control.workers.write().await;
    let mut new_worker = crate::process::spawn::spawn_worker(
        control,
        new_id,
        incarnation,
        target_worker_count,
        state,
    )?;
    new_worker.reload_revision = reload_revision;
    let new_pid = new_worker.pid().expect("new worker has no child handle");
    {
        if let Some(index) =
            workers.iter().position(|worker| worker.id == new_id)
        {
            workers[index] = new_worker;
        } else {
            workers.push(new_worker);
            workers.sort_unstable_by_key(|worker| worker.id);
        }
    }
    drop(workers);
    control.changed.notify_waiters();

    confirm_added_worker(
        control,
        state,
        previous_worker_count,
        target_worker_count,
        new_pid,
        operation,
    )
    .await
}

/// Commit growth only after the added process completes boot. Retained slots
/// are not restart targets; their bootstrap counts describe their birth.
async fn confirm_added_worker(
    control: &WorkerControl,
    state: &AppState,
    previous_worker_count: u8,
    target_worker_count: u8,
    new_pid: u32,
    operation: OperationId,
) -> Result<()> {
    let new_id = WorkerId::new(target_worker_count);
    if let Err(error) =
        crate::management::actions::restart::wait_for_replacement(
            control, state, new_id, new_pid,
        )
        .await
    {
        if let Err(cleanup) =
            crate::management::actions::resize::rollback_added_worker(
                control, state, new_id, operation,
            )
            .await
        {
            return Err(Error::service(format!(
                "{error}; failed to roll back worker #{target_worker_count}: {cleanup}"
            )));
        }
        return Err(error);
    }

    state
        .set_worker_count(WorkerCount::new(target_worker_count))
        .map_err(Error::service)?;

    info!(
        previous_worker_count,
        target_worker_count,
        added_worker_id = %new_id,
        pid = new_pid,
        "worker fleet growth completed without replacing retained workers"
    );
    Ok(())
}

pub(crate) async fn shrink_fleet(
    control: &WorkerControl,
    state: &AppState,
    previous_worker_count: u8,
    target_worker_count: u8,
    operation: OperationId,
) -> Result<()> {
    let running = control
        .workers
        .read()
        .await
        .iter()
        .filter(|worker| {
            worker.id.raw() <= previous_worker_count && worker.is_running()
        })
        .count();
    if running < 2 {
        return Err(Error::service(format!(
            "FLEET_CONTINUITY requires at least two running workers before shrinking; found {running}"
        )));
    }

    info!(
        previous_worker_count,
        target_worker_count,
        retiring_worker_id = previous_worker_count,
        "shrinking worker fleet without replacing retained workers"
    );
    crate::management::actions::resize::retire_worker_slot(
        control,
        state,
        WorkerId::new(previous_worker_count),
        operation,
    )
    .await?;
    state
        .set_worker_count(WorkerCount::new(target_worker_count))
        .map_err(Error::service)?;
    info!(target_worker_count, "worker fleet shrink completed");
    Ok(())
}

pub(crate) async fn validate_resize_table(
    control: &WorkerControl,
    worker_count: u8,
) -> Result<()> {
    let workers = control.workers.read().await;
    for worker_id in (1..=worker_count).map(WorkerId::new) {
        let worker =
            workers.iter().find(|worker| worker.id == worker_id).ok_or_else(
                || Error::service(format!("worker #{worker_id} is missing")),
            )?;
        if !worker.is_running() && !worker.is_stopped() {
            return Err(Error::service(format!(
                "fleet resize requires a stable fleet; worker #{worker_id} is {}",
                worker.lifecycle().as_str()
            )));
        }
    }
    if let Some(worker) = workers
        .iter()
        .find(|worker| worker.id.raw() > worker_count && !worker.is_stopped())
    {
        return Err(Error::service(format!(
            "unconfigured worker #{} is {}, not stopped",
            worker.id,
            worker.lifecycle().as_str()
        )));
    }
    Ok(())
}

pub(crate) async fn rollback_added_worker(
    control: &WorkerControl,
    state: &AppState,
    worker_id: WorkerId,
    operation: OperationId,
) -> Result<()> {
    let (identity, previous_intent) = {
        let mut workers = control.workers.write().await;
        let worker = workers
            .iter_mut()
            .find(|worker| worker.id == worker_id)
            .ok_or_else(|| {
            Error::service(format!("worker #{worker_id} disappeared"))
        })?;
        let identity = worker
            .process
            .as_ref()
            .ok_or_else(|| {
                Error::service(format!("worker #{worker_id} has no process"))
            })?
            .identity();
        let previous_intent = worker
            .begin_drain(DrainReason::ScaleDown, ExitDisposition::StayStopped);
        if let Err(error) = crate::lifecycle::drain::request_drain(
            control,
            state,
            worker,
            operation,
            DrainReason::ScaleDown,
        ) {
            worker.set_intent(previous_intent);
            return Err(error);
        }
        (identity, previous_intent)
    };
    if let Err(error) = crate::lifecycle::drain::await_slot_exit(
        control,
        worker_id,
        identity,
        "add rollback",
    )
    .await
    {
        crate::lifecycle::drain::restore_intent(
            control,
            worker_id,
            previous_intent,
        )
        .await;
        return Err(error);
    }
    Ok(())
}

pub(crate) async fn retire_worker_slot(
    control: &WorkerControl,
    state: &AppState,
    worker_id: WorkerId,
    operation: OperationId,
) -> Result<()> {
    let identity = {
        let mut workers = control.workers.write().await;
        let worker = workers
            .iter_mut()
            .find(|worker| worker.id == worker_id)
            .ok_or_else(|| {
            Error::service(format!("worker #{worker_id} disappeared"))
        })?;
        if worker.is_stopped() {
            return Ok(());
        }
        if !worker.is_running() {
            return Err(Error::service(format!(
                "worker #{worker_id} became {} before removal",
                worker.lifecycle().as_str()
            )));
        }
        let identity = worker
            .process
            .as_ref()
            .ok_or_else(|| {
                Error::service(format!("worker #{worker_id} has no process"))
            })?
            .identity();
        worker.begin_drain(DrainReason::ScaleDown, ExitDisposition::RemoveSlot);
        if let Err(error) = crate::lifecycle::drain::request_drain(
            control,
            state,
            worker,
            operation,
            DrainReason::ScaleDown,
        ) {
            worker.set_intent(WorkerIntent::EnsureRunning);
            return Err(error);
        }
        info!(%worker_id, pid = identity.pid(), "draining worker removed from fleet");
        identity
    };
    if let Err(error) = crate::lifecycle::drain::await_slot_exit(
        control,
        worker_id,
        identity,
        "fleet shrink",
    )
    .await
    {
        crate::lifecycle::drain::restore_intent(
            control,
            worker_id,
            WorkerIntent::EnsureRunning,
        )
        .await;
        return Err(error);
    }
    Ok(())
}

#[cfg(test)]
#[path = "../../../tests/management/actions/resize.rs"]
mod tests;
