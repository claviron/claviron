#[cfg(feature = "metrics")]
use std::sync::atomic::Ordering;

#[cfg(feature = "metrics")]
use claviron_runtime::AppState;

use crate::control::WorkerControl;

#[cfg(feature = "metrics")]
pub(crate) async fn capture_metrics(
    control: &WorkerControl,
    state: &AppState,
) -> crate::metrics::ProcessMetricsSnapshot {
    let configured = state.worker_count();
    let alive = control
        .workers
        .read()
        .await
        .iter()
        .filter(|worker| worker.is_capacity())
        .count()
        .min(usize::from(u8::MAX)) as u8;

    crate::metrics::ProcessMetricsSnapshot::master(
        control.metrics_started_at,
        control.worker_restarts_total.load(Ordering::Relaxed),
        control.worker_rotations_total.load(Ordering::Relaxed),
        configured,
        alive,
    )
}

pub(crate) fn record_worker_restart(_control: &WorkerControl) {
    #[cfg(feature = "metrics")]
    _control.worker_restarts_total.fetch_add(1, Ordering::Relaxed);
}
