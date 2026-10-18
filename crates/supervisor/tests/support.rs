use std::time::Duration;

use claviron_runtime::WorkerId;
use claviron_sys::process::ProcessIdentity;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::lifecycle::*;
use crate::manager::WorkerCommand;
use crate::shutdown::{WorkerDrainAcknowledged, WorkerTeardownCompleted};
use crate::snapshot::WorkerListSnapshot;
use crate::startup::WorkerStarted;

impl WorkerControl {
    /// This process as the master, generation one, for tests that never boot.
    #[cfg(test)]
    pub(crate) fn establish_master_for_tests(&self) -> MasterIdentity {
        let process = claviron_sys::process::identity(std::process::id())
            .expect("own process identity");
        let master = MasterIdentity::new(process, 1);
        let _ = self.master.set(master);
        master
    }
    #[cfg(test)]
    pub(crate) fn set_master_for_tests(
        &self,
        master: MasterIdentity,
    ) {
        self.master
            .set(master)
            .expect("master identity already established in test");
    }

    #[cfg(test)]
    pub(crate) async fn check_worker_health(
        &self
    ) -> (Vec<WorkerProcessSnapshot>, Vec<WorkerId>) {
        crate::process::health::check_worker_health(self).await
    }
    #[cfg(test)]
    pub(crate) fn recover_worker_topology(
        &self,
        state: &AppState,
        fleet: &claviron_orbit::Fleet,
    ) -> Result<u8> {
        crate::lifecycle::reconcile::recover_worker_topology(self, state, fleet)
    }

    #[cfg(test)]
    pub(crate) async fn drain_acknowledged(
        &self,
        event: &WorkerDrainAcknowledged,
    ) -> bool {
        crate::lifecycle::drain::drain_acknowledged(self, event).await
    }
    #[cfg(test)]
    pub(crate) async fn teardown_completed(
        &self,
        event: &WorkerTeardownCompleted,
    ) -> bool {
        crate::lifecycle::drain::teardown_completed(self, event).await
    }

    #[cfg(test)]
    pub(crate) async fn blocked_workers(&self) -> Vec<BlockedWorker> {
        crate::lifecycle::drain::blocked_workers(self).await
    }

    #[cfg(test)]
    pub(crate) async fn shutdown_workers_after_drain_requests(
        &self,
        state: &AppState,
    ) {
        crate::lifecycle::drain::shutdown_workers_after_drain_requests(
            self, state,
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn await_slot_exit(
        &self,
        worker_id: WorkerId,
        identity: ProcessIdentity,
        operation: &'static str,
    ) -> Result<ExitOutcome> {
        crate::lifecycle::drain::await_slot_exit(
            self, worker_id, identity, operation,
        )
        .await
    }
    #[cfg(test)]
    pub(crate) async fn drain_stale_worker(
        &self,
        state: &AppState,
        worker_id: WorkerId,
        expected_pid: u32,
        heartbeat_timeout: Duration,
        operation: OperationId,
    ) -> Result<()> {
        crate::lifecycle::recovery::drain_stale_worker(
            self,
            state,
            worker_id,
            expected_pid,
            heartbeat_timeout,
            operation,
        )
        .await
    }

    #[cfg(test)]
    pub(crate) fn submit_manager_command(
        self: &Arc<Self>,
        state: &AppState,
        command: WorkerCommand,
    ) -> SubmitResult {
        crate::management::submit::submit_manager_command(self, state, command)
    }

    #[cfg(test)]
    pub(crate) async fn stop_worker(
        &self,
        worker_id: WorkerId,
        state: &AppState,
        operation: OperationId,
    ) -> Result<()> {
        crate::management::actions::worker::stop_worker(
            self, worker_id, state, operation,
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn management_snapshot(
        &self,
        state: &AppState,
    ) -> WorkerListSnapshot {
        crate::observation::project::management_snapshot(self, state).await
    }

    #[cfg(test)]
    pub(crate) async fn wait_workers_started(
        &self,
        state: &AppState,
        started: &mut broadcast::Receiver<WorkerStarted>,
        cancellation: &CancellationToken,
        timeout: Duration,
    ) -> Result<()> {
        let wait = crate::lifecycle::startup::wait_workers_started(
            self,
            state,
            cancellation,
            timeout,
        );
        tokio::pin!(wait);
        loop {
            tokio::select! {
                result = &mut wait => return result,
                event = started.recv() => {
                    crate::lifecycle::startup::confirm_worker_started(self, crate::startup::received(event)?).await;
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) async fn confirm_worker_started(
        &self,
        event: WorkerStarted,
    ) -> bool {
        crate::lifecycle::startup::confirm_worker_started(self, event).await
    }

    #[cfg(test)]
    pub(crate) async fn resize_workers(
        &self,
        state: &AppState,
        expected_worker_count: u8,
        target_worker_count: u8,
        operation: OperationId,
    ) -> Result<()> {
        crate::management::actions::resize::resize_workers(
            self,
            state,
            expected_worker_count,
            target_worker_count,
            operation,
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn validate_resize_table(
        &self,
        worker_count: u8,
    ) -> Result<()> {
        crate::management::actions::resize::validate_resize_table(
            self,
            worker_count,
        )
        .await
    }
}
