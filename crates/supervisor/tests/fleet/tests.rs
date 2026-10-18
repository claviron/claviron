use super::*;
/// A blocked slot's presence evidence refreshes on every worker heartbeat, but
/// the block has stood since it was first refused: the phase clock follows the
/// published phase and reason, not the evidence.
#[tokio::test]
async fn blocked_phase_keeps_its_clock_when_only_the_evidence_changes() {
    use claviron_orbit::OrbitEpoch;
    use claviron_sys::process::ProcessIdentity;

    use crate::lifecycle::{MasterIdentity, WorkerBlockReason, WorkerIdentity};
    use crate::presence::{PresencePhase, WorkerPresence};

    let master = MasterIdentity::new(ProcessIdentity::new(4_242, 0x0102), 1);
    let presence = |counter_at: u64| {
        WorkerPresence::new(
            WorkerIdentity::new(
                WorkerId::new(2),
                ProcessIdentity::new(31_337, 0x0304),
                5,
                master,
            ),
            1_757_780_000_000,
            PresencePhase::Running,
            4_242,
            None,
            OrbitEpoch::from_unix_ms(counter_at),
        )
    };
    let mut worker = WorkerProcess::new(
        WorkerId::new(2),
        None,
        WorkerPhase::Unclaimed {
            watch: crate::lifecycle::LaneWatch::starting_at(1),
        },
        WorkerIntent::EnsureRunning,
    );
    worker.set_phase(WorkerPhase::ReconciliationBlocked {
        presence: presence(1),
        reason: WorkerBlockReason::MasterStillAlive,
    });
    let blocked_since = worker.observed_since;
    tokio::time::sleep(Duration::from_millis(20)).await;

    worker.set_phase(WorkerPhase::ReconciliationBlocked {
        presence: presence(2),
        reason: WorkerBlockReason::MasterStillAlive,
    });
    assert_eq!(
        worker.observed_since, blocked_since,
        "newer evidence does not restart the block"
    );
    assert!(worker.phase_age() >= Duration::from_millis(20));
    assert_eq!(
        worker.phase().evidence().map(|p| p.captured_at().as_unix_ms()),
        Some(2)
    );

    worker.set_phase(WorkerPhase::ReconciliationBlocked {
        presence: presence(3),
        reason: WorkerBlockReason::StalePresence,
    });
    assert!(
        worker.observed_since > blocked_since,
        "a different reason is a new block"
    );
}
