use super::*;

#[test]
fn exit_is_classified_from_intent_not_from_the_status_label() {
    let planned = |reason, after_exit| {
        WorkerIntent::Drain { reason, after_exit }.on_exit()
    };

    // A process that leaves while it should be running has failed and is
    // still wanted.
    assert_eq!(
        WorkerIntent::EnsureRunning.on_exit(),
        ExitTransition {
            outcome: ExitOutcome::Failed,
            next: WorkerIntent::EnsureRunning
        }
    );
    // Operator, rotation, resize and shutdown drains are planned exits,
    // and the disposition decides what the slot wants next.
    assert_eq!(
        planned(DrainReason::ManualStop, ExitDisposition::StayStopped),
        ExitTransition {
            outcome: ExitOutcome::Planned,
            next: WorkerIntent::Stopped
        }
    );
    assert_eq!(
        planned(DrainReason::Rotation, ExitDisposition::Replace),
        ExitTransition {
            outcome: ExitOutcome::Planned,
            next: WorkerIntent::EnsureRunning
        }
    );
    assert_eq!(
        planned(DrainReason::ScaleDown, ExitDisposition::RemoveSlot),
        ExitTransition {
            outcome: ExitOutcome::Planned,
            next: WorkerIntent::Stopped
        }
    );
    assert_eq!(
        planned(DrainReason::Shutdown, ExitDisposition::FinishFleetShutdown),
        ExitTransition {
            outcome: ExitOutcome::Planned,
            next: WorkerIntent::Stopped
        }
    );
    // A hung worker that had to be drained is a failure, and still wanted.
    assert_eq!(
        planned(DrainReason::HeartbeatTimeout, ExitDisposition::Replace),
        ExitTransition {
            outcome: ExitOutcome::Failed,
            next: WorkerIntent::EnsureRunning
        }
    );
}

#[test]
fn lifecycle_is_a_projection_of_phase_and_intent() {
    let drain = WorkerIntent::Drain {
        reason: DrainReason::RollingRestart,
        after_exit: ExitDisposition::Replace,
    };
    assert_eq!(
        WorkerLifecycle::project(WorkerPhase::Running, drain),
        WorkerLifecycle::Stopping
    );
    assert_eq!(
        WorkerLifecycle::project(
            WorkerPhase::Running,
            WorkerIntent::EnsureRunning
        ),
        WorkerLifecycle::Running
    );
    assert_eq!(
        WorkerLifecycle::project(
            WorkerPhase::Exited { outcome: ExitOutcome::Failed },
            WorkerIntent::EnsureRunning
        ),
        WorkerLifecycle::Crashed
    );
    assert_eq!(
        WorkerLifecycle::project(
            WorkerPhase::Exited { outcome: ExitOutcome::Failed },
            WorkerIntent::Stopped
        ),
        WorkerLifecycle::Stopped
    );
    assert_eq!(
        WorkerLifecycle::project(
            WorkerPhase::Exited { outcome: ExitOutcome::Planned },
            WorkerIntent::EnsureRunning
        ),
        WorkerLifecycle::Exited
    );
}
