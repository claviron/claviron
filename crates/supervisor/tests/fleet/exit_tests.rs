use claviron_runtime::WorkerId;

use super::*;

fn slot() -> WorkerProcess {
    WorkerProcess::new(
        WorkerId::new(2),
        None,
        WorkerPhase::Running,
        WorkerIntent::EnsureRunning,
    )
    .with_incarnation(5)
}

#[test]
fn a_slot_the_master_still_wants_exits_failed() {
    // The intent that stood when the process was reaped is what classifies
    // the exit: a slot still wanted is a failure, not a completion.
    assert_eq!(slot().record_exit(), ExitOutcome::Failed);
}
