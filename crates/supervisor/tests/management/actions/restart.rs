use claviron_runtime::WorkerId;

use super::restart_order;
use crate::control::WorkerProcess;
use crate::lifecycle::{ExitOutcome, WorkerIntent, WorkerPhase};

fn ids<const N: usize>(raw: [u8; N]) -> Vec<WorkerId> {
    raw.into_iter().map(WorkerId::new).collect()
}

#[test]
fn command_origin_is_restarted_last() {
    let workers = vec![running(1), running(2), running(3)];
    assert_eq!(
        restart_order(&workers, Some(WorkerId::new(2))).unwrap(),
        ids([1, 3, 2])
    );
}

#[test]
fn stopped_slots_are_preserved_outside_the_plan() {
    let workers = vec![running(1), stopped(2), running(3)];
    assert_eq!(restart_order(&workers, None).unwrap(), ids([1, 3]));
}

#[test]
fn one_running_worker_cannot_preserve_fleet_continuity() {
    let workers = vec![running(1), stopped(2)];
    assert!(restart_order(&workers, Some(WorkerId::new(1))).is_err());
}

fn running(id: u8) -> WorkerProcess {
    worker(id, WorkerPhase::Running, WorkerIntent::EnsureRunning)
}

fn stopped(id: u8) -> WorkerProcess {
    worker(
        id,
        WorkerPhase::Exited { outcome: ExitOutcome::Planned },
        WorkerIntent::Stopped,
    )
}

fn worker(
    id: u8,
    phase: WorkerPhase,
    intent: WorkerIntent,
) -> WorkerProcess {
    WorkerProcess::new(WorkerId::new(id), None, phase, intent)
}
