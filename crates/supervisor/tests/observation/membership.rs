use std::time::Duration;

use claviron_sys::process::ProcessIdentity;

use super::*;
use crate::lifecycle::{
    DrainReason, ExitDisposition, ExitOutcome, FleetMode, WorkerIntent,
    WorkerObservedPhase,
};
use crate::{WorkerOwnership, WorkerStatus};

#[test]
fn stopped_slots_do_not_require_service_reports_but_live_slots_do() {
    let worker = |id, phase, desired, pid: Option<u32>| WorkerStatus {
        worker_id: id,
        ownership: if pid.is_some() {
            WorkerOwnership::Spawned
        } else {
            WorkerOwnership::Unowned
        },
        process: pid.map(|pid| ProcessIdentity::new(pid, 1)),
        uptime: None,
        recovery_attempts: 0,
        incarnation: 1,
        reload_revision: 0,
        phase,
        phase_age: Duration::ZERO,
        desired,
        desired_age: Duration::ZERO,
        operation: None,
        heartbeat_age: None,
        block_reason: None,
    };
    let stopped = (
        WorkerObservedPhase::Exited(ExitOutcome::Planned),
        WorkerIntent::Stopped,
    );
    let mut snapshot = WorkerListSnapshot {
        captured_at: std::time::SystemTime::now(),
        workers_expected: 2,
        build: crate::lifecycle::BuildIdentity::current(),
        master: None,
        master_runtime: None,
        fleet_mode: FleetMode::Running,
        workers: vec![
            worker(1, stopped.0, stopped.1, None),
            worker(
                2,
                WorkerObservedPhase::Running,
                WorkerIntent::EnsureRunning,
                Some(123),
            ),
        ],
    };
    let terminal = [
        (WorkerLifecycle::Stopped, stopped),
        (
            WorkerLifecycle::Exited,
            (
                WorkerObservedPhase::Exited(ExitOutcome::Planned),
                WorkerIntent::EnsureRunning,
            ),
        ),
        (
            WorkerLifecycle::Crashed,
            (
                WorkerObservedPhase::Exited(ExitOutcome::Failed),
                WorkerIntent::EnsureRunning,
            ),
        ),
    ];
    for (lifecycle, (phase, desired)) in terminal {
        snapshot.workers[0].phase = phase;
        snapshot.workers[0].desired = desired;
        assert_eq!(snapshot.workers[0].lifecycle(), lifecycle);
        let members = from_snapshot(&snapshot).unwrap();
        assert_eq!(
            members.iter().map(|m| m.node_id).collect::<Vec<_>>(),
            [0, 2]
        );
    }
    let live = [
        (
            WorkerLifecycle::Starting,
            (WorkerObservedPhase::Starting, WorkerIntent::EnsureRunning),
        ),
        (
            WorkerLifecycle::Running,
            (WorkerObservedPhase::Running, WorkerIntent::EnsureRunning),
        ),
        (
            WorkerLifecycle::Reloading,
            (WorkerObservedPhase::Reloading, WorkerIntent::EnsureRunning),
        ),
        (
            WorkerLifecycle::Stopping,
            (
                WorkerObservedPhase::Running,
                WorkerIntent::Drain {
                    reason: DrainReason::ManualStop,
                    after_exit: ExitDisposition::StayStopped,
                },
            ),
        ),
    ];
    for (lifecycle, (phase, desired)) in live {
        snapshot.workers[0].phase = phase;
        snapshot.workers[0].desired = desired;
        assert_eq!(snapshot.workers[0].lifecycle(), lifecycle);
        assert_eq!(from_snapshot(&snapshot).unwrap().len(), 3);
    }
    snapshot.workers[0].phase = stopped.0;
    snapshot.workers[0].desired = stopped.1;
    snapshot.workers[0].process = Some(ProcessIdentity::new(456, 1));
    assert!(from_snapshot(&snapshot).is_err());
    snapshot.workers[0].process = None;
    snapshot.workers[1].worker_id = 1;
    assert!(from_snapshot(&snapshot).is_err());
    snapshot.workers.pop();
    assert!(from_snapshot(&snapshot).is_err());
}
