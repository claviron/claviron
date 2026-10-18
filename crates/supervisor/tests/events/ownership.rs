use std::process::Command;
use std::sync::Arc;
use std::time::Instant;

use claviron_orbit::EVENT_PAYLOAD_MAX;
use claviron_runtime::{FleetCapacity, FleetNodeId, WorkerCount, WorkerId};
use claviron_sys::process::{self, ProcessIdentity};
use tokio::sync::Notify;

use super::*;
use crate::control::WorkerProcess;
use crate::lifecycle::{OwnedProcess, ProcessExit, WorkerIntent, WorkerPhase};

fn identities() -> (WorkerIdentity, MasterIdentity) {
    let old = MasterIdentity::new(ProcessIdentity::new(100, 200), 3);
    let new = MasterIdentity::new(ProcessIdentity::new(300, 400), 4);
    (
        WorkerIdentity::new(
            WorkerId::new(2),
            ProcessIdentity::new(500, 600),
            7,
            old,
        ),
        new,
    )
}

#[test]
fn ownership_codecs_round_trip_exact_identities() {
    let (worker, claimant) = identities();
    let claim = WorkerOwnershipClaim { target: worker, claimant };
    let payload = claim.encode_orbit();
    assert_eq!(payload.len(), OWNERSHIP_CLAIM_BYTES);
    assert_eq!(WorkerOwnershipClaim::decode_orbit(&payload).unwrap(), claim);

    let accepted = WorkerOwnershipAccepted {
        previous: worker,
        current: worker.with_master(claimant),
    };
    let payload = accepted.encode_orbit();
    assert_eq!(payload.len(), OWNERSHIP_ACCEPTED_BYTES);
    assert_eq!(
        WorkerOwnershipAccepted::decode_orbit(&payload).unwrap(),
        accepted
    );
    assert!(
        12 + WorkerOwnershipAccepted::TOPIC.len() + payload.len()
            <= EVENT_PAYLOAD_MAX
    );
    assert!(WorkerOwnershipClaim::decode_orbit(&payload).is_err());
    assert!(
        WorkerOwnershipAccepted::decode_orbit(&payload[..payload.len() - 1])
            .is_err()
    );
}

#[test]
fn ambiguous_identity_fails_closed_with_the_pid() {
    let mut worker = Command::new("/bin/sleep").arg("60").spawn().unwrap();
    let worker_process =
        process::identity(worker.id()).expect("worker identity");
    let unknown_master =
        MasterIdentity::new(ProcessIdentity::new(424_242, 11), 1);
    let current_master = MasterIdentity::new(
        process::identity(std::process::id()).expect("current master identity"),
        2,
    );
    let presence = WorkerPresence::new(
        WorkerIdentity::new(
            WorkerId::new(1),
            worker_process,
            1,
            unknown_master,
        ),
        1_757_780_000_123,
        PresencePhase::Running,
        1,
        None,
        OrbitEpoch::from_unix_ms(100),
    );

    let refusal = assess_presence(
        presence,
        current_master,
        false,
        OrbitEpoch::from_unix_ms(101),
        Duration::from_secs(1),
    )
    .unwrap_err()
    .to_string();
    assert!(refusal.contains(&worker.id().to_string()));
    assert!(refusal.contains("original master identity is absent"));
    assert!(
        worker.try_wait().unwrap().is_none(),
        "refusal must not terminate the worker"
    );
    worker.kill().unwrap();
    worker.wait().unwrap();
}

#[test]
fn reused_pid_is_never_adopted() {
    let me = process::identity(std::process::id()).expect("own identity");
    let reused = ProcessIdentity::new(me.pid(), me.started_at() ^ 1);
    let old_master = MasterIdentity::new(ProcessIdentity::new(100, 200), 1);
    let current_master = MasterIdentity::new(me, 2);
    let presence = WorkerPresence::new(
        WorkerIdentity::new(WorkerId::new(1), reused, 1, old_master),
        1_757_780_000_123,
        PresencePhase::Running,
        1,
        None,
        OrbitEpoch::from_unix_ms(100),
    );

    assert_eq!(
        assess_presence(
            presence,
            current_master,
            true,
            OrbitEpoch::from_unix_ms(101),
            Duration::from_secs(1)
        ),
        Ok(PresenceDecision::ReclaimDead)
    );
}

#[tokio::test]
async fn live_worker_of_a_dead_master_is_adopted_after_claim_accept() {
    let mut old_master_child =
        Command::new("/bin/sleep").arg("60").spawn().unwrap();
    let old_master_process =
        process::identity(old_master_child.id()).expect("old master");
    old_master_child.kill().unwrap();
    old_master_child.wait().unwrap();
    assert!(!old_master_process.is_alive());

    let mut worker_child =
        Command::new("/bin/sleep").arg("60").spawn().unwrap();
    let worker_process =
        process::identity(worker_child.id()).expect("worker identity");
    let old_master = MasterIdentity::new(old_master_process, 1);
    let current_master = MasterIdentity::new(
        process::identity(std::process::id()).expect("current master identity"),
        2,
    );
    let previous =
        WorkerIdentity::new(WorkerId::new(1), worker_process, 7, old_master);
    let provider = WorkerIdentityProvider::new(previous, 1_757_780_000_123);
    let presence = WorkerPresence::new(
        previous,
        provider.runtime_started_at_unix_ms(),
        PresencePhase::Running,
        1,
        None,
        OrbitEpoch::now(),
    );
    let claim =
        WorkerOwnershipClaim { target: previous, claimant: current_master };
    let current = provider
        .accept_master_claim(claim.target, claim.claimant)
        .expect("worker accepts a live newer master after its old master died");
    assert_eq!(provider.identity(), previous.with_master(current_master));

    let observer = Arc::new(crate::observer::HeartbeatObserver::new());
    let supervisor = WorkerControl::new(observer);
    supervisor.set_master_for_tests(current_master);
    supervisor.workers.write().await.push(
        WorkerProcess::new(
            WorkerId::new(1),
            None,
            WorkerPhase::OwnershipPending {
                presence,
                requested_at: Instant::now(),
            },
            WorkerIntent::EnsureRunning,
        )
        .with_incarnation(0),
    );

    let state = AppState::initialize(
        RunMode::Master,
        FleetNodeId::ZERO,
        WorkerCount::new(1),
        FleetCapacity::new(2).unwrap(),
        Some("ownership-test".into()),
        ".",
    );
    assert!(
        crate::lifecycle::reconcile::ownership_accepted(
            &supervisor,
            WorkerOwnershipAccepted { previous, current },
            &state,
        )
        .await
    );
    {
        let workers = supervisor.workers.read().await;
        let adopted = &workers[0];
        assert_eq!(adopted.phase(), WorkerPhase::Running);
        assert_eq!(adopted.pid(), Some(worker_child.id()));
        assert_eq!(adopted.incarnation, 7);
        assert!(adopted.process.as_ref().unwrap().is_adopted());
        assert_eq!(adopted.identity(current_master), Some(current));
    }
    assert!(
        crate::lifecycle::reconcile::install_adopted_worker(
            &supervisor,
            current.worker_id(),
            current,
            presence,
            &state,
        )
        .await
        .unwrap(),
        "the presence path must accept an adoption already completed by the acknowledgement"
    );
    assert_eq!(
        crate::observation::project::management_snapshot(&supervisor, &state)
            .await
            .workers[0]
            .ownership,
        crate::WorkerOwnership::Adopted,
        "manager status must derive adoption from the live handle kind"
    );

    worker_child.kill().unwrap();
    worker_child.wait().unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let exited = {
                let mut workers = supervisor.workers.write().await;
                workers[0].process.as_mut().unwrap().try_reap().unwrap()
            };
            if matches!(exited, Some(ProcessExit::Observed)) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("adopted handle observes exact worker exit");

    let replacement = OwnedProcess::from_child(
        Command::new("/bin/sleep").arg("60").spawn().unwrap(),
        Arc::new(Notify::new()),
    )
    .unwrap();
    {
        let mut workers = supervisor.workers.write().await;
        workers[0].process = Some(replacement);
        workers[0].set_phase(WorkerPhase::Running);
        workers[0].incarnation = 8;
    }
    assert_eq!(
        crate::observation::project::management_snapshot(&supervisor, &state)
            .await
            .workers[0]
            .ownership,
        crate::WorkerOwnership::Spawned,
        "a replacement child must replace the adopted status in the next snapshot"
    );
    let mut workers = supervisor.workers.write().await;
    workers[0].process.as_mut().unwrap().child_mut().kill().unwrap();
    workers[0].process.as_mut().unwrap().child_mut().wait().unwrap();
}
