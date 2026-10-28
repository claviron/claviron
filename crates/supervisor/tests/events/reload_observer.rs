use super::*;
#[test]
fn evidence_is_correlated_and_dropped_with_its_request() {
    let issuer = master(10, 100, 1);
    let observer = ReloadObserver::default();
    let request = observer.begin(issuer, 8);
    observer.observe(WorkerReloaded { issuer, generation: 7, worker_id: 1 });
    observer.observe(WorkerReloaded { issuer, generation: 8, worker_id: 0 });
    assert!(request.received().is_empty());
    observer.observe(WorkerReloaded { issuer, generation: 8, worker_id: 2 });
    assert_eq!(request.received(), BTreeSet::from([WorkerId::new(2)]));
    drop(request);
    let next = observer.begin(issuer, 9);
    observer.observe(WorkerReloaded { issuer, generation: 8, worker_id: 1 });
    assert!(next.received().is_empty());
    assert_eq!(observer.requests.lock().unwrap().len(), 1);
}

fn master(
    pid: u32,
    started_at: u64,
    generation: u64,
) -> MasterIdentity {
    MasterIdentity::new(
        claviron_sys::process::ProcessIdentity::new(pid, started_at),
        generation,
    )
}

#[test]
fn reload_issuer_reused_counter_cannot_acknowledge_a_successor_master() {
    let observer = ReloadObserver::default();
    let previous = master(10, 100, 1);
    let current = master(20, 200, 2);
    let old_request = observer.begin(previous, 1);
    let request = observer.begin(current, 1);
    // Each part of the exact issuer identity is significant, including a
    // different process reusing a PID and the same per-process request number.
    for issuer in
        [previous, master(10, 200, 2), master(20, 100, 2), master(20, 200, 1)]
    {
        observer.observe(WorkerReloaded {
            issuer,
            generation: 1,
            worker_id: 1,
        });
        assert!(request.received().is_empty());
    }
    observer.observe(WorkerReloaded {
        issuer: current,
        generation: 1,
        worker_id: 1,
    });
    assert_eq!(request.received(), BTreeSet::from([WorkerId::new(1)]));
    assert!(old_request.received().is_empty());
}
