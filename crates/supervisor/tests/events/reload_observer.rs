use super::*;
#[test]
fn evidence_is_correlated_and_dropped_with_its_request() {
    let observer = ReloadObserver::default();
    let request = observer.begin(8);
    observer.observe(WorkerReloaded { generation: 7, worker_id: 1 });
    observer.observe(WorkerReloaded { generation: 8, worker_id: 0 });
    assert!(request.received().is_empty());
    observer.observe(WorkerReloaded { generation: 8, worker_id: 2 });
    assert_eq!(request.received(), BTreeSet::from([WorkerId::new(2)]));
    drop(request);
    let next = observer.begin(9);
    observer.observe(WorkerReloaded { generation: 8, worker_id: 1 });
    assert!(next.received().is_empty());
    assert_eq!(observer.requests.lock().unwrap().len(), 1);
}
