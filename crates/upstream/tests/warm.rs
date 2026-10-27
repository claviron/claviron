use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::oneshot;

#[tokio::test(flavor = "current_thread")]
async fn audit_warm_cancellation_before_first_poll_does_not_start_work() {
    let owner = Arc::new(Owner::default());
    let calls = Arc::new(AtomicUsize::new(0));
    assert!(!owner.spawn(async {}));
    let cancellation = CancellationToken::new();
    let run = owner.start(&cancellation).unwrap();
    assert!(owner.start(&cancellation).is_err());
    let observed = Arc::clone(&calls);
    assert!(owner.spawn(async move {
        observed.fetch_add(1, Ordering::SeqCst);
    }));
    cancellation.cancel();
    run.drain().await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(!owner.spawn(async { panic!("closed generation cannot run") }));
}

#[tokio::test]
async fn audit_warm_restart_drains_parked_work_without_cancelling_other_owners()
{
    struct Released {
        _sender: oneshot::Sender<()>,
    }
    let owner = Arc::new(Owner::default());
    let independent = CancellationToken::new();
    for _ in 0..3 {
        let cancel = CancellationToken::new();
        let run = owner.start(&cancel).unwrap();
        let (entered, started) = oneshot::channel();
        let (released, finished) = oneshot::channel();
        assert!(owner.spawn(async move {
            let _owned = Released { _sender: released };
            entered.send(()).unwrap();
            std::future::pending::<()>().await;
        }));
        started.await.unwrap();
        cancel.cancel();
        run.drain().await;
        assert!(
            finished.await.is_err(),
            "owned future was dropped before drain returned"
        );
        assert!(!independent.is_cancelled());
    }
}
