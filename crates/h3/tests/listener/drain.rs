use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use claviron_runtime::gate::{Gate, GateDrainOutcome};
use tokio::sync::oneshot;

use super::*;

const TIMEOUT: Duration = Duration::from_secs(5);

struct Released(Arc<AtomicUsize>);
impl Drop for Released {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Clone, Copy)]
enum StopAt {
    Driver,
    Drain,
    Panic,
}

async fn verify_cleanup(stop: StopAt) {
    let gate = Gate::new(Some(1), TIMEOUT);
    let permit = gate.try_enter().unwrap();
    let released = Arc::new(AtomicUsize::new(0));
    let observed = released.clone();
    let closed = Arc::new(AtomicBool::new(false));
    let on_close = closed.clone();
    let (ready, waiting) = oneshot::channel();
    let (panic_now, panic_wait) = oneshot::channel();
    let owner = tokio::spawn(async move {
        let mut streams = JoinSet::new();
        let exit = drive(
            async {
                for _ in 0..3 {
                    let guard = Released(observed.clone());
                    streams.spawn(async move {
                        let _guard = guard;
                        std::future::pending::<()>().await;
                    });
                }
                ready.send(()).unwrap();
                match stop {
                    StopAt::Driver => std::future::pending::<()>().await,
                    StopAt::Drain => {}
                    StopAt::Panic => {
                        let _ = panic_wait.await;
                        panic!("injected driver failure");
                    }
                }
            },
            &permit,
        )
        .await;
        assert!(matches!(
            (stop, &exit),
            (StopAt::Driver, DriverExit::Forced)
                | (StopAt::Drain, DriverExit::Complete)
                | (StopAt::Panic, DriverExit::Panicked)
        ));
        finish(streams, &permit, exit, || {
            on_close.store(true, Ordering::SeqCst)
        })
        .await;
        assert_eq!(
            observed.load(Ordering::SeqCst),
            3,
            "children must be joined before capacity releases"
        );
        drop(permit);
    });
    tokio::time::timeout(TIMEOUT, waiting).await.unwrap().unwrap();
    assert_eq!(gate.count(), 1);
    if matches!(stop, StopAt::Panic) {
        panic_now.send(()).unwrap();
    } else {
        gate.graceful_shutdown(Some(Duration::ZERO));
        assert!(matches!(
            gate.wait_all_done().await,
            GateDrainOutcome::Forced { remaining: 1 }
        ));
    }
    tokio::time::timeout(TIMEOUT, owner).await.unwrap().unwrap();
    assert!(closed.load(Ordering::SeqCst));
    assert_eq!(gate.count(), 0);
}

#[tokio::test]
async fn force_interrupts_stalled_driver_and_joins_children() {
    verify_cleanup(StopAt::Driver).await;
}

#[tokio::test]
async fn force_during_session_drain_joins_children() {
    verify_cleanup(StopAt::Drain).await;
}

#[tokio::test]
async fn one_failed_stream_does_not_cancel_other_streams() {
    let gate = Gate::new(Some(1), TIMEOUT);
    let permit = gate.try_enter().unwrap();
    let mut streams = JoinSet::new();
    let completed = Arc::new(AtomicBool::new(false));
    streams.spawn(async {
        panic!("injected stream failure");
    });
    let observed = completed.clone();
    streams.spawn(async move {
        tokio::task::yield_now().await;
        observed.store(true, Ordering::SeqCst);
    });
    finish(streams, &permit, DriverExit::Complete, || panic!("normal finish"))
        .await;
    assert!(completed.load(Ordering::SeqCst));
}
