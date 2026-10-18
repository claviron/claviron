use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use super::*;
use futures_util::poll;

#[tokio::test]
async fn environment_renewal_waits_for_owned_execution() {
    let cancellation = CancellationToken::new();
    let child = cancellation.clone();
    let joined = Arc::new(AtomicBool::new(false));
    let finished = joined.clone();
    let (finish, wait) = tokio::sync::oneshot::channel();
    let running = async move {
        child.cancelled().await;
        wait.await.expect("execution finish signal");
        finished.store(true, Ordering::SeqCst);
        Ok(())
    };
    tokio::pin!(running);
    let drain = drain_for_environment(running.as_mut(), &cancellation);
    tokio::pin!(drain);

    assert!(poll!(&mut drain).is_pending());
    assert!(cancellation.is_cancelled());
    assert!(!joined.load(Ordering::SeqCst));
    finish.send(()).expect("execution still owned");
    drain.await.expect("drain succeeds");
    assert!(joined.load(Ordering::SeqCst));
}

#[tokio::test]
async fn environment_renewal_does_not_cancel_sibling_or_parent() {
    let parent = CancellationToken::new();
    let selected = parent.child_token();
    let sibling = parent.child_token();
    let child = selected.clone();
    let running = async move {
        child.cancelled().await;
        Err(Error::stopped(Reason::Cancelled))
    };
    tokio::pin!(running);
    drain_for_environment(running.as_mut(), &selected).await.unwrap();
    assert!(selected.is_cancelled());
    assert!(!parent.is_cancelled());
    assert!(!sibling.is_cancelled());
}

#[tokio::test]
async fn failed_engine_drain_refuses_replacement() {
    let cancellation = CancellationToken::new();
    let running = async { Err(Error::other("owned work did not join")) };
    tokio::pin!(running);
    let result = drain_for_environment(running.as_mut(), &cancellation).await;
    assert!(
        matches!(result, Err(Error::Other(message)) if message == "owned work did not join")
    );
}

#[tokio::test]
async fn one_shot_environment_has_no_notification_waiter() {
    let mut environment = ExecutionEnvironment {
        snapshot: Snapshot::new(HashMap::from([(
            "APP_NAME".into(),
            "example".into(),
        )])),
        notification: None,
    };
    assert_eq!(environment.values()["APP_NAME"], "example");
    let notified = environment.notified();
    tokio::pin!(notified);
    assert!(poll!(&mut notified).is_pending());
}
