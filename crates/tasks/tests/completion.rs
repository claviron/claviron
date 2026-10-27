use super::{Completion, wait};
use futures_util::{FutureExt, poll};
use tokio::sync::oneshot;

#[tokio::test]
async fn audit_lifecycle_completion_before_and_after_waiter_registration() {
    let completed = Completion::new(tokio::spawn(async {}));
    completed.waiter().await;
    assert!(completed.is_finished());
    assert!(wait(vec![completed.waiter()], None).await);

    let (release, held) = oneshot::channel::<()>();
    let active = Completion::new(tokio::spawn(async move {
        held.await.unwrap();
    }));
    let first = wait(vec![active.waiter()], None);
    let second = wait(vec![active.waiter()], None);
    tokio::pin!(first, second);
    assert!(poll!(&mut first).is_pending());
    assert!(poll!(&mut second).is_pending());
    release.send(()).unwrap();
    assert!(first.await);
    assert!(second.await);
}

#[tokio::test]
async fn audit_lifecycle_completion_deadline_and_dropped_waiter_preserve_work()
{
    let (release, held) = oneshot::channel::<()>();
    let active = Completion::new(tokio::spawn(async move {
        held.await.unwrap();
    }));
    assert!(
        !wait(vec![active.waiter()], Some(std::time::Duration::ZERO)).await
    );
    let mut abandoned = wait(vec![active.waiter()], None).boxed();
    assert!(poll!(&mut abandoned).is_pending());
    drop(abandoned);
    assert!(!active.is_finished());
    release.send(()).unwrap();
    assert!(wait(vec![active.waiter()], None).await);
}
