use super::{ProcessHandle, identity};

#[tokio::test]
async fn current_process_identity_can_be_opened() {
    let identity =
        identity(std::process::id()).expect("current process identity");
    assert_eq!(identity.pid(), std::process::id());
    assert!(identity.is_alive());
    ProcessHandle::open(identity).expect("current process handle");
}

#[tokio::test]
async fn process_handle_observes_exit() {
    use std::future::{Future, poll_fn};
    use std::pin::pin;
    use std::process::Stdio;
    use std::task::Poll;

    let mut child = std::process::Command::new("/bin/cat")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .expect("spawn child");
    let identity = identity(child.id()).expect("child identity");
    let handle = ProcessHandle::open(identity).expect("child handle");
    {
        let mut cancelled = pin!(handle.wait());
        assert!(
            poll_fn(|cx| Poll::Ready(cancelled.as_mut().poll(cx)))
                .await
                .is_pending()
        );
    }
    handle.signal(libc::SIGTERM).expect("signal child");
    let (first, second) = tokio::join!(handle.wait(), handle.wait());
    first.expect("first waiter observes exit");
    second.expect("second waiter observes exit");
    handle.wait().await.expect("late waiter observes the same exit");
    #[cfg(target_os = "freebsd")]
    assert!(!identity.is_alive(), "an unreaped zombie is not alive");
    child.wait().expect("reap child after all observers complete");
}

/// LEGACY_LINUX (Linux < 5.3).
///
/// A kernel without `pidfd_open` (Linux before 5.3) leaves the handle without
/// a descriptor. It must still see its child exit before the child is reaped:
/// the parent waits on the handle first and reaps after.
#[cfg(all(target_os = "linux", feature = "legacy-linux"))]
#[tokio::test]
async fn handle_without_pidfd_observes_an_unreaped_child() {
    let mut child = std::process::Command::new("/bin/sleep")
        .arg("30")
        .spawn()
        .expect("spawn sleep");
    let identity = identity(child.id()).expect("sleep identity");
    let handle = ProcessHandle { identity, notifier: None };
    handle.signal(libc::SIGTERM).expect("signal sleep");
    handle.wait().await.expect("observe sleep exit");
    child.wait().expect("reap sleep");
}
