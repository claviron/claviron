use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;

use super::OwnedProcess;

#[tokio::test]
async fn kernel_exit_runs_cleanup_once_before_exit_waiters_complete() {
    use std::process::Stdio;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let mut process = OwnedProcess::from_child(
        Command::new("/bin/cat")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .spawn()
            .unwrap(),
        Arc::new(Notify::new()),
    )
    .unwrap();
    let cleaned = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&cleaned);
    assert!(
        process
            .exit()
            .cleanup
            .set(Box::new(move || {
                observed.fetch_add(1, Ordering::SeqCst);
            }))
            .is_ok()
    );
    assert!(process.try_reap().unwrap().is_none());
    assert_eq!(cleaned.load(Ordering::SeqCst), 0);
    let first = process.exit();
    let second = process.exit();
    process.signal(nix::sys::signal::Signal::SIGKILL).unwrap();
    tokio::join!(first.exited(), second.exited());
    assert_eq!(cleaned.load(Ordering::SeqCst), 1);
    process.child_mut().wait().unwrap();
    process.try_reap().unwrap();
    assert_eq!(cleaned.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn owned_process_reports_pid_from_identity_and_dies_with_it() {
    let child = Command::new("/bin/sleep").arg("60").spawn().unwrap();
    let pid = child.id();
    let mut process =
        OwnedProcess::from_child(child, Arc::new(Notify::new())).unwrap();

    assert_eq!(process.pid(), pid);
    assert_eq!(process.identity().pid(), pid);
    assert!(process.identity().is_alive());
    assert!(process.try_reap().unwrap().is_none());
    assert!(!process.has_exited());

    process.child_mut().kill().unwrap();
    process.child_mut().wait().unwrap();
    assert!(
        !process.identity().is_alive(),
        "a reaped process must not read as alive"
    );
    assert!(process.try_reap().unwrap().is_some());
    assert!(process.has_exited());
}

#[tokio::test]
async fn one_exit_observer_serves_two_waiters_for_the_same_identity() {
    let wake = Arc::new(Notify::new());
    let woken = wake.notified();
    let process = OwnedProcess::from_child(
        Command::new("/bin/sleep").arg("60").spawn().unwrap(),
        Arc::clone(&wake),
    )
    .unwrap();
    let first = process.exit();
    let second = process.exit();
    assert!(Arc::ptr_eq(&first, &second), "waiters must share one observation");

    process.signal(nix::sys::signal::Signal::SIGKILL).unwrap();
    // Neither waiter holds the handle; the observer proves the exit through
    // the kernel identity alone, before anything has reaped the zombie.
    tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(first.exited(), second.exited(), woken);
    })
    .await
    .expect("both waiters observe the exit");
    assert!(first.status().is_none(), "no reap has happened yet");
}
