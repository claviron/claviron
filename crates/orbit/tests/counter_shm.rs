//! Cross-process keyed counter updates over the Claviron Orbit SHM table.

#![cfg(unix)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use claviron_orbit::{Counter, Fleet, NodeId};
use nix::sys::wait::{WaitStatus, waitpid};
use nix::unistd::{ForkResult, fork};

const INCREMENTS_PER_PROCESS: usize = 10_000;

fn fresh_name() -> &'static str {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let pid = std::process::id() & 0xffff;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .subsec_nanos();
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed) & 0xff;
    Box::leak(format!("c{pid:04x}{nonce:08x}{counter:02x}").into_boxed_str())
}

fn wait_child(pid: nix::unistd::Pid) -> i32 {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        match waitpid(pid, Some(nix::sys::wait::WaitPidFlag::WNOHANG)) {
            Ok(WaitStatus::Exited(_, code)) => return code,
            Ok(WaitStatus::Signaled(_, signal, _)) => {
                panic!("child killed by {signal:?}")
            }
            Ok(WaitStatus::StillAlive) => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "child timed out"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
            Ok(other) => panic!("unexpected child status: {other:?}"),
            Err(error) => panic!("waitpid failed: {error}"),
        }
    }
}

#[test]
fn parent_and_child_increment_without_lost_updates() {
    let name = fresh_name();
    let parent_fleet = Arc::new(
        Fleet::join_shm_as(name, 2, NodeId::new(0)).expect("parent fleet"),
    );
    let parent = Counter::new(parent_fleet).expect("parent counter");
    parent.reset_all().expect("reset counter table");

    match unsafe { fork() }.expect("fork") {
        ForkResult::Child => {
            let fleet = match Fleet::join_shm_as(name, 2, NodeId::new(1)) {
                Ok(fleet) => Arc::new(fleet),
                Err(_) => std::process::exit(11),
            };
            let counter = match Counter::new(fleet) {
                Ok(counter) => counter,
                Err(_) => std::process::exit(12),
            };
            for _ in 0..INCREMENTS_PER_PROCESS {
                if counter.increment("shared", 1).is_err() {
                    std::process::exit(13);
                }
            }
            std::process::exit(0);
        }
        ForkResult::Parent { child } => {
            for _ in 0..INCREMENTS_PER_PROCESS {
                parent.increment("shared", 1).expect("parent increment");
            }
            assert_eq!(wait_child(child), 0);
            assert_eq!(
                parent.get("shared").expect("read counter"),
                Some((INCREMENTS_PER_PROCESS * 2) as i64)
            );
            parent.unlink().expect("unlink counter table");
        }
    }
}
