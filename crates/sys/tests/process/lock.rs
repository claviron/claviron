use std::io::{BufRead, BufReader};
use std::os::unix::fs::symlink;
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use super::*;

#[test]
fn names_have_stable_host_scope_and_safe_paths() {
    assert_eq!(
        lock_path("shop").unwrap(),
        Path::new("/tmp")
            .join(format!("claviron-process-{}.lock", blake3::hash(b"shop")))
    );
    assert_ne!(lock_path("shop/a").unwrap(), lock_path("shop_a").unwrap());
    assert_ne!(lock_path("Shop").unwrap(), lock_path("shop").unwrap());
    for name in ["../shop", "/shop", "a\0b", "mağaza", &"long".repeat(1024)] {
        let path = lock_path(name).unwrap();
        assert_eq!(path.parent(), Some(Path::new("/tmp")));
        assert!(path.file_name().unwrap().len() < 255);
    }
    assert_eq!(lock_path(" ").unwrap_err().kind(), io::ErrorKind::InvalidInput);
}

// #[test]
// fn competing_handles_fail_and_drop_releases_without_unlinking() {
//     let dir = tempfile::tempdir().unwrap();
//     let path = dir.path().join("primary.lock");
//     fs::write(&path, "stale PID or arbitrary contents are not ownership").unwrap();
//     let guard = ProcessLock::at_path(&path).unwrap();
//     assert_eq!(ProcessLock::at_path(&path).unwrap_err().kind(), io::ErrorKind::WouldBlock);
//     // A different name does not contend.
//     let _other = ProcessLock::at_path(&dir.path().join("other.lock")).unwrap();
//     drop(guard);
//     assert_eq!(
//         fs::read_to_string(&path).unwrap(),
//         "stale PID or arbitrary contents are not ownership"
//     );
//     let _restarted = ProcessLock::at_path(&path).unwrap();
// }

#[test]
fn rejects_symlinks_hardlinks_and_directories_without_touching_targets() {
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("target");
    fs::write(&target, "untouched").unwrap();
    let link = dir.path().join("symlink");
    symlink(&target, &link).unwrap();
    assert!(ProcessLock::at_path(&link).is_err());
    let hardlink = dir.path().join("hardlink");
    fs::hard_link(&target, &hardlink).unwrap();
    assert_eq!(
        ProcessLock::at_path(&hardlink).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert!(ProcessLock::at_path(dir.path()).is_err());
    assert_eq!(fs::read_to_string(&target).unwrap(), "untouched");
}

#[test]
fn process_crash_releases_lock_without_stale_file_cleanup() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("primary.lock");
    let mut child = Fixture::spawn(&path, "hold");
    assert_eq!(
        ProcessLock::at_path(&path).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    assert!(path.exists());
    let _guard = ProcessLock::at_path(&path).unwrap();
}

#[test]
fn exec_does_not_keep_the_parent_lock_alive() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("primary.lock");
    let mut child = Fixture::spawn(&path, "exec");
    assert!(child.0.try_wait().unwrap().is_none());
    let _guard = ProcessLock::at_path(&path).unwrap();
}

// Only activated in a re-executed test process; no global env mutation.
#[test]
fn subprocess_entry() {
    let Some(path) = std::env::var_os("CLAVIRON_TEST_LOCK_PATH") else {
        return;
    };
    let _guard = ProcessLock::at_path(Path::new(&path)).unwrap();
    if std::env::var("CLAVIRON_TEST_LOCK_MODE").unwrap() == "exec" {
        let error = Command::new("/bin/sh")
            .args(["-c", "printf 'lock-fixture-ready\\n'; read line"])
            .exec();
        panic!("fixture exec failed: {error}");
    }
    println!("lock-fixture-ready");
    let mut input = String::new();
    std::io::stdin().read_line(&mut input).unwrap();
}

struct Fixture(Child);

impl Fixture {
    fn spawn(
        path: &Path,
        mode: &str,
    ) -> Self {
        let mut fixture = Self(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "process::lock::tests::subprocess_entry",
                    "--nocapture",
                ])
                .env("CLAVIRON_TEST_LOCK_PATH", path)
                .env("CLAVIRON_TEST_LOCK_MODE", mode)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .unwrap(),
        );
        let stdout = fixture.0.stdout.take().unwrap();
        let (send, receive) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                if line.unwrap() == "lock-fixture-ready" {
                    let _ = send.send(());
                    break;
                }
            }
        });
        receive.recv_timeout(Duration::from_secs(10)).expect("fixture ready");
        reader.join().unwrap();
        fixture
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // Only this test-owned child; never signal an external Claviron process.
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
