use super::*;

#[test]
fn self_rss_is_some_on_supported_platforms() {
    // On Linux/FreeBSD/macOS the self path should always succeed.
    // On other platforms `None` is the documented result.
    let v = read_rss(None);
    if cfg!(any(
        target_os = "linux",
        target_os = "freebsd",
        target_os = "macos"
    )) {
        assert!(
            v.is_some(),
            "self RSS unexpectedly None on supported platform"
        );
        let bytes = v.unwrap();
        assert!(bytes > 0, "self RSS must be > 0");
    }
}

#[cfg(target_os = "macos")]
#[test]
fn explicit_pid_rss_is_some_on_macos() {
    let bytes = read_rss(Some(std::process::id()))
        .expect("explicit current pid RSS unexpectedly unavailable on macOS");
    assert!(bytes > 0, "explicit current pid RSS must be > 0");
}
