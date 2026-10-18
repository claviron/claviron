//! `scrub_process_env` removes everything outside the allow-list.
//!
//! Keep all env-mutating scenarios in this single integration test: separate
//! `#[test]` functions would race over the same process environment. Other test
//! targets can run normally; no global serialization flag is needed.

use claviron_runtime::env::{RUNTIME_ESSENTIALS, scrub_process_env};

#[test]
fn scrubs_app_secrets_keeps_essentials() {
    let original_path = std::env::var_os("PATH");
    // SAFETY: this target runs one test and starts no background env readers.
    unsafe {
        std::env::set_var("CLAVIRON_TEST_FAKE_SECRET", "should_disappear");
        std::env::set_var("CLAVIRON_TEST_DB_PASSWORD", "rotate_me");
    }
    scrub_process_env(RUNTIME_ESSENTIALS);
    assert_eq!(std::env::var_os("PATH"), original_path);
    assert!(std::env::var("CLAVIRON_TEST_FAKE_SECRET").is_err());
    assert!(std::env::var("CLAVIRON_TEST_DB_PASSWORD").is_err());

    // A custom allow-list still preserves the inherited PATH.
    // SAFETY: same exclusive access as above; scenarios run sequentially.
    unsafe {
        std::env::set_var("CLAVIRON_TEST_X", "1");
        std::env::set_var("CLAVIRON_TEST_Y", "2");
    }
    scrub_process_env(&["PATH"]);
    assert!(std::env::var("CLAVIRON_TEST_X").is_err());
    assert!(std::env::var("CLAVIRON_TEST_Y").is_err());
    assert_eq!(std::env::var_os("PATH"), original_path);
    assert!(std::env::vars_os().all(|(key, _)| key == "PATH"));

    // Listing an absent key must not create it or discard the retained PATH.
    let never_set = "CLAVIRON_TEST_NEVER_SET";
    // SAFETY: same exclusive access; make this scenario independent of the
    // environment inherited by the test runner.
    unsafe { std::env::remove_var(never_set) };
    scrub_process_env(&["PATH", never_set]);
    assert!(std::env::var("CLAVIRON_TEST_NEVER_SET").is_err());
    assert_eq!(std::env::var_os("PATH"), original_path);
}
