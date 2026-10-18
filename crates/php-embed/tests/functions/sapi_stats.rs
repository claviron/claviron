use super::*;

#[test]
fn retained_stats_reader_includes_added_peer_and_excludes_removed_lane() {
    // Short: the whole segment name has to fit macOS's 31 bytes.
    let instance = format!("rs{:x}", std::process::id());
    struct Cleanup(String);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            reset_shared_memory(&self.0);
        }
    }
    let _cleanup = Cleanup(instance.clone());
    let state = claviron_runtime::AppState::standalone(instance.as_str(), ".");
    let first = SapiStatsTable::open(state.clone(), 1, 4).unwrap();
    let sample = SapiThreadPoolSnapshot {
        accepting: true,
        elastic: false,
        threads_min: 1,
        threads_max: 1,
        threads_max_idle: 1,
        thread_idle_timeout_ms: 0,
        thread_max_life_ms: None,
        in_flight: 0,
        max_in_flight: 0,
        threads_current: 1,
        max_threads_current: 1,
        threads_created_total: 1,
        busy_threads: 0,
        max_busy_threads: 0,
        idle_threads: 1,
        request_count_total: 0,
        slots: vec![],
    };
    first.publish(&sample);
    assert_eq!(first.summary_for(&[1]).unwrap().threads_current_total, 1);
    let missing = first.summary_for(&[1, 3]).unwrap();
    assert!(!missing.ok);
    assert_eq!(missing.missing_worker_ids, [3]);
    let later = SapiStatsTable::open(state, 3, 4).unwrap();
    later.publish(&sample);
    let grown = first.summary_for(&[1, 3]).unwrap();
    assert!(grown.ok);
    assert_eq!(grown.fleet_size, 2);
    assert_eq!(grown.threads_current_total, 2);
    drop(later);
    let shrunk = first.summary_for(&[1]).unwrap();
    assert!(shrunk.ok);
    assert_eq!(shrunk.fleet_size, 1);
    assert_eq!(shrunk.threads_current_total, 1);
}
