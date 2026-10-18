use super::*;

#[test]
fn rss_sampling_delegates_to_claviron_sys() {
    const TASK: &str = "rss_sampler_current_process_test";
    const ROLE: &str = "test";
    let pid = std::process::id();
    let supported = claviron_sys::rss::read_rss(Some(pid)).is_some();

    register_command_pid(TASK, 1, pid);
    let observed = sample_once(ROLE);
    unregister_command_pid(TASK, 1);

    assert_eq!(observed.contains(TASK), supported);
    if supported {
        assert!(
            TASK_METRICS.snapshot().rss_bytes
                [&(TASK.to_string(), ROLE.to_string())]
                > 0
        );
    }
    TASK_METRICS.clear_rss_bytes(TASK, ROLE);
}
