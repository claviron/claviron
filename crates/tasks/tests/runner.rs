use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use claviron_runtime::AppState;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use super::{
    StopContext, TaskRunner, cron_search_after, cron_wait_delay,
    interval_bucket,
};
use crate::config::TaskServiceConfig;
use crate::{Error, TaskDispatch, TaskExecutionResult, TaskScope};

#[test]
fn reload_signals_notify_every_live_child_without_replaying_to_replacements() {
    use crate::process::ProcessSignal;

    let stop = StopContext::default();
    stop.send_reload_signal(ProcessSignal::Hup);
    let mut first = stop.subscribe_reload_signals();
    let second = stop.subscribe_reload_signals();
    assert!(!first.has_changed().unwrap(), "no replay after an idle reload");
    stop.send_reload_signal(ProcessSignal::Hup);
    stop.send_reload_signal(ProcessSignal::Usr1);
    assert!(first.has_changed().unwrap());
    assert!(second.has_changed().unwrap());
    assert_eq!(*first.borrow_and_update(), Some(ProcessSignal::Usr1));
    assert!(!first.has_changed().unwrap(), "pending reloads coalesce");
    let replacement = stop.subscribe_reload_signals();
    assert!(
        !replacement.has_changed().unwrap(),
        "no replay after a child exits"
    );
}

#[test]
fn different_boot_offsets_share_the_same_interval_occurrence() {
    let early = interval_bucket(1_005, 100);
    let late = interval_bucket(1_095, 100);
    let next = interval_bucket(1_100, 100);

    assert_eq!(early, 1_000);
    assert_eq!(late, early);
    assert_eq!(next, 1_100);
}

#[test]
fn early_cron_wake_waits_and_never_selects_the_same_occurrence_again() {
    use std::str::FromStr;

    use chrono::{TimeZone, Utc};
    use cron::Schedule;

    let schedule = Schedule::from_str("0 * * * * *").expect("cron schedule");
    let occurrence = Utc.with_ymd_and_hms(2026, 9, 10, 1, 26, 0).unwrap();
    let early_wake = occurrence - chrono::Duration::milliseconds(4);

    assert_eq!(
        cron_wait_delay(occurrence, early_wake),
        Some(Duration::from_millis(4))
    );

    let search_after = cron_search_after(early_wake, Some(occurrence));
    let next = schedule.after(&search_after).next().expect("next occurrence");
    assert_eq!(next, occurrence + chrono::Duration::minutes(1));
}

#[tokio::test]
async fn unlimited_attempt_limit_retries_until_success() {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_task = Arc::clone(&calls);
    let mut runner =
        TaskRunner::from(move |_: AppState, _: CancellationToken| {
            let calls = Arc::clone(&calls_for_task);
            async move {
                if calls.fetch_add(1, Ordering::SeqCst) < 2 {
                    Err(Error::other("retry"))
                } else {
                    Ok(())
                }
            }
        });
    runner.set_name("retrying");

    let parsed: TaskServiceConfig = serde_yaml::from_str(
        r#"
tasks:
  retrying:
    timing: { kind: daemon }
    attempts: unlimited
    backoff: 0s
"#,
    )
    .expect("task config");
    let cfg = parsed.tasks.get("retrying").expect("retrying").clone();
    let state = AppState::standalone("task-runner-test", PathBuf::from("."));

    timeout(
        Duration::from_secs(1),
        Arc::new(runner).run_with_retries(
            1,
            state,
            cfg,
            TaskScope::Fleet,
            TaskDispatch::Each,
        ),
    )
    .await
    .expect("unlimited retry completed");

    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn scheduled_execution_timeout_bounds_the_whole_run() {
    let mut runner =
        TaskRunner::from(|_: AppState, _: CancellationToken| async move {
            std::future::pending::<crate::TaskResult<()>>().await
        });
    runner.set_name("bounded");
    let runner = Arc::new(runner);
    let parsed: TaskServiceConfig = serde_yaml::from_str(
        r#"
tasks:
  bounded:
    timing: { kind: interval, every: 1h, execution_timeout: 10ms }
"#,
    )
    .expect("task config");
    let cfg = parsed.tasks["bounded"].clone();
    let state = AppState::standalone("task-runner-test", PathBuf::from("."));

    timeout(
        Duration::from_secs(1),
        Arc::clone(&runner).run_with_retries(
            1,
            state,
            cfg,
            TaskScope::Fleet,
            TaskDispatch::Each,
        ),
    )
    .await
    .expect("scheduled execution timeout completed");

    assert_eq!(
        runner.last_execution().map(|outcome| outcome.result),
        Some(TaskExecutionResult::TimedOut)
    );
}
