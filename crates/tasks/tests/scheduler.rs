use super::{DeadlineQueue, OccurrenceGuard, ScheduleStatus};
use chrono::{TimeZone, Utc};
use std::sync::Arc;
use std::sync::atomic::Ordering;

#[test]
fn due_deadlines_are_ordered_and_future_work_stays_registered() {
    let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let mut queue = DeadlineQueue::<u8>::default();
    let future = queue.insert(now + chrono::Duration::seconds(1), 1);
    let first = queue.insert(now, 2);
    let second = queue.insert(now, 3);
    assert_eq!(queue.pop_due(now), Some((now, first)));
    assert_eq!(queue.pop_due(now), Some((now, second)));
    assert_eq!(queue.pop_due(now), None);
    assert_eq!(queue.entries.get(&future), Some(&1));
}

#[test]
fn removal_releases_only_the_selected_deadline() {
    let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let mut queue = DeadlineQueue::<u8>::default();
    let removed = queue.insert(now, 1);
    let retained = queue.insert(now + chrono::Duration::seconds(1), 2);
    assert_eq!(queue.remove(removed), Some(1));
    assert_eq!(queue.pop_due(now), None);
    assert_eq!(
        queue.pop_due(now + chrono::Duration::seconds(1)),
        Some((now + chrono::Duration::seconds(1), retained))
    );
}

#[test]
fn closed_registration_drains_admitted_occurrences_before_finishing() {
    let status = Arc::new(ScheduleStatus::default());
    status.pending.store(1, Ordering::Release);
    status.closed.store(true, Ordering::Release);
    let guard = OccurrenceGuard(Arc::clone(&status));
    assert!(!status.finished());
    drop(guard);
    assert!(status.finished());
}

#[test]
fn scheduler_cadence_accepts_seconds_and_defaults_to_one_minute() {
    let default: crate::config::TaskServiceConfig =
        serde_yaml::from_str("scheduler: {}\n").unwrap();
    let seconds: crate::config::TaskServiceConfig =
        serde_yaml::from_str("scheduler: { poll_interval: 1s }\n").unwrap();
    assert_eq!(default.scheduler.poll_interval.as_secs(), 60);
    assert_eq!(seconds.scheduler.poll_interval.as_secs(), 1);
}

#[test]
fn overlap_skip_excludes_a_running_occurrence_but_allow_does_not_wait() {
    let limit = Arc::new(tokio::sync::Semaphore::new(1));
    let first = crate::TaskOverlap::Skip.reserve(&limit).unwrap();
    assert!(crate::TaskOverlap::Skip.reserve(&limit).is_err());
    assert!(crate::TaskOverlap::Allow.reserve(&limit).unwrap().is_none());
    assert!(crate::TaskOverlap::Allow.reserve(&limit).unwrap().is_none());
    drop(first);
    assert!(crate::TaskOverlap::Skip.reserve(&limit).unwrap().is_some());
}

#[test]
fn overlap_configuration_defaults_to_skip_and_rejects_unknown_policy() {
    assert_eq!(
        crate::TaskConfig::from_yaml(
            "timing: { kind: cron, expression: '0 * * * * *' }"
        )
        .unwrap()
        .timing
        .overlap(),
        Some(crate::TaskOverlap::Skip)
    );
    assert_eq!(
        crate::TaskConfig::from_yaml(
            "timing: { kind: interval, every: 1s, overlap: allow }"
        )
        .unwrap()
        .timing
        .overlap(),
        Some(crate::TaskOverlap::Allow)
    );
    assert!(
        crate::TaskConfig::from_yaml(
            "timing: { kind: interval, every: 1s, overlap: run }"
        )
        .is_err()
    );
    assert!(crate::TaskConfig::from_yaml("overlap: skip").is_err());
    assert!(
        crate::TaskConfig::from_yaml("timing: { kind: manual, overlap: skip }")
            .is_err()
    );
    assert!(
        crate::TaskConfig::from_yaml(
            "timing: { kind: daemon, overlap: allow }"
        )
        .is_err()
    );
}
