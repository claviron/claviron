//! One service-owned cron deadline queue. Waiting is not a task execution.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use claviron_helpers::sync::LockUnpoisoned;
use cron::Schedule;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::runner::{ScheduleContext, TaskRunner};

pub(crate) fn register(
    builder: &mut crate::service::TaskServiceBuilder
) -> Result<(), String> {
    let mut runner = TaskRunner::from(
        |state: claviron_runtime::AppState, token: CancellationToken| async move {
            let service = state
                .registry_ref()
                .resolve::<crate::service::TaskService>()
                .ok_or_else(|| {
                    crate::Error::other("scheduler requires the task service")
                })?;
            let interval = service
                .inner
                .tasks
                .load()
                .get("scheduler")
                .and_then(|cfg| cfg.scheduler.as_ref())
                .map_or_else(crate::SchedulerConfig::default, Clone::clone)
                .poll_interval;
            service.inner.scheduler.run(token, interval).await
        },
    );
    runner.set_task_scope(crate::TaskScope::Fleet);
    runner.set_task_dispatch(crate::TaskDispatch::Each);
    builder.register_builtin(
        "scheduler",
        crate::TaskConfig::from_yaml(
            "timing: { kind: daemon }\nscope: fleet\ndispatch: each\n",
        )?,
        runner,
    );
    Ok(())
}

#[derive(Default)]
struct DeadlineQueue<T> {
    next_id: u64,
    deadlines: BinaryHeap<Reverse<(DateTime<Utc>, u64)>>,
    entries: HashMap<u64, T>,
}

impl<T> DeadlineQueue<T> {
    fn insert(
        &mut self,
        deadline: DateTime<Utc>,
        entry: T,
    ) -> u64 {
        self.next_id =
            self.next_id.checked_add(1).expect("schedule id exhausted");
        let id = self.next_id;
        self.entries.insert(id, entry);
        self.deadlines.push(Reverse((deadline, id)));
        id
    }

    fn remove(
        &mut self,
        id: u64,
    ) -> Option<T> {
        self.deadlines.retain(|Reverse((_, key))| *key != id);
        self.entries.remove(&id)
    }

    fn next(&self) -> Option<DateTime<Utc>> {
        self.deadlines.peek().map(|Reverse((deadline, _))| *deadline)
    }

    fn pop_due(
        &mut self,
        now: DateTime<Utc>,
    ) -> Option<(DateTime<Utc>, u64)> {
        if self.next().is_some_and(|deadline| deadline <= now) {
            self.deadlines.pop().map(|Reverse(item)| item)
        } else {
            None
        }
    }
}

struct Registration {
    schedule: Schedule,
    runner: Arc<TaskRunner>,
    run: ScheduleContext,
    status: Arc<ScheduleStatus>,
}

impl Registration {
    fn stop_metrics(&self) {
        #[cfg(feature = "metrics")]
        {
            let role = crate::metrics::role_label_from_run_mode(
                self.run.state.run_mode(),
            );
            crate::metrics::set_workers_healthy(self.runner.name(), &role, 0);
        }
    }
}

#[derive(Default)]
struct ScheduleStatus {
    closed: AtomicBool,
    pending: AtomicUsize,
    changed: Notify,
}

impl ScheduleStatus {
    fn finished(&self) -> bool {
        self.closed.load(Ordering::Acquire)
            && self.pending.load(Ordering::Acquire) == 0
    }
}

struct OccurrenceGuard(Arc<ScheduleStatus>);

impl Drop for OccurrenceGuard {
    fn drop(&mut self) {
        self.0.pending.fetch_sub(1, Ordering::AcqRel);
        self.0.changed.notify_one();
    }
}

/// A registration, not a parked coroutine. Removal wakes the shared scheduler
/// and completion includes bodies already admitted before removal.
pub(crate) struct ScheduleHandle {
    scheduler: Arc<Scheduler>,
    id: u64,
    status: Arc<ScheduleStatus>,
}

impl ScheduleHandle {
    pub(crate) fn stop(&self) {
        self.scheduler.remove(self.id);
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.status.finished()
    }

    pub(crate) async fn stopped(&self) {
        loop {
            let changed = self.status.changed.notified();
            if self.is_finished() {
                return;
            }
            changed.await;
        }
    }
}

impl Drop for ScheduleHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

pub(crate) struct Scheduler {
    queue: Mutex<DeadlineQueue<Registration>>,
    running: AtomicBool,
}

impl Default for Scheduler {
    fn default() -> Self {
        Self {
            running: AtomicBool::new(false),
            queue: Mutex::new(DeadlineQueue {
                next_id: 0,
                deadlines: BinaryHeap::new(),
                entries: HashMap::new(),
            }),
        }
    }
}

impl Scheduler {
    pub(crate) fn register(
        self: &Arc<Self>,
        expression: &str,
        runner: Arc<TaskRunner>,
        run: ScheduleContext,
    ) -> Result<ScheduleHandle, String> {
        let schedule =
            Schedule::from_str(expression).map_err(|e| e.to_string())?;
        let next = schedule
            .after(&Utc::now())
            .next()
            .ok_or_else(|| "cron produced no next occurrence".to_owned())?;
        let status = Arc::new(ScheduleStatus::default());
        #[cfg(feature = "metrics")]
        {
            let role =
                crate::metrics::role_label_from_run_mode(run.state.run_mode());
            crate::metrics::set_workers_total(runner.name(), &role, 1);
            crate::metrics::set_workers_healthy(runner.name(), &role, 1);
        }
        let id = self.queue.lock_unpoisoned().insert(
            next,
            Registration { schedule, runner, run, status: Arc::clone(&status) },
        );
        Ok(ScheduleHandle { scheduler: Arc::clone(self), id, status })
    }

    fn remove(
        &self,
        id: u64,
    ) {
        if let Some(entry) = self.queue.lock_unpoisoned().remove(id) {
            entry.stop_metrics();
            entry.run.token.cancel();
            entry.status.closed.store(true, Ordering::Release);
            entry.status.changed.notify_one();
        }
    }

    fn fire_due(
        &self,
        now: DateTime<Utc>,
        cancel: &CancellationToken,
    ) {
        let mut queue = self.queue.lock_unpoisoned();
        while !cancel.is_cancelled() {
            let Some((due, id)) = queue.pop_due(now) else { break };
            let Some(entry) = queue.entries.get(&id) else { continue };
            if entry.run.token.is_cancelled() {
                let entry = queue.entries.remove(&id).unwrap();
                entry.stop_metrics();
                entry.status.closed.store(true, Ordering::Release);
                entry.status.changed.notify_one();
                continue;
            }
            if let Some(execution) =
                entry.runner.scheduled_occurrence(entry.run.clone())
            {
                entry.status.pending.fetch_add(1, Ordering::AcqRel);
                let guard = OccurrenceGuard(Arc::clone(&entry.status));
                tokio::spawn(async move {
                    let _guard = guard;
                    execution.await;
                });
            }
            if let Some(next) = entry
                .schedule
                .after(&crate::runner::cron_search_after(now, Some(due)))
                .next()
            {
                queue.deadlines.push(Reverse((next, id)));
            } else {
                let entry = queue.entries.remove(&id).unwrap();
                entry.stop_metrics();
                entry.status.closed.store(true, Ordering::Release);
                entry.status.changed.notify_one();
                error!(
                    task = entry.runner.name(),
                    "cron produced no next occurrence"
                );
            }
        }
    }

    pub(crate) async fn run(
        &self,
        cancel: CancellationToken,
        poll_interval: std::time::Duration,
    ) -> crate::TaskResult<()> {
        if self
            .running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(crate::Error::other("scheduler is already running"));
        }
        struct Running<'a>(&'a AtomicBool);
        impl Drop for Running<'_> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::Release);
            }
        }
        let _running = Running(&self.running);
        let mut poll = tokio::time::interval(poll_interval);
        poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                _ = poll.tick() => self.fire_due(Utc::now(), &cancel),
            }
        }
        // Stopping this task pauses admission, not registrations or running
        // bodies. Their lifecycle belongs to the individual task generation.
        Ok(())
    }
}

#[cfg(test)]
#[path = "../tests/scheduler.rs"]
mod tests;
