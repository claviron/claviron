//! Sole lifecycle owner of a fixed set of PHP execution instances.
use super::dispatch::{Control, Mailbox};
use super::{ApplicationRequests, INBOX, Inbox, Job};
use crate::PhpApplication;
use crate::engine::interrupt::ExecutionOwner;
use crate::engine::program::{PhpProgram, run};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tokio::sync::{Notify, mpsc, oneshot};

struct Slot {
    id: u8,
    tx: Option<mpsc::Sender<Job>>,
    control: Arc<Control>,
    handle: Option<std::thread::JoinHandle<Result<(), String>>>,
    joining: Option<oneshot::Receiver<Result<(), String>>>,
    failures: u32,
    retry_at: Option<Instant>,
}

/// Only the lifecycle service mutates slots. HTTP holds ApplicationRequests,
/// which contains no mutex, slot owner, retry timer or thread constructor.
pub(crate) struct ApplicationWorkers {
    definition: PhpApplication,
    slots: Vec<Slot>,
    pub(crate) requests: Arc<ApplicationRequests>,
    lifecycle: Arc<Notify>,
    owner: ExecutionOwner,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct PhpApplicationSnapshot {
    pub accepting: bool,
    pub instances: usize,
    pub ready: usize,
    pub busy: usize,
    pub retiring: usize,
    pub backing_off: usize,
    pub exhausted: usize,
}

impl ApplicationWorkers {
    pub(crate) fn new(
        mut definition: PhpApplication,
        lifecycle: Arc<Notify>,
    ) -> Result<Self, String> {
        definition.validate()?;
        definition.script_path = std::fs::canonicalize(&definition.script_path)
            .map_err(|e| e.to_string())?;
        if !definition.script_path.is_file() {
            return Err("application script is not a file".into());
        }
        let requests = ApplicationRequests::new(
            definition.max_waiting,
            Arc::clone(&lifecycle),
        );
        let slots = (0..definition.instances)
            .map(|id| Slot {
                id,
                tx: None,
                control: Arc::new(Control::new(
                    Arc::clone(&requests.wake),
                    Arc::clone(&lifecycle),
                )),
                handle: None,
                joining: None,
                failures: 0,
                retry_at: Some(Instant::now()),
            })
            .collect();
        Ok(Self {
            definition,
            slots,
            requests,
            lifecycle,
            owner: ExecutionOwner::next(),
        })
    }

    pub(crate) fn snapshot(&self) -> PhpApplicationSnapshot {
        let accepting = self.accepting();
        PhpApplicationSnapshot {
            accepting,
            instances: self
                .slots
                .iter()
                .filter(|s| s.handle.is_some() || s.joining.is_some())
                .count(),
            ready: self.slots.iter().filter(|s| s.control.ready()).count(),
            busy: self.slots.iter().filter(|s| s.control.busy()).count(),
            retiring: self
                .slots
                .iter()
                .filter(|s| {
                    !accepting && (s.handle.is_some() || s.joining.is_some())
                })
                .count(),
            backing_off: self
                .slots
                .iter()
                .filter(|s| {
                    s.handle.is_none()
                        && s.joining.is_none()
                        && !self.exhausted(s)
                })
                .count(),
            exhausted: self.slots.iter().filter(|s| self.exhausted(s)).count(),
        }
    }

    fn exhausted(
        &self,
        slot: &Slot,
    ) -> bool {
        self.definition.retry.exhausted(slot.failures)
    }

    fn spawn(
        &self,
        slot: &mut Slot,
    ) {
        let (tx, rx) = mpsc::channel(1);
        let program = PhpProgram::new(
            self.definition.name.clone(),
            self.definition.script_path.clone(),
            self.definition.args.clone(),
            // Applications are reached by path, not started from a shell, so
            // they carry no working directory of their own yet.
            None,
            self.definition.environment.clone(),
            // Applications arm and disarm per request by hand in
            // `functions::application`; the program itself is unbounded.
            None,
        );
        slot.control = Arc::new(Control::new(
            Arc::clone(&self.requests.wake),
            Arc::clone(&self.lifecycle),
        ));
        let control = Arc::clone(&slot.control);
        let owner = self.owner;
        let execution_timeout = crate::engine::interrupt::resolve(
            self.definition.execution_timeout,
        );
        let id = slot.id;
        let runtime = tokio::runtime::Handle::current();
        let handle = std::thread::Builder::new()
            .name(format!(
                "claviron-php-application-{}-{id}",
                self.definition.name
            ))
            .spawn(move || {
                struct ExitGuard(Arc<Control>);
                impl Drop for ExitGuard {
                    fn drop(&mut self) {
                        self.0.exited();
                    }
                }
                let _exit = ExitGuard(Arc::clone(&control));
                struct InboxGuard;
                impl Drop for InboxGuard {
                    fn drop(&mut self) {
                        INBOX.with(|cell| {
                            cell.borrow_mut().take();
                        });
                    }
                }
                INBOX.with(|cell| {
                    *cell.borrow_mut() = Some(Inbox {
                        rx,
                        runtime,
                        current: None,
                        outgoing: None,
                        responded: false,
                        event_handled: false,
                        control: Arc::clone(&control),
                        execution_timeout,
                    })
                });
                let _inbox = InboxGuard;
                let (started, _) = oneshot::channel();
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    run(
                        program,
                        crate::engine::ProgramModel::Application,
                        id,
                        control.cancel.clone(),
                        started,
                        owner,
                    )
                }))
                .unwrap_or_else(|_| {
                    Err("application PHP thread panicked".into())
                })
            });
        match handle {
            Ok(handle) => {
                slot.tx = Some(tx);
                slot.handle = Some(handle);
            }
            Err(error) => {
                self.failed(slot, &error.to_string());
            }
        }
    }

    fn failed(
        &self,
        slot: &mut Slot,
        error: &str,
    ) {
        if slot.control.served.load(Ordering::Relaxed) > 0 {
            slot.failures = 0;
        }
        slot.failures = slot.failures.saturating_add(1);
        let delay = self.definition.retry.delay_for(slot.failures);
        slot.retry_at = (!self.exhausted(slot)).then(|| Instant::now() + delay);
        if self.exhausted(slot) {
            tracing::error!(application = %self.definition.name, instance = slot.id, attempts = slot.failures, %error, "application instance failed; retry budget exhausted");
        } else {
            tracing::warn!(application = %self.definition.name, instance = slot.id, attempts = slot.failures, %error, ?delay, "application instance stopped; replacement scheduled after join");
        }
    }

    pub(crate) fn maintain(&mut self) {
        // Move the fixed slots out while applying lifecycle-only operations.
        // Requests cannot see this owner or cause a new slot to be allocated.
        let mut slots = std::mem::take(&mut self.slots);
        let mut changed = false;
        for slot in &mut slots {
            if slot.control.finished()
                && let Some(handle) = slot.handle.take()
            {
                slot.tx.take();
                let (finished, completion) = oneshot::channel();
                slot.joining = Some(completion);
                let lifecycle = Arc::clone(&self.lifecycle);
                // Join only after the instance has left its execution loop.
                // This is lifecycle teardown, never HTTP request execution.
                drop(tokio::task::spawn_blocking(move || {
                    let result = handle.join().unwrap_or_else(|_| {
                        Err("PHP thread teardown panicked".into())
                    });
                    let _ = finished.send(result);
                    lifecycle.notify_one();
                }));
                changed = true;
            }
            let joined = slot.joining.as_mut().and_then(|completion| {
                match completion.try_recv() {
                    Ok(result) => Some(result),
                    Err(oneshot::error::TryRecvError::Closed) => {
                        Some(Err("PHP instance join failed".into()))
                    }
                    Err(oneshot::error::TryRecvError::Empty) => None,
                }
            });
            if let Some(result) = joined {
                slot.joining = None;
                if self.accepting() {
                    self.failed(
                        slot,
                        result
                            .as_ref()
                            .err()
                            .map(String::as_str)
                            .unwrap_or("application script returned"),
                    );
                }
                changed = true;
            }
            if self.accepting()
                && slot.handle.is_none()
                && slot.joining.is_none()
                && slot.retry_at.is_some_and(|at| at <= Instant::now())
            {
                slot.retry_at = None;
                self.spawn(slot);
                changed = true;
            }
        }
        self.slots = slots;
        if changed && self.accepting() {
            self.requests.dispatch.store(Arc::new(
                self.slots
                    .iter()
                    .filter_map(|slot| {
                        slot.tx.as_ref().map(|sender| {
                            Mailbox::new(sender, Arc::clone(&slot.control))
                        })
                    })
                    .collect(),
            ));
            self.requests.wake.notify_waiters();
        }
    }

    pub(crate) fn next_retry(&self) -> Option<Instant> {
        self.accepting()
            .then(|| self.slots.iter().filter_map(|s| s.retry_at).min())
            .flatten()
    }
    pub(crate) fn owner(&self) -> ExecutionOwner {
        self.owner
    }
    pub(crate) fn accepting(&self) -> bool {
        self.requests.accepting.load(Ordering::Acquire)
    }
    pub(crate) fn stop(&mut self) {
        self.requests.stop();
        for slot in &mut self.slots {
            slot.control.retire();
            slot.tx.take();
            slot.retry_at = None;
        }
    }
    pub(crate) fn drained(&self) -> bool {
        !self.accepting()
            && self
                .slots
                .iter()
                .all(|s| s.handle.is_none() && s.joining.is_none())
            && self.requests.admission.available_permits()
                == self.definition.max_waiting
    }
}
impl Drop for ApplicationWorkers {
    fn drop(&mut self) {
        self.stop();
    }
}
