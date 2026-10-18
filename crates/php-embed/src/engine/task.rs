//! PHP task executions run on reserved OS/ZTS threads. Each occurrence owns
//! one Zend execution scope for its lifetime, with only a script path,
//! environment, task identity, and cooperative cancellation token.
//! The process SAPI and thread prologue are shared with the separate HTTP pool.

use std::path::PathBuf;
use std::thread::JoinHandle;
use std::time::Duration;

use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

use super::program::{PhpProgram, run};
use crate::config::TaskProgram;

struct TaskThread {
    identity: String,
    handle: JoinHandle<Result<(), String>>,
}

/// Owned by one TaskService occurrence through execution and joined drain.
/// The occurrence retains every OS handle until its thread completes.
pub(crate) struct TaskExecutions {
    threads: Vec<TaskThread>,
    cancellation: CancellationToken,
    /// Identity of the current generation, refreshed with `cancellation`.
    /// A drain interrupts the executions carrying this and no others.
    owner: super::interrupt::ExecutionOwner,
    failed: bool,
}

impl Default for TaskExecutions {
    fn default() -> Self {
        Self {
            threads: Vec::new(),
            cancellation: CancellationToken::new(),
            owner: super::interrupt::ExecutionOwner::next(),
            failed: false,
        }
    }
}

impl TaskExecutions {
    pub(crate) fn failed(&self) -> bool {
        self.failed
    }

    pub(crate) async fn start_instance(
        &mut self,
        program: &TaskProgram,
        instance: u8,
    ) -> Result<(), String> {
        self.reap_finished();
        if !self.threads.is_empty() {
            return Err(format!(
                "{} dedicated PHP instance(s) from the previous generation are still running",
                self.threads.len()
            ));
        }
        self.failed = false;

        // A relative script resolves against the program's own working
        // directory when it declares one, and against the process working
        // directory otherwise — the same rule a command task's `cwd` gives
        // its `program`.
        let candidate = match (&program.cwd, program.script_path.is_absolute())
        {
            (Some(cwd), false) => cwd.join(&program.script_path),
            _ => program.script_path.clone(),
        };
        let script = std::fs::canonicalize(&candidate).map_err(|err| {
            format!(
                "PHP program '{}': {}: {err}",
                program.name,
                candidate.display()
            )
        })?;
        if !script.is_file() {
            return Err(format!(
                "PHP program '{}': {} is not a file",
                program.name,
                script.display()
            ));
        }

        self.cancellation = CancellationToken::new();
        // New generation, new identity: a drain of the previous one must not
        // reach the threads this one is about to spawn.
        self.owner = super::interrupt::ExecutionOwner::next();

        let identity = format!("{}:{instance}", program.name);
        let thread_identity = identity.clone();
        let cancellation = self.cancellation.clone();
        let owner = self.owner;
        let program = program.clone();
        let (started_tx, started_rx) = oneshot::channel();
        let handle = match std::thread::Builder::new()
            .name(format!("claviron-php-dedicated-{identity}"))
            .spawn(move || {
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    run_task_program(
                        program,
                        instance,
                        script,
                        cancellation,
                        started_tx,
                        owner,
                    )
                }))
                .unwrap_or_else(|_| Err("PHP thread panicked".to_string()))
                .map_err(|err| {
                    format!("dedicated PHP instance '{thread_identity}': {err}")
                })
            }) {
            Ok(handle) => handle,
            Err(err) => {
                let error = format!(
                    "failed to spawn dedicated PHP instance '{identity}': {err}"
                );
                error!(instance = %identity, %error, "dedicated PHP instance failed to spawn");
                // OS thread creation is a manager failure, unlike a script
                // outcome. The caller drains accepted work.
                return Err(error);
            }
        };
        self.threads.push(TaskThread { identity, handle });
        // The JoinHandle is the authoritative outcome, including a startup
        // error or panic. Do not duplicate it via this signal.
        let _ = started_rx.await;
        Ok(())
    }

    pub(crate) fn running(&self) -> usize {
        self.threads
            .iter()
            .filter(|thread| !thread.handle.is_finished())
            .count()
    }

    /// Log and reap completed instances while peers continue. Script outcomes
    /// never become manager failures. An empty group completes immediately;
    /// no successful or failed instance is respawned.
    pub(crate) async fn wait(&mut self) {
        loop {
            self.reap_finished();
            if self.threads.is_empty() {
                return;
            }
            // As in drain, is_finished includes TLS teardown. No extra blocking
            // executor thread is reserved merely to join each resident thread.
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Slack for an interrupted execution to unwind, not for it to notice.
    ///
    /// It is polled rather than slept through in one piece. `EG(vm_interrupt)`
    /// is observed between opcodes, so a script inside a blocking C call —
    /// `sleep()`, a socket read — does not see it until that call returns. One
    /// flat sleep of this length would give a `queue:work --sleep=1` exactly
    /// one chance to be looking.
    const INTERRUPT_GRACE: Duration = Duration::from_secs(5);

    /// Request cooperative shutdown and wait for actual OS-thread completion.
    ///
    /// `None` is deliberately indefinite and never interrupts: that is
    /// [GRACEFUL_OWNERSHIP: NO_IMPLICIT_TERMINATION][graceful-ownership].
    /// An operator who wrote no deadline gets no kill. A deadline authorises the
    /// opposite, so it escalates rather than merely giving up.
    ///
    /// `drain` against a deadline the caller owns.
    ///
    /// The caller owns it because one drain is reached through two call sites
    /// in the slot supervisor — the attempt loop and the final teardown — and
    /// a `Duration` would hand each of them the full grace, letting one stuck
    /// slot spend the budget twice.
    ///
    /// [graceful-ownership]: https://github.com/claviron/claviron/blob/main/docs/design/GRACEFUL_OWNERSHIP.md
    pub(crate) async fn drain_until(
        &mut self,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<(), String> {
        let started = tokio::time::Instant::now();
        // Nothing to drain is not an outcome worth a line. The slot supervisor
        // reaches this twice — the attempt loop and the final teardown — so the
        // second call routinely finds the work already gone, and reporting that
        // as a cooperative stop credits cooperation that never happened.
        let had_work = !self.threads.is_empty();
        self.cancellation.cancel();
        if had_work {
            // Said before the wait, not after it. Whatever these instances
            // print on their way out — and a framework on a fatal prints a
            // lot — arrives with nothing naming it, so the name has to be
            // already on the page. It also turns an otherwise silent grace
            // into something an operator can read: this is who we are waiting
            // for, and this is how long before it stops being a request.
            debug!(
                instances = %self.identities(),
                grace_ms = deadline
                    .map(|deadline| deadline.saturating_duration_since(started).as_millis() as i64)
                    .unwrap_or(-1),
                "🔸 draining dedicated PHP; waiting for instances to stop"
            );
        }
        let mut signalled: Option<usize> = None;
        let mut interrupt_deadline = None;
        loop {
            self.reap_finished_after(signalled.is_some());
            if self.threads.is_empty() {
                if !had_work {
                    return Ok(());
                }
                // Said out loud, and at the severity the outcome earns: a drain
                // that needed the interrupt means a script ignored cancellation,
                // which the operator wants to know about even though it worked.
                match signalled {
                    None => debug!(
                        elapsed_ms = started.elapsed().as_millis(),
                        "dedicated PHP drain complete — instances stopped cooperatively"
                    ),
                    // The ERROR already fired when the interrupt was raised;
                    // this only closes the story with how long it took.
                    Some(signalled) => debug!(
                        elapsed_ms = started.elapsed().as_millis(),
                        signalled,
                        "dedicated PHP drain complete, but only by interrupt"
                    ),
                }
                return Ok(());
            }

            let now = tokio::time::Instant::now();
            if deadline.is_some_and(|deadline| now >= deadline) {
                // Cancellation here is cooperative — `claviron_is_cancelled()`
                // — and a loop that never asks will never stop. Having spent
                // the full grace waiting for it to ask, the remaining option is
                // to end the execution from outside.
                if signalled.is_none() {
                    let count = crate::engine::interrupt::interrupt_owned_by(
                        self.owner,
                    );
                    // What this line reports is the *action*. The reason lives
                    // where it belongs: the interrupt takes PHP's own timeout
                    // path, so PHP raises its ordinary fatal naming the script
                    // and line it was on, in the PHP log. It reads "Maximum
                    // execution time of 0 seconds exceeded" — the 0 is this
                    // runtime's deliberate ini value, not a budget that ran out.
                    // ERROR, not warn: this is the moment work is dropped. The
                    // severity tracks whether someone's execution was ended, not
                    // whether this machinery did its job — it did, and that is
                    // exactly when an operator needs to see it.
                    error!(
                        instances = %self.identities(),
                        running = self.running(),
                        signalled = count,
                        grace_ms = started.elapsed().as_millis(),
                        "🚨 dedicated PHP drain expired — cancellation was never observed; \
                         interrupting. PHP names the script and line in its own fatal"
                    );
                    signalled = Some(count);
                    interrupt_deadline = Some(now + Self::INTERRUPT_GRACE);
                } else if interrupt_deadline.is_some_and(|until| now >= until) {
                    // The interrupt was raised and the thread still did not
                    // end, which means it is blocked outside the VM — `sleep`,
                    // a socket read — where no interrupt point is reached. This
                    // is the one outcome nothing here can fix.
                    return Err(format!(
                        "gave up after {}ms: interrupted {} execution(s), {} still running and \
                         now retained — blocked outside the VM, where an interrupt cannot land",
                        started.elapsed().as_millis(),
                        signalled.unwrap_or(0),
                        self.running()
                    ));
                }
            }

            // JoinHandle::is_finished includes TLS destructors. Polling avoids
            // allocating one extra blocking-pool thread per resident instance.
            let wake = tokio::time::Instant::now() + Duration::from_millis(10);
            let next = match (deadline, interrupt_deadline) {
                // Before the interrupt, do not sleep past the grace; after it,
                // the interrupt deadline is the only one still ahead.
                (Some(deadline), None) => deadline.min(wake),
                (_, Some(until)) => until.min(wake),
                (None, None) => wake,
            };
            tokio::time::sleep_until(next).await;
        }
    }

    /// The instances still resident, for a log line that has to say which.
    fn identities(&self) -> String {
        self.threads
            .iter()
            .map(|thread| thread.identity.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn reap_finished(&mut self) {
        self.reap_finished_after(false);
    }

    /// `interrupted` says this drain ended the execution itself.
    ///
    /// It changes what a failed join *means*. `zend_timeout()` unwinds as a
    /// bailout, so an interrupted instance always joins with "A bailout
    /// occurred during the execution" — reporting that as an unexplained
    /// instance failure, at error, right after the line saying we interrupted
    /// it, tells the same story twice and blames the script for what the host
    /// did.
    fn reap_finished_after(
        &mut self,
        interrupted: bool,
    ) {
        let mut index = 0;
        while index < self.threads.len() {
            if !self.threads[index].handle.is_finished() {
                index += 1;
                continue;
            }
            let thread = self.threads.swap_remove(index);
            let result = thread.handle.join().unwrap_or_else(|_| {
                Err(format!(
                    "dedicated PHP instance '{}' panicked during thread teardown",
                    thread.identity
                ))
            });
            match result {
                Ok(()) => {
                    debug!(instance = %thread.identity, "🧩 dedicated PHP instance completed; thread joined")
                }
                Err(error) if interrupted => {
                    error!(instance = %thread.identity, %error, "dedicated PHP instance ended by interrupt; thread joined");
                }
                Err(error) => {
                    self.failed = true;
                    error!(instance = %thread.identity, %error, "❌ dedicated PHP instance failed; thread joined");
                }
            }
        }
    }
}

impl Drop for TaskExecutions {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

pub(super) fn run_task_program(
    program: TaskProgram,
    instance: u8,
    script: PathBuf,
    cancellation: CancellationToken,
    started: oneshot::Sender<Result<(), String>>,
    owner: super::interrupt::ExecutionOwner,
) -> Result<(), String> {
    let cwd = program.cwd.clone();
    let output = program.log.open(cwd.as_deref(), &program.name);
    let program = PhpProgram::new(
        program.name,
        script,
        program.args,
        program.cwd,
        program.environment,
        program.execution_timeout,
    )
    .with_output(output);
    run(
        program,
        super::ProgramModel::Task,
        instance,
        cancellation,
        started,
        owner,
    )
}
