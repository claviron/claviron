//! Ending a PHP execution that will not end itself.
//!
//! Embedding PHP took away the escape hatch every other runner has. php-fpm
//! kills the worker process on `request_terminate_timeout`; RoadRunner kills
//! the worker; `claviron-php-fcgi` drops the socket and lets FPM do it. A
//! script running on one of this process's own ZTS threads has none of that:
//! the thread cannot be killed safely, so a runaway script holds its pool slot
//! until it finishes, a drain waits for something that will never arrive, and
//! stopping the service turns into killing it.
//!
//! What PHP does offer is the mechanism behind `max_execution_time`. The VM
//! checks `EG(vm_interrupt)` at its interrupt points; when the flag is set it
//! looks at `EG(timed_out)` and, if that is set too, calls `zend_timeout()` —
//! which raises the ordinary "Maximum execution time exceeded" fatal error and
//! unwinds through machinery PHP already owns. Setting those two flags from
//! outside the thread is therefore not a trick: it is the same path PHP takes
//! for its own timeout, reached by a different hand.
//!
//! Deliberately no `zend_interrupt_function` of our own and no `zend_bailout`
//! call. This crate has already been burned once by firing a bailout from the
//! wrong place — see `sapi::handle_abort_connection`, where doing it during
//! request shutdown hung the worker thread for good. Using PHP's path means
//! never having to answer the question of where a bailout is safe.
//!
//! One cosmetic consequence of using PHP's path: the fatal error names
//! `EG(timeout_seconds)`, so an execution ended by [`interrupt_owned_by`]
//! rather than by an expired budget reports "Maximum execution time of 0
//! seconds exceeded" — zero being the ini value this runtime sets
//! deliberately. The log line that precedes it says what actually happened.
//!
//! **Everything here is scoped by owner and by activity.** The registry holds
//! every PHP thread in the process, but the three execution owners — script
//! pool, task execution, application pool — run their own generations side by
//! side and each drains independently. [`ExecutionOwner`] is what lets a drain say
//! "mine"; [`Active`] is what keeps it off a thread that is between requests.
//! Both exist because the first version of this module had neither and signalled
//! the whole process.
//!
//! **What this cannot do.** The VM only reaches an interrupt point while it is
//! executing bytecode. A script blocked in `sleep()`, a blocking socket read or
//! any other call that does not return to the VM is not interrupted until it
//! comes back — exactly the limitation PHP's own `max_execution_time` carries
//! on a build without POSIX interval timers, which is every macOS build. A busy
//! loop is interrupted; a blocking call is not.
//!
//! # What a budget bounds, per model
//!
//! A budget can only be armed around something Rust can see start and end.
//! That is not the same unit in all three models, and saying which is which is
//! part of the contract:
//!
//! * **Script** — one execution is one request, so the budget wraps it
//!   ([`Budget::from_php`]) and php.ini can describe it. See the
//!   `set_time_limit` note below.
//! * **Application** — the program outlives every request it serves, so the
//!   budget is armed when `claviron_application_receive` hands PHP a request or
//!   an event, and disarmed at the next `receive`. Both units are bounded; the
//!   program between them is not.
//! * **Task** — the host bounds the **whole program**, not an iteration. A
//!   task's internal loop is invisible from Rust: there is no call that means
//!   "one unit of work is starting", so there is nothing to arm around. A task
//!   that wants finer granularity gets it cooperatively, by calling
//!   `claviron_is_cancelled()` in its loop and returning. What the host keeps
//!   either way is the drain-time interrupt, scoped to the generation that owns
//!   the program.
//!
//! # `set_time_limit()` is not observed mid-execution
//!
//! [`Budget::from_php`] reads `EG(timeout_seconds)` once, before the script
//! runs, and turns it into a Rust deadline. A later `set_time_limit()` changes
//! PHP's field but not that deadline, so the host bound stands.
//!
//! This is the intended policy, not an accident of implementation: a budget the
//! host imposed is not something the guest may extend. It does mean a script
//! that legitimately calls `set_time_limit()` to buy itself more time will not
//! get it — if that becomes a real workload, the answer is to intercept the
//! call and re-arm **only** when the budget in force came from PHP's own
//! `max_execution_time` rather than from `lifecycle.execution_timeout`.
//! `Budget::from_php` already knows which of the two it used.

use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};
use std::{ptr, thread};

use ext_php_rs::ffi::{zend_atomic_bool_store, zend_executor_globals};
use tracing::warn;

/// How often the watchdog looks for an expired execution.
///
/// The cost of being late is a request that overruns its budget by up to this
/// much; the cost of being early is a wakeup on an idle process. A runaway
/// script is measured in seconds, so this is not a tight loop.
const SCAN_INTERVAL: Duration = Duration::from_millis(50);

/// The executor globals of one PHP thread, as a pointer that may cross to the
/// watchdog thread.
struct Globals(*mut zend_executor_globals);

// SAFETY: the pointer is only ever dereferenced under `Execution::globals`'s
// lock, which is also what teardown takes before the context is released. The
// value crosses threads; the access does not happen concurrently with the free.
unsafe impl Send for Globals {}

/// Which service generation owns a PHP execution.
///
/// The registry holds every PHP thread in the process, and the three execution
/// owners — script pool, task execution, application pool — run their own
/// generations side by side. Without an identity on each entry a drain has no way to say
/// "mine", and the only thing it can do is signal everything; that is what
/// `interrupt_all` did, and it ended other services' requests.
///
/// One identity per pool or group instance, not one per thread: a generation
/// is what a drain owns and what a restart replaces.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ExecutionOwner(u64);

impl ExecutionOwner {
    pub(crate) fn next() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);

        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

/// One PHP thread, as far as interrupting it is concerned.
struct Execution {
    /// The generation that spawned this thread.
    owner: ExecutionOwner,
    /// Whether PHP is executing on this thread right now.
    ///
    /// Separate from `deadline`, which is only set when a *budget* was armed:
    /// an execution with no budget is still an execution, and an idle thread
    /// with no budget must not be interrupted at all. Signalling an idle thread
    /// leaves `timed_out` and `vm_interrupt` set on globals that `arm` does not
    /// clear, so the next request on that thread dies of a timeout it never
    /// had.
    active: AtomicBool,
    /// The thread's executor globals, behind a lock rather than an `AtomicPtr`.
    ///
    /// On ZTS the accessor resolves through TSRM to the *calling* thread, so
    /// this can only be captured from inside the thread it describes, and it
    /// stays valid only until that thread releases its PHP context.
    ///
    /// It was an `AtomicPtr`, which published the value safely and said nothing
    /// about the allocation behind it: the watchdog could load a live pointer,
    /// the thread could then store null and release its context, and the
    /// watchdog would write into freed memory. Publication is not lifetime, and
    /// a second load does not help — the race is between the load and the
    /// dereference.
    ///
    /// The lock is the barrier. An interrupt holds it across the whole
    /// dereference and teardown takes it to store null, so a thread cannot
    /// release its context while the watchdog is inside its globals, and the
    /// watchdog cannot enter after teardown has begun. Cold on both sides: the
    /// watchdog scans every 50ms, teardown runs once per thread, and arming a
    /// budget touches only the atomics below.
    globals: Mutex<Globals>,
    /// Milliseconds from [`epoch`] at which the current work is overdue, or
    /// zero when nothing is being timed.
    deadline: AtomicU64,
    /// The armed budget in whole seconds, written into `EG(timeout_seconds)`
    /// at the moment the interrupt is raised. `arm` writes it too, but
    /// `php_execute_script` overwrites that with php.ini's
    /// `max_execution_time` before the script runs, and PHP's fatal names
    /// whatever is in the global -- so a 2 s budget was reported as "60
    /// seconds". Writing it again here, under the same lock as the flags, is
    /// what makes the message describe the deadline that actually fired.
    budget_secs: AtomicU64,
    /// `EG(timeout_seconds)` as it was before `arm` wrote the budget into it.
    ///
    /// PHP reads that global at the next request startup on this thread and
    /// arms its own timer from it. Left dirty, a budget meant for one
    /// execution becomes a process-wide `setitimer` whose `SIGPROF` lands on
    /// whichever thread the kernel picks; one without PHP on it can only log
    /// "zend_timeout_handler() called in a thread not managed by PHP".
    ini_timeout_seconds: AtomicI64,
    /// Whether the interrupt has already been raised for this stretch of work,
    /// so it is raised once rather than on every scan.
    raised: AtomicBool,
}

impl Execution {
    /// The lock, whether or not a previous holder panicked. A poisoned mutex
    /// here would mean losing the ability to interrupt for the rest of the
    /// process; the data it guards is a pointer that is either null or valid,
    /// and neither state is corrupted by a panic elsewhere.
    fn globals(&self) -> std::sync::MutexGuard<'_, Globals> {
        self.globals.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn interrupt(&self) -> bool {
        // Held across the write below, which is the whole point.
        let globals = self.globals();
        let globals = globals.0;
        if globals.is_null() || self.raised.swap(true, Ordering::AcqRel) {
            return false;
        }

        // `timed_out` first: the VM reads `vm_interrupt`, and only then asks
        // whether the interrupt was a timeout. Setting them the other way round
        // leaves a window where the interrupt is seen with no reason attached
        // and is simply swallowed.
        unsafe {
            let budget = self.budget_secs.load(Ordering::Acquire);
            if budget > 0 {
                (*globals).timeout_seconds = budget as _;
            }
            zend_atomic_bool_store(
                ptr::addr_of_mut!((*globals).timed_out),
                true,
            );
            zend_atomic_bool_store(
                ptr::addr_of_mut!((*globals).vm_interrupt),
                true,
            );
        }

        true
    }
}

fn epoch() -> Instant {
    static EPOCH: OnceLock<Instant> = OnceLock::new();

    *EPOCH.get_or_init(Instant::now)
}

fn now_ms() -> u64 {
    epoch().elapsed().as_millis() as u64
}

/// Every PHP thread that has bound a context, weakly: the owning `Arc` lives in
/// the thread's own local, so a thread that goes away without unbinding is
/// pruned by the next scan rather than leaving the watchdog a dangling entry.
fn registry() -> &'static Mutex<Vec<Weak<Execution>>> {
    static REGISTRY: OnceLock<Mutex<Vec<Weak<Execution>>>> = OnceLock::new();

    REGISTRY.get_or_init(|| Mutex::new(Vec::new()))
}

thread_local! {
    static CURRENT: RefCell<Option<Arc<Execution>>> = const { RefCell::new(None) };
}

/// Publishes the calling PHP thread so the watchdog can reach its VM.
///
/// Called from the thread prologue, after TSRM has bound this thread's context
/// and therefore after the accessor below resolves to the right globals.
pub(crate) fn bind_current_thread(owner: ExecutionOwner) {
    // Deliberately the raw accessor rather than `ExecutorGlobals::get()`: that
    // one takes ext-php-rs's global lock on ZTS, which is the very lock this
    // SAPI exists to stay out of.
    let globals = unsafe { ext_php_rs::ffi::ext_php_rs_executor_globals() };

    let execution = Arc::new(Execution {
        owner,
        active: AtomicBool::new(false),
        globals: Mutex::new(Globals(globals)),
        deadline: AtomicU64::new(0),
        budget_secs: AtomicU64::new(0),
        ini_timeout_seconds: AtomicI64::new(0),
        raised: AtomicBool::new(false),
    });

    registry().lock().map_or_else(
        |_| warn!("php interrupt registry is poisoned; this thread cannot be interrupted"),
        |mut registry| {
            registry.retain(|entry| entry.strong_count() > 0);
            registry.push(Arc::downgrade(&execution));
        }
    );

    CURRENT.with_borrow_mut(|current| *current = Some(execution));
    start_watchdog();
}

/// Withdraws the calling thread. Called from the teardown guard, before the
/// thread's PHP context goes away and the captured pointer with it.
pub(crate) fn unbind_current_thread() {
    CURRENT.with_borrow_mut(|current| {
        if let Some(execution) = current.take() {
            execution.deadline.store(0, Ordering::Release);
            // Blocks until an interrupt already inside the globals has
            // finished writing. The caller releases the PHP context as soon as
            // this returns, so this is the only thing between the watchdog and
            // freed memory.
            execution.globals().0 = ptr::null_mut();
        }
    });
}

/// The host-level budget, published once when the runtime is built.
///
/// A process-global rather than a value threaded through every execution path,
/// because that is what it is: execution is owned by the host, so the bound
/// belongs to the host and not to a site, a pool or a request.
static DEFAULT_BUDGET: OnceLock<Option<Duration>> = OnceLock::new();

/// Publishes the host budget for this process.
///
/// Called at **registration**, not at `build()`. It used to be the latter, and
/// the storage is first-write-wins: a builder that was constructed and then
/// dropped — a test, an exploratory call, a configuration that failed
/// validation — froze the budget for everything that came after it. Building
/// once with `None` permanently stopped a later, real runtime from installing a
/// duration, and nothing said so. Registration is the point where a runtime has
/// been validated and is actually going to serve.
///
/// A second registration with a *different* budget is refused rather than
/// dropped. One process runs one embedded PHP runtime — `ensure_sapi()` is
/// first-caller-wins for the same reason — so a conflict is a configuration
/// mistake, and the one thing it must not do is leave the operator reading a
/// value that is not in force.
pub(crate) fn configure(budget: Option<Duration>) -> Result<(), String> {
    match DEFAULT_BUDGET.get() {
        None => {
            let _ = DEFAULT_BUDGET.set(budget);

            Ok(())
        }
        Some(installed) if *installed == budget => Ok(()),
        Some(installed) => Err(format!(
            "php execution_timeout is already {installed:?} for this process and cannot be \
             changed to {budget:?}: one process runs one embedded PHP runtime"
        )),
    }
}

/// The budget a caller with its own preference should actually apply: its own
/// when it has one, the host's otherwise.
pub(crate) fn resolve(budget: Option<Duration>) -> Option<Duration> {
    budget.or_else(host_budget)
}

fn host_budget() -> Option<Duration> {
    DEFAULT_BUDGET.get().copied().flatten()
}

/// A budget, armed for as long as this value is held.
///
/// Must be held *outside* any `try_catch` boundary. A Zend bailout is a
/// `longjmp`, which does not run destructors — a guard living inside the
/// catch would simply never disarm. Outside it, the catch returns normally and
/// the drop runs like any other.
pub(crate) struct Budget;

impl Budget {
    /// Arms the budget PHP itself was given, which is the knob a PHP developer
    /// already reaches for: `max_execution_time` in php.ini, and whatever
    /// `set_time_limit()` last wrote, both land in `EG(timeout_seconds)`.
    ///
    /// Used by the script model, where one execution is one request and php.ini
    /// can therefore describe it. Zero is PHP's "no limit", and falls back to
    /// the host budget — unbounded is not a default a supervised runtime should
    /// keep, and a fallback that is written back keeps the fatal error's
    /// wording honest.
    ///
    /// The application model does not use this: php.ini is process-wide and
    /// every application shares the process, so it cannot tell them apart.
    pub(crate) fn from_php() -> Option<Self> {
        let declared = CURRENT.with_borrow(|current| {
            let execution = current.as_ref()?;
            let globals = execution.globals();
            if globals.0.is_null() {
                return None;
            }

            let seconds = unsafe { (*globals.0).timeout_seconds };

            u64::try_from(seconds)
                .ok()
                .filter(|seconds| *seconds > 0)
                .map(Duration::from_secs)
        });

        declared.or_else(host_budget).map(Self::arm)
    }

    pub(crate) fn arm(budget: Duration) -> Self {
        arm(budget);

        Self
    }
}

impl Drop for Budget {
    fn drop(&mut self) {
        disarm();
    }
}

/// Starts timing the work about to run on this thread.
///
/// `EG(timeout_seconds)` is read here and left alone. PHP arms its own timer
/// from that global at request startup, which comes after this call, and a
/// budget written into it here would become a second, process-wide timer with
/// nothing to catch its signal. The budget reaches the global only when it is
/// raised, so the fatal error PHP prints names what actually applied, and
/// `disarm` puts the ini value back before the next request can read it.
///
/// Public because not every model has a Rust scope to hang a guard on: the
/// application model returns into PHP at `claviron_application_receive` and
/// only comes back at `claviron_application_respond`, so its request is armed
/// and disarmed by hand at those two points.
pub(crate) fn arm(budget: Duration) {
    CURRENT.with_borrow(|current| {
        let Some(execution) = current.as_ref() else {
            return;
        };

        let globals = execution.globals();
        if !globals.0.is_null() {
            unsafe {
                execution
                    .ini_timeout_seconds
                    .store((*globals.0).timeout_seconds, Ordering::Release);
            }
        }

        execution.raised.store(false, Ordering::Release);
        execution.budget_secs.store(budget.as_secs().max(1), Ordering::Release);
        execution.deadline.store(
            now_ms().saturating_add(budget.as_millis() as u64) + 1,
            Ordering::Release,
        );
    });
}

/// Stops timing and gives PHP back the `EG(timeout_seconds)` it had. Safe to
/// call when nothing was armed.
pub(crate) fn disarm() {
    CURRENT.with_borrow(|current| {
        if let Some(execution) = current.as_ref() {
            if execution.deadline.load(Ordering::Acquire) != 0 {
                let mut globals = execution.globals();
                if !globals.0.is_null() {
                    unsafe {
                        (*globals.0).timeout_seconds = execution
                            .ini_timeout_seconds
                            .load(Ordering::Acquire)
                            as _;
                    }
                }
            }
            execution.deadline.store(0, Ordering::Release);
            execution.budget_secs.store(0, Ordering::Release);
            execution.raised.store(false, Ordering::Release);
        }
    });
}

/// Marks the calling thread as executing PHP for as long as the guard lives.
///
/// This is what a drain looks at, and it is deliberately not the budget: a
/// service may run with no `execution_timeout` at all and still own work that
/// an expired drain may end. What it must never end is a thread sitting idle
/// between requests.
pub(crate) struct Active;

impl Active {
    pub(crate) fn enter() -> Self {
        activate();

        Self
    }
}

impl Drop for Active {
    fn drop(&mut self) {
        deactivate();
    }
}

/// Marks the calling thread as executing PHP.
///
/// The guard above is the right shape where one Rust scope is one execution —
/// a pool request, a task program. The application model has no such scope:
/// it returns into PHP at `claviron_application_receive` and comes back at
/// `respond`, so it activates and deactivates by hand at those two points, the
/// same boundary its budget uses.
pub(crate) fn activate() {
    CURRENT.with_borrow(|current| {
        if let Some(execution) = current.as_ref() {
            execution.active.store(true, Ordering::Release);
        }
    });
}

/// The end of one execution. Also clears `raised`, so an interrupt raised for
/// this one cannot suppress the next one's.
pub(crate) fn deactivate() {
    CURRENT.with_borrow(|current| {
        if let Some(execution) = current.as_ref() {
            execution.active.store(false, Ordering::Release);
            execution.raised.store(false, Ordering::Release);
        }
    });
}

/// Interrupts the executions `owner` has in flight, and nothing else.
///
/// This is the shutdown lever: a drain that has run out of patience can end the
/// executions it owns instead of walking away from them, which is the
/// difference between a service that stops and one that has to be killed.
/// Returns how many threads were signalled.
///
/// Scoped on two axes, and both matter. By owner, so one expired drain cannot
/// end another owner's request — the three execution models run concurrently
/// in one process and each owns its own drain. And by activity, so
/// an idle thread is left alone: signalling one sets flags that `arm` never
/// clears, and the next request to land on it would die of a timeout that was
/// never its own.
pub(crate) fn interrupt_owned_by(owner: ExecutionOwner) -> usize {
    let Ok(registry) = registry().lock() else {
        warn!("php interrupt registry is poisoned; nothing can be interrupted");

        return 0;
    };

    registry
        .iter()
        .filter_map(Weak::upgrade)
        .filter(|execution| {
            execution.owner == owner && execution.active.load(Ordering::Acquire)
        })
        .filter(|execution| execution.interrupt())
        .count()
}

/// One OS thread, started on first use and never stopped.
///
/// Deliberately not a Tokio task: its whole purpose is to act when something is
/// stuck, and a runtime whose threads are all parked on PHP is exactly the
/// situation it exists for.
fn start_watchdog() {
    static STARTED: OnceLock<()> = OnceLock::new();

    STARTED.get_or_init(|| {
        let spawned = thread::Builder::new().name("claviron-php-watchdog".to_owned()).spawn(|| {
            loop {
                thread::sleep(SCAN_INTERVAL);
                scan();
            }
        });

        if let Err(error) = spawned {
            warn!(%error, "php execution watchdog could not start; budgets will not be enforced");
        }
    });
}

fn scan() {
    let Ok(mut registry) = registry().lock() else {
        return;
    };

    registry.retain(|entry| entry.strong_count() > 0);

    let now = now_ms();
    for execution in registry.iter().filter_map(Weak::upgrade) {
        let deadline = execution.deadline.load(Ordering::Acquire);
        if deadline == 0 || now < deadline {
            continue;
        }

        if execution.interrupt() {
            warn!(
                overdue_ms = now.saturating_sub(deadline),
                "🚨 php execution exceeded its budget; asking Zend to end it"
            );
        }
    }
}
