# Tasks — Architecture

The background-work hub. Anything that runs **outside** the request hot
path — periodic GC, ACME renewal, cron-style sweeps, FCGI pool
housekeeping, sidecar daemons, external `php artisan` commands — flows
through this crate.

The crate's claim: **tasks are first-class citizens, not bolt-on
shell scripts**. They participate in the same lifecycle as everything
else (Provider/Reloadable/Runnable from `runtime`'s registry),
they're tracked by cancellation tokens so shutdown policy is explicit,
they share a uniform retry/backoff machine, and they speak
the same scheduling vocabulary regardless of whether the body is Rust
code or an external process. One mental model, one `tasks.yaml`, one
shutdown contract.

There's deliberate elegance in the surface: a single `#[task]`
attribute macro turns any `async fn` into a registered task with
zero boilerplate, while a `command:` block in `tasks.yaml` does the
same for arbitrary executables. Both end up as a [`TaskRunner`] that
the same supervisor drives.

---

## 1. Two task flavours, one supervisor

The crate accepts two task **bodies** and wraps both in the same
[`TaskRunner`] machinery:

### 1.1 Rust tasks — `async fn` + `#[task]` attribute

```rust
use claviron_runtime::AppState;
use claviron_tasks::{TaskResult, task};
//                              ^ proc-macro re-exported from this crate.
//                                Always import via `claviron_tasks::task` —
//                                in-workspace and out, sibling and binary
//                                consumers alike. Never depend on the
//                                proc-macro crate directly.
use tokio_util::sync::CancellationToken;

#[task(name = "cache_gc", scope = "workers")]
async fn cache_gc(state: AppState, _token: CancellationToken) -> TaskResult<()> {
    // periodic GC body — returns when the work is done.
    // tasks.yaml `timing` optionally chooses an automatic trigger.
    // Without it, this body remains available through TaskManager::run.
    Ok(())
}
```

The `#[task]` attribute emits an `inventory::submit!` of
[`TaskRegistration`] right next to the function. At service boot,
`TaskServiceBuilder::register_discovered_tasks` walks every
`TaskRegistration` in the binary and instantiates the matching
`TaskRunner`s — no central list, no manual wiring, dead-code-
elimination safe.

Manual registration also exists via `TaskService::register_fn` for
tasks that need to be conditional on runtime config (rare, but
supported):

```rust
service.register_fn("custom_task", |state, token| async move { /* … */ });
```

Runner names use `Arc<str>`, including command and engine names materialized
from configuration. Runner clones and per-attempt `TaskRuntime` metadata share
that allocation; rematerialization drops only the replaced registry owner.
The name is reclaimed after the last runner or metadata owner drops. Static
inventory names still enter through the existing registration API, but no
runtime path manufactures a static reference with `Box::leak`.
`TaskRunner::name()` borrows `&str`; `TaskRuntime` is `Clone`, not `Copy`, and
inspection-only process helpers borrow it. Controllers retain an explicit
clone when their lifetime needs its identity.

### 1.2 Builtin cron scheduler

`claviron_tasks::register` automatically installs the `scheduler` task. It
runs once in each eligible runtime, including master and standalone
(`scope: fleet`, `dispatch: each`, `concurrency: 1`). Those are internal
mechanics, not configurable scheduling choices. Its service-level config is:

```yaml
scheduler: { poll_interval: 1m }
```

The cadence is a positive duration; `1s` supports second-resolution cron.
Every poll drains due registrations from one local deadline heap and spawns
their bodies concurrently. There is no timer future per cron task. Several
missed occurrences coalesce into one admission. Each scheduled task selects
`timing.overlap: skip | allow` (default `skip`): skip rejects admission while its
previous scheduled body runs; allow starts the new occurrence concurrently.
Neither policy queues occurrences. Both cron and interval use this policy;
manual and daemon timing do not accept the field. Example:

```yaml
tasks:
  cleanup:
    timing:
      kind: cron
      expression: "0 * * * * *"
      overlap: skip
```

`concurrency` still controls placement/trigger slots, not overlap backlog.
Next occurrences
are computed strictly after both the poll time and the consumed deadline,
so a backward wall-clock step cannot replay that deadline. Interval tasks
retain their own timers and do not depend on this scheduler.

The scheduler participates in normal TaskManager enable/disable/restart and
reload. A changed `scheduler.poll_interval` restarts only its generation.
Stopping it pauses new cron admission without removing registrations or
cancelling bodies already admitted. Stopping/reloading a cron task removes
its own registration, cancels its bodies and drains them before replacing
the generation. `dispatch: one` still keeps its ownership/lease controller;
that controller registers with the same scheduler instead of holding a
separate cron timer. A runtime rejects a second active scheduler body.
`tasks.scheduler` is rejected: lifecycle management uses the builtin's
TaskManager identity, while YAML configures only the service-level cadence.

### 1.3 External process tasks — `tasks.yaml` `command:` block

```yaml
queue:
  timing: { kind: daemon }
  scope: workers
  dispatch: each
  command:
    program: php
    args: [ "artisan", "queue:work", "--sleep=1", "--tries=3" ]
    cwd: /sites/example/public
    env_file: sites/example/.env
    env:
      APP_NAME: queue
    exit_action: terminate
    reload_action: restart
    log: { stdout: /var/log/queue.log, stderr: /var/log/queue.err }
  concurrency: 4
  attempts: unlimited
  graceful_timeout: "20s"
```

`TaskService::register_commands` walks the loaded `TaskTable` and
synthesises a `TaskRunner` whose body is `process::run_command`. The
external child gets:

- A **clean** environment (`env_clear()`) plus `LANG` and a `PATH`, plus
  the operator's whitelisted `env_file` + `env:` block (file as base
  layer, inline YAML wins), plus reserved `CLAVIRON_TASK_*` runtime
  variables (see [`process::run_command`] doc).

  The `PATH` is the service-level `command.path` (§10), stamped onto every
  spec as `tasks.yaml` is loaded so the spawn path reads one field. A task's
  own `env: { PATH: ... }` still wins. Because the environment is cleared,
  this value is the *entire* answer to where a deployment's executables are,
  which is why it is operator config and not a literal in the spawn path:
  a tool installed under a user prefix — `deno`, `uv`, `pipx`,
  `cargo install` — is invisible to a child otherwise, and the failure
  arrives as a bare `No such file or directory` naming the task rather than
  the missing directory.

  `$VAR` and `${VAR}` in it are expanded against the **supervisor's** own
  environment at spawn, and a segment naming a variable that is not set is
  dropped whole rather than left half-resolved (`$HOME/.local/bin` must not
  degrade into `/.local/bin`). The default keeps `$HOME/.local/bin`
  unresolved deliberately: which home holds a deployment's tools is the
  operator's answer. A unit that runs as one account while pointing `HOME`
  at the one owning the toolchain is ordinary, and a home resolved at
  compile time would be wrong in exactly that case.
- Explicit process lifecycle policies for Claviron exit and task-service
  reload. `exit_action` defaults to `terminate`: send SIGTERM, observe the
  optional grace window, then fall back to SIGKILL. Reload does not touch an
  unchanged command unless `reload_action` explicitly requests `restart` or a
  named signal (see §4.3).
- The same retry / backoff loop the runner uses for Rust tasks —
  `attempts`, `backoff`, exponential growth with cap and jitter. The
  command's `restart` policy decides which process completions enter it.

A Rust handler and an external command share the same scheduling and
retry machinery. External commands additionally expose OS-process exit
policy because they can survive the task runner that started them.

---

## 2. Position in the layering

`TaskServiceConfig` keeps the task document strict while accepting its known
`metrics` section in every build. With `metrics` enabled it reads the typed
runtime policy; without the feature it consumes that section as `IgnoredAny`,
without validating policy or registering metrics publishers/RSS samplers.
Task definitions, management snapshots and pool policy are unchanged. The
`php` and `js` task declarations remain data-only fields independent of which
executors the embedding application registers.

```
   ┌────────────────────────────────────────────────────────────────┐
   │  this crate                                                    │
   │                                                                │
   │   #[task] proc-macro       re-exported as `claviron_tasks::task`. │
   │                            Lives in a sibling proc-macro crate │
   │                            (Rust requires proc-macros in their │
   │                            own crate) but consumers always go  │
   │                            through this one — never depend on  │
   │                            the proc-macro crate directly.      │
   │                                                                │
   │   registry.rs    TaskRegistration, TaskScope + TaskDispatch   │
   │                  inventory::collect! pickup point              │
   │                                                                │
   │   runner.rs      TaskRunner (the supervisor body):             │
   │                    - run_supervisor → spawns N workers         │
   │                    - run_with_interval / run_with_cron / daemon│
   │                    - run_with_retries → the attempts/backoff   │
   │                  TaskRuntime (per-attempt context)             │
   │   output.rs      executor-independent stdout/stderr policy     │
   │                                                                │
   │   pool/          TaskService-owned coordinated dispatch:      │
   │     controller.rs  slot acquire / renew / release             │
   │     reload.rs      task-scoped generations + reload           │
   │                                                                │
   │   process/       external-process task boundary:               │
   │     mod.rs       supervision + completion policy               │
   │     spawn.rs     command/env construction + spawn              │
   │     child.rs     live child wait/signal/termination            │
   │     instance.rs  persistent instance lock/reattach             │
   │     logger.rs    applies shared output policy to child Stdio   │
   │     spec.rs      command config + lifecycle policy types       │
   │                                                                │
   │   service.rs     TaskService — Provider+Reloadable+Runnable    │
   │                  owns tasks.yaml (Congif), owns the registry,  │
   │                  drives `serve()` lifecycle                    │
   │   manager.rs     TaskManager — named fleet commands over      │
   │                  the typed Orbit event bridge + snapshots     │
   │   snapshot.rs    transport-neutral status model and kind 226  │
   │                  per-member Orbit snapshot codec               │
   │                                                                │
   │   config.rs      TaskServiceConfig + TaskConfig                │
   │   error.rs       Error / Reason — task-specific result types   │
   │   lib.rs         public exports + TaskService registration     │
   └────────────────────────────────────────────────────────────────┘
                                   │
                                   │  Provider / Reloadable / Runnable
                                   ▼
                       claviron-runtime::registry
                       (boot order, reload, shutdown)
```

The single upstream surface is **`claviron-runtime::registry`** —
the lifecycle traits (`Provider`, `Reloadable`, `Runnable`) and
`AppState`. `TaskService` implements all three (see §6); it's
registered in the registry exactly like any other long-lived
service.

Consumers import everything from `claviron_tasks::*` — the macro,
the result types, the registry types. The proc-macro crate is an
implementation detail of how Rust ships proc-macros, not a public
boundary.

---

## 3. Timing is trigger policy, not body lifetime

For `dispatch: each`, `TaskRunner::run_supervisor` spawns worker coroutines for
configured automatic triggers. The service-owned controller drives both
fleet-arbitrated dispatches: `one` owns one automatic trigger and `pool` owns
daemon capacity. Omitting `timing` keeps the task registered for explicit
execution without starting a scheduler:

| `tasks.yaml::timing` | Loop body | Semantics |
|---|---|---|
| omitted / `{ kind: manual, execution_timeout: 30s }` | none | No automatic trigger. `TaskManager::run` may execute one bounded occurrence. |
| `{ kind: cron, enabled: true, expression: "...", execution_timeout: 30s }` | `run_with_cron` | Sleep until next cron occurrence (UTC), then run within the configured budget; repeat. |
| `{ kind: interval, enabled: true, every: 30s, execution_timeout: 30s }` | `run_with_interval` | Tick every interval; first tick after one interval unless `skip_on_boot: false`. |
| `{ kind: daemon, enabled: true }` | `run_with_retries` only | Trigger once when enabled. The body may be long- or short-lived; its `restart` policy decides whether natural completion enters attempts/backoff. |

`timing` defaults to `manual`. The enum keeps automatic trigger policies
mutually exclusive, but it deliberately says nothing about how long the body
lives. `manual`, `interval`, and `cron` each own an `execution_timeout`
(default 30s) that bounds one complete logical run, including retries and
backoff. `daemon` deliberately has no execution timeout: enable, disable,
reload, and runtime shutdown own its lifecycle. `enabled` belongs only to
`daemon`, `interval`, and `cron`; manual timing has no automatic trigger state.

When a bounded Rust future reaches its deadline, its occurrence token is
cancelled and the future is dropped. A bounded external command is SIGKILLed
and reaped even if its normal `exit_action` is `keep-running`;
`graceful_timeout` remains the separate budget for lifecycle-driven graceful
stops.

### Target capability — externally triggered in-runtime execution

A deliberate capability of the task system is to let an authenticated external
scheduler trigger application-owned work without moving that work outside
Claviron. For example, system cron can call `claviron-client run-task cache_gc`;
gRPC is only the control-plane transport. The registered task body still runs
inside the Claviron runtime that receives the request through its normal
`TaskRunner`, with the real `AppState` and the usual cancellation,
retry/backoff, scope, metrics, and shutdown contracts. An optional request
timeout overrides the configured timing timeout for that occurrence only.

This is not remote shell execution, and it does not make cron the task runtime.
The external caller chooses when to request one occurrence; Claviron remains
the execution and lifecycle authority. This also gives hard-coded Rust tasks a
narrow authenticated management entry point without exposing arbitrary
functions or serializing application state across the transport boundary.

`dispatch: one` changes interval scheduling from a process-local Tokio
epoch to UTC-aligned wall-clock buckets. Workers that boot at different
times therefore derive the same occurrence id. Cron already has a shared
UTC occurrence timestamp.

`dispatch: one` and `dispatch: pool` are deliberately outside
`TaskRunner::run_supervisor`. Every eligible runtime's `TaskService` drives a
lightweight task-scoped controller. The controller owns that task's fleet slot
state; a runner only executes a trigger after the controller gives it
ownership. This makes the same behavior available to every executor kind while
reload leaves unrelated coordinated tasks alone.

### 3.1 Concurrency

`concurrency: N` (default 1) is interpreted by dispatch:

- `each` — `N` local worker coroutines in every eligible runtime.
- `one` — must be `1`; one runtime owns the automatic trigger fleet-wide.
- `pool` — `N` daemon slots total across the eligible scope.

Each active slot has its own `worker_number` and retry counter. Use cases:

- `concurrency: 4` on a `command:` queue worker → 4 PHP queue
  consumers in parallel, each getting its own child process.
- `concurrency: 1` on a cron cleanup task → one worker, periodic
  schedule. Default and most common.

### 3.2 Overlap suppression

Cron / interval modes use a shared `Semaphore(1)` (`interval_limit`) per
runner: a tick that fires while the previous attempt is still in
flight is **dropped with a warn log**, never queued. Prevents
pile-up if a task drifts slower than its schedule.

The permit is shared by all worker loops cloned from the runner.
Scheduled concurrency greater than one therefore does not create N
simultaneous copies of a tick: the first local loop takes the permit and
the others drop that tick. Scheduled tasks should normally use
`concurrency: 1`.

### 3.3 Retries and backoff

`run_with_retries` is the inner attempt loop:

```
attempt 1 → run → ok? → return                      // first attempt fast
                   ↓
                 err
                   ↓
        sleep(backoff) → attempt 2 → run → ok? → return
                                    ↓
                                  err
                                    ↓
            sleep(backoff·2 ±20%) → attempt 3 → … → cap at 60s
```

- **Initial delay** = `backoff` (default 1s).
- **Growth** = `backoff · 2^(attempt-1)` (exponential), with ±20%
  jitter to avoid synchronised retry storms across multiple instances.
- **Cap** = `MAX_BACKOFF_SECS` (60s).
- **Reset** = if the task ran for ≥ `BACKOFF_RESET_SECS` (300s)
  before failing, the next failure resets `attempt` to 1 — a
  long-lived task that finally crashes shouldn't get the harshest
  exponential delay.
- **Give up** = `attempt >= attempts` (default 1) → log and end the
  current runner. `attempts` counts total attempts, so the default `1`
  performs no retry. Unlimited retry is deliberately explicit as
  `attempts: unlimited`; zero is invalid and has no sentinel meaning.

What happens after the runner ends depends on dispatch:

- `each` daemon — that local worker remains stopped until task-service reload
  or process restart.
- `each` / `one` scheduled action — that occurrence is finished; the next
  interval or cron occurrence gets a fresh attempt counter.
- `one` / `pool` daemon — the limit applies to the current **slot ownership session**.
  The owner retains and renews the slot throughout its attempts and backoff.
  When the runner succeeds, reaches a terminal exit, panics, or exhausts its
  attempts, the lock is released and the missing slot returns to the pool.
  Any eligible manager, including the previous owner, may acquire it;
  the new ownership session starts again at attempt 1.

This distinction is intentional: transient body failures retry locally
without moving capacity between workers, while exhausted local recovery makes
the anonymous slot eligible for redistribution. A worker/process failure does
not wait for `attempts`; peers reclaim the slot after lease expiry.

---

## 4. Cancellation — the token contract

This section applies
[GRACEFUL_OWNERSHIP: NO_IMPLICIT_TERMINATION](../../docs/design/GRACEFUL_OWNERSHIP.md)
to task cancellation, drain and external process actions.

Every running task in claviron is tracked through a
`tokio_util::sync::CancellationToken`. This is how shutdown stays
genuinely graceful instead of degenerating into "kill the process,
hope nothing was mid-write".

### 4.1 Per-runner token tree

`TaskRunner` owns one parent token under a `Mutex` plus a small stop-cause
value (`shutdown` or `reload`). `run_with_retries` creates a **child** token
per attempt:

```
runner.cancel  ── parent ──┐
                           ├── attempt 1 child (cancelled when parent is)
                           ├── attempt 2 child
                           └── …
```

Cancelling the parent (`runner.cancel()`) cascades to every active
child, so a single signal stops all `concurrency` workers plus any
in-flight retry. Children also drop on natural completion, freeing
the link without affecting siblings. Rust task callbacks continue to see only
the cancellation token. External command runners also read the internal cause
so they can select the correct process action.

### 4.2 Shutdown ladder

`TaskService::serve` waits on `state.shutdown_token().cancelled()`.
On shutdown:

1. `cancel_all()` flips every runner's parent token. Every worker
   observes `select!` on `token.cancelled()` and drops out of its
   loop.
2. `wait_all_stopped(timeout)` polls each runner's `running` counter
   (incremented on attempt entry, decremented on drop via a
   `RunningGuard` that fires even on panic). Returns when every
   runner reads zero, or the **service-wide** `graceful_timeout` elapses
   — in which case a forced exit is logged and `serve` returns anyway.
   Omitted or explicit `null` means wait indefinitely; runtime code preserves
   that `None` and never supplies a fallback.

The "RunningGuard fires on panic" detail is load-bearing: a panicking
task body still decrements the counter via `Drop`, so a runaway
panic loop can't deadlock shutdown. Tasks may misbehave, the
supervisor must not.

### 4.3 Graceful timeout — task body wins or runner forces

Per-task `graceful_timeout` (in `tasks.yaml`) bounds how long a single
attempt can ignore cancellation before the runner reports
`Error::Exit(Reason::GracefulTimeout)`. Three branches in
`run_with_retries`:

1. `graceful_timeout: None` → the runner waits **indefinitely** for
   the task body to finish naturally after cancel (assumes the body
   handles the token correctly). Use for tasks that *must* finish
   their current unit of work.
2. `graceful_timeout: Some(d)` + Rust task → `tokio::time::timeout(d,
   future)` — task gets `d` to wind down, then we mark
   `GracefulTimeout` (no force-stop because Rust futures can't be
   killed; the dropped future just stops being polled).
3. `graceful_timeout: Some(d)` + external `command:` using a selected
   `terminate` action → the ladder in `process::run_command`:
   SIGTERM, wait `d`, then SIGKILL if still running.

For a command, normal shutdown selects `exit_action`. It defaults to
`terminate`; `{ signal: kill }` skips the grace period but still reaps before
the runner stops. `exit_action: keep-running` sends no signal and relinquishes
supervision.

`reload_action` is optional and has a separate vocabulary. Omission leaves an
unchanged command and its supervision untouched. `restart` performs the
managed termination ladder before the candidate plan starts. `{ signal: ... }`
with `hup`, `int`, `quit`, `term`, `usr1`, or `usr2` sends that signal to the
current daemon **without cancelling its runner or releasing pool ownership**.
The same child remains supervised. If it exits in response, its completion
follows the normal `restart`/`attempts` policy. An in-place signal is not
assumed to be non-terminating at the OS level. `{ signal: kill }` instead
kills and reaps before rebuilding the selected generation.
Structural plan changes are independent: TaskService stops and rebuilds a
changed task even when its normal reload action is omitted or an in-place signal.

Recoverable daemon processes use a stable logical instance record. `each`
instances are keyed by application, runtime lane, task name, and local
concurrency slot; `pool` instances are keyed by application, task name, and
fleet-wide pool slot, deliberately excluding the current worker lane. A kernel
`flock` descriptor is inherited by the foreground command and therefore lives
exactly as long as that command retains it. A later Claviron incarnation sees
the busy lock, validates PID plus the kernel process-start identity and command
fingerprint, and attaches rather than spawning a duplicate. If that attached
daemon exits while Claviron is running, the same supervision session fills the
vacant instance immediately.

Recoverable processes require daemon timing. `each` instances are runtime-local;
`one` and `pool` instances use stable fleet slot identities. The next worker to
acquire an expired coordinated slot lease attaches to the surviving command
from an abruptly lost owner and assumes its supervision; normal shutdown still
applies the configured terminating action and reaps it. Exit actions that
intentionally relinquish coordinated supervision remain rejected, because a
live command cannot be separated from its slot ownership. `one` has exactly one
such logical slot.

### 4.4 Persistent command ownership and inventory (C8)

Recoverable external daemon commands publish a versioned `.presence` record
beside their existing `.instance` file. `ProcessOwner` identifies a master,
worker or standalone controller by exact `ProcessIdentity`; a worker number
alone is never a controller identity. Task name, dispatch and slot identify the
command independently. `spawned_by` is immutable for one command incarnation;
`last_controller` changes on attachment. This intentionally does not change the
child's environment or kernel PPID. An inherited command's environment still
describes its original launcher.

A separate close-on-exec `.controller` flock is held for the complete command
supervision session, including attached-process replacement. The inherited
child lock prevents duplicate processes; the controller lock also prevents two
Rust runtimes from attaching and sending lifecycle actions concurrently. A
bounded record-protocol rendezvous tolerates short inventory lock probes; it
does not create a shutdown or termination deadline. Explicit controller drop
unlocks the tenure, including descriptors briefly duplicated by concurrent
forks. Process death releases it through the OS. Pool leases and dispatch
placement policy remain unchanged; this does not redistribute `each` instances
or adopt excess pool slots.

`TaskManager::processes()` inspects the complete same-application namespace on
the blocking executor. It does not filter by current tasks.yaml, concurrency,
worker count, or membership: an old worker #4's task is still visible with a
new two-worker topology. The scan never starts, adopts, terminates or deletes
anything. This read model is separate from task placement `in_flight`, which
continues to count running task bodies, not every surviving OS process.

The snapshot exposes process identity, logical task/slot, birth owner, current
controller when verified, last recorded controller/action, and independent
kernel liveness. A free controller lock reports `Detached`, even if the old
controller process is still alive. Missing or inconsistent metadata reports
`Unknown`; an unverified PID/start stamp is not an exit acknowledgement. The
metadata is a lifecycle checkpoint, not a PHP heartbeat or a claim of application
health. Publications are atomic and contain no command arguments/environment.
Publication failure logs an error but must not drop a live command's owner.

Legacy v1 instance files remain readable and attachable. Before an updated
controller publishes metadata they appear by instance key and exact PID, with
unknown task/owner fields. Attaching preserves an unknown birth owner rather
than inventing one. Mixed old/new controllers are not a safe handover protocol:
old binaries do not acquire the new controller lock. Stop the old controller
before handing its commands to an updated runtime. No SHM clearing is needed.
Malformed records and permission failures are explicit scan issues rather than
silently missing processes. Exited/unverified historical records remain visible;
the reader does not prune them or decide termination policy.

The task list remains the sole list surface; no separate process-list CLI or
RPC is added. Presentation of this detail in a task specification is deferred.
Worker adoption, descendant traversal, per-process remote stop, GUI presentation
and pool reconciliation are separate consumers/policies; this inventory does
not grant them implicit termination authority.

External processes are the only place we can actually **kill**
something. Rust tasks have to cooperate.

---

## 5. Where and how tasks run — `TaskScope` × `TaskDispatch`

claviron has three process roles (`RunMode`): `Master` (forks the desired
worker count and supervises), `Worker(id)` (handles traffic), `Standalone`
(everything in one process, no fork).

The desired worker count is read from `AppState`, not encoded into `RunMode`.
Orbit fleet capacity is separate immutable SHM geometry and is never used as
the number of eligible task runtimes.

Placement and cardinality are deliberately separate:

- `TaskScope` re-exports `claviron_orbit::placement::Scope`; `TaskDispatch`
  re-exports its `Dispatch`. Task owns timing and execution, not duplicate
  placement enums. Existing Rust names `Workers` and `Fleet` and YAML spellings
  `workers`, `fleet`, and `both` remain compatibility aliases. Canonical scope
  variants are `Master`, `Worker` (default), and `Any`.
- `TaskScope` (`master` / `worker` / `any`) says
  which runtime roles are eligible.
- `TaskDispatch` (`each` / `one` / `pool`, default `each`) says whether
  concurrency is local, a single scheduled winner, or a shared daemon pool.

Inventory registration is identical in every process. The scope filter
is applied once, when supervisors start after `tasks.yaml` has loaded.
YAML values override annotation defaults, so placement can change on
reload without recompilation.

Omitting scope and dispatch selects `worker` and `each` for Rust runners,
macro registrations, commands and hosted-language tasks. Master and all-role
execution must be explicit; standalone admits worker-scoped execution as the
combined process role. The builtin scheduler retains its explicit fleet scope.

Decision matrix:

| Task `scope:` | `Master` eligible? | `Worker` eligible? | `Standalone` eligible? |
|---|:-:|:-:|:-:|
| `master`     | ✅ | ❌ | ✅ (performs both roles) |
| `workers` (default) | ❌ | ✅ | ✅ (acts as the sole worker) |
| `fleet` | ✅ | ✅ | ✅ |

Dispatch then applies inside that candidate set:

| Task `dispatch:` | Behavior |
|---|---|
| `each` (default) | Every eligible runtime executes with local `concurrency`. |
| `one` | Eligible runtimes contend for one stable slot; its owner runs the schedule or daemon body while peers remain ready to take over. |
| `pool` | Worker- or fleet-scope daemon only: eligible runtimes share `concurrency` stable slots and run only the slots they lease. |

`one` is stable single ownership of an automatic trigger and requires
`concurrency: 1`. For cron/interval, the owner arms the schedule and executes
each occurrence. For daemon timing, the owner runs the single live body.
Other eligible runtimes keep only a lightweight controller and can acquire the
slot after orderly release or lease expiry. All coordinated tasks share the
fleet lock table; no task-specific SHM path exists.

`pool` is valid with `scope: workers` or `scope: fleet` and no
cron/interval. `scope: master` is rejected because a single eligible
runtime has nothing to distribute; use `dispatch: each` for local master
concurrency. Pool is continuous-capacity placement, not an occurrence seal.
Slot keys are `task:<name>:pool:<slot>`. `TaskService` keeps one controller
per pool task in each eligible runtime, renews owned locks every third of
`pool.lease_ttl`, and stops a local task before releasing its lock. A killed owner cannot
run `Drop`, so peers wait for lease expiry before reclaiming the slot.
Standalone uses the same controller locally without joining an Orbit fleet.
This is one controller per `runtime × task`, never one waiting acquisition
loop per concurrency slot.

Retry and ownership are nested, not competing supervisors. A locked slot
runs `TaskRunner`'s normal `attempts`/`backoff` loop in its owning runtime;
the ownership lock remains live during backoff, so peers continue yielding.
Only when that runner ends is the lock dropped and the slot exposed to a new
acquisition. Re-acquisition does not promise migration: the former owner participates
anonymously and may win again. For an external `command:` task, a local retry
spawns a new child process in the same owning runtime; the pool itself never
forks processes.

Each eligible manager limits one reconciliation's successful acquisitions to
approximately `ceil(concurrency / eligible_runtime_count)`. Worker scope uses
the process owner's current worker membership; fleet scope includes master.
The controller resolves `FleetMembership` on every acquisition pass, so a
retained worker observes growth/removal without restarting. Missing/stale
membership defers new acquisitions; existing leases still renew and drain
normally. Reporting membership is not a health or readiness claim.
This matters because an early runtime could otherwise take every slot before
peers join. Placement remains anonymous and self-healing; the budget only gives
peers time to participate and does not pin a slot to a named worker.

Task inventory also reads current membership, collects only those lanes and
reports their count as `members_expected`. A removed worker's still-fresh
task batch cannot remain a placement. Local publications represent one member;
they do not assert fleet size. Task execution `CLAVIRON_WORKER_COUNT` remains
bootstrap-context metadata, not an input to slot arbitration or live inventory.

On Linux, external `command:` children using `exit_action: terminate` install
`PR_SET_PDEATHSIG(SIGKILL)` before exec. A hard-killed/OOM owner therefore
cannot leave a managed orphan consumer alive when its lease expires and a peer
replaces it. On platforms without a parent-death signal, including macOS and
FreeBSD, the command may survive its owner, but its inherited instance lock
remains authoritative: after lease expiry the next pool owner attaches to that
exact process instead of spawning duplicate capacity. A non-terminating
`exit_action` deliberately omits the Linux parent-death signal and uses the
same lock for later reattachment where that lifecycle is supported.

Real-world examples (from the workspace):

- **`acme_gc` (`scope: master`, `dispatch: each`)** — ACME challenge file cleanup. Lives
  next to ACME orchestration which runs in master. Workers don't
  know about ACME state.
- **`cache_gc` (`scope: workers`, `dispatch: each`)** — files cache GC. Each worker has
  its own RAM cache, so each worker runs its own GC; master has no
  cache to clean.
- **`fcgi_housekeep` (`scope: workers`, `dispatch: each`)** — FPM pool staleness sweep.
  Per-worker because the pool is per-worker.
- **`sync_domains` (`scope: workers`, `dispatch: one`)** — every worker
  can perform the DB/provider synchronization, but one lease owner arms and
  executes the cron schedule; a peer takes over after ownership ends.
- **Laravel queue (`scope: workers`, `dispatch: pool`, `concurrency: 3`)** —
  three desired daemon slots are shared by worker runtimes while master stays
  reserved for supervision;
  ownership may move after shutdown or lease expiry.

The intent: scope says who *can* execute; dispatch says whether automatic
execution is local to every candidate, has one fleet owner, or uses a shared
daemon pool.

---

## 6. TaskService — the lifecycle host

`TaskService` is the singleton this crate registers in
`runtime`'s registry. It does three things at startup
(`boot`): loads `tasks.yaml` from `state.config_dir()` (with
`${ENV_KEY}` expansion through the frozen env snapshot — same path
as `vhost.yaml` loading, see `edge` ARCHITECTURE.md for
the env scrub story), atomically swaps the parsed `TaskTable` into
its inner `ArcSwap`, and walks `command:`-style tasks turning them
into `TaskRunner`s via `register_commands`. Rust tasks are already
in the registry from the inventory walk done by crate-root `register`
during sync registration. On reload (SIGHUP — priority 10, above
default 100 and below EARLY 50, so tasks settle late but before
anything that resolves them) it re-reads the YAML, classifies its diff,
drains only the selected task generations, then starts their candidate
replacements; multiple SIGHUPs while one reload is in
flight collapse into one through a `tokio::sync::Mutex`. At runtime
its `run()` drives `serve()`: spawn supervisors per task, wait on the current
service generation's `RunContext`, drain, return. Process shutdown is the
parent of that same cancellation scope, so targeted service stop/restart and
whole-process shutdown use one teardown path. The runtime
keeps the future in a `JoinSet` alongside everything else
participating in the global lifecycle. Rust `#[task]` bodies and YAML command
runners remain in separate catalogs so changing a task between those body
types cannot leave a stale runner behind. Both are resolved into the same
active-supervisor state and use the same scheduling machinery.

For non-pool work, `TaskService` retains one active supervisor record per task
name. The record owns both the exact `TaskRunner` generation and its Tokio join
handle. This is runtime state, not desired config: it lets reload prove that an
old scheduler has exited before resetting its token or installing a candidate
runner, and prevents duplicate supervisor generations.

Reload parsing produces a private, validated candidate first. The candidate is
compared with the active table and each task is classified as unchanged,
start, stop, rebuild, reschedule, or reconfigure. This classification is the
boundary for the incremental reload model. The candidate is not published
until every selected supervisor and in-flight
scheduled attempt has drained within one service-wide deadline. A timeout
leaves the previous config published and the affected runtime records in
`stopping`; it never starts a duplicate candidate generation.

For `dispatch: pool`, the spawned unit is a service-owned, task-scoped pool
controller instead of `TaskRunner::run_supervisor`. `PoolReloader` retains
those controller generations by task name. Reload and shutdown cancel a
selected controller through the runner token; it drains owned children before
slot locks are released and continues renewing those leases throughout the
drain. The body still passes through the runner's normal
retry, cancellation, metrics, and external-process machinery. A task-plan or
explicit restarting reload action replaces only that pool task. Unchanged
pool tasks keep their children and leases; a pool-wide lease-policy change
selects every pool controller.

Unchanged command tasks with in-place signals are separate notification
targets, not drain targets. Only after a candidate is accepted does TaskService
notify the exact active runner (not a newly materialized command-catalog
runner). Each live child subscribes to its runner's `watch` channel; pending
reload notifications coalesce to the latest selected signal, reach every
current slot, and are not replayed to future children during backoff or after
replacement. The child-owning wait loop sends the signal and continues waiting,
retaining its PID metrics registration. Notification errors are logged without
dropping supervision. A mixed reload that times out does not send signal
notifications for the rejected candidate.

`TaskManager` is an owned domain handle resolved from `AppState`, not a
provider or runnable. Its `enable`, `disable`, `reload`, and daemon-only
`restart` methods publish
one typed command naming the logical task. Every current fleet member that
observes the event applies its own scope eligibility; the manager never chooses
a worker.

Enable starts the configured automatic trigger. For interval and cron
tasks, `Active` with `in_flight == 0` is the normal pending state. A manual task
has no trigger to enable or disable and remains `Inactive`, ready for explicit
runs. Disable stops the automatic trigger and drains its supervised bodies.
Restart preserves the enabled trigger policy while fully draining and replacing
an enabled daemon generation. For `dispatch: each`, all local concurrency slots
drain before their replacements start; this is not a rolling restart. A pool
controller keeps renewing each owned slot lease until its body has stopped, so
the replacement generation cannot overlap the old owner. Each placement exposes
a process-local monotonic generation so clients can prove that replacement
occurred instead of mistaking a pre-command `Active` snapshot for completion.
`run` is deliberately local:
it executes exactly one occurrence in the runtime receiving the call, without
publishing an Orbit event or changing trigger state. Scope remains a hard
runtime capability boundary, while configured `dispatch` and `concurrency`
describe automatic supervision and do not multiply or arbitrate the explicit
occurrence. Independent explicit requests may overlap; callers or task bodies
that require serialization own that policy. A disabled interval or cron may
still be run explicitly; disabling its scheduler does not revoke the body as a
local capability or cancel an explicit run already in flight. Daemon timing
rejects `run` and must be managed through enable. `run` waits for the actual
`TaskResult` and uses the timing variant's `execution_timeout`, unless the
caller supplies a one-request override. TaskService owns the spawned execution;
the caller owns only a request-scoped one-shot waiter. Registration and spawn
happen under the active-run lock, in that order, before the lock is released;
the result is awaited only after release. A body that completes immediately
therefore cannot outrun its waiter, while a disconnected caller merely drops
that waiter and does not implicitly cancel application work.

An operator disable is stronger than application shutdown policy for the
automatic trigger: its supervised command bodies are stopped and reaped even
when their configured `exit_action` is `keep-running`. That option controls
Claviron process exit only; it cannot make an explicit management disable
report success while leaving the supervised child detached.

This first command surface reports acceptance, not fleet convergence. A
receiving member mutates the `enabled` field of its live `TaskTiming`; there is
no parallel trigger-state overlay. A member that boots after the event, or any
member that reloads configuration, reads the desired state from `tasks.yaml`
again. Durable command state, command IDs, and per-member acknowledgements
require a separate desired-state store and are not claimed by the event-only
API. Orbit event lag can therefore leave a member on the previous state until a
later command or config reload reaches it.

`TaskManager::list()` and `TaskManager::status(name)` are observations, not
command outcomes. `list` returns a transport-neutral `TaskListSnapshot`;
`status` narrows the same current observation to a `TaskStatusSnapshot` without
creating another state source. gRPC and CLI layers map those domain values into
their own response formats. The local `TaskTable` supplies only the compact
policy identity (name, body kind, scope, dispatch, timing, and concurrency).
Runtime placement comes from every member's `TaskService`.

Each supervised member periodically publishes its complete local view as one
atomic per-node batch in Orbit kind `226`. A reader accepts only a complete,
fresh batch and joins its rows by task name. `members_expected` and
`members_reporting` make missing or stale members explicit. Standalone mode
reads `TaskService` directly and does not open the ring. The publisher remains
part of `TaskService`; `TaskManager` itself is still neither a provider nor a
runnable.
Kind `226` is registered through managed SHM lifecycle as `Reuse` / `Preserve`;
complete-batch and freshness checks replace the former master boot reset.

Collection scans every physically reserved Orbit lane because a live member
may occupy any admitted node id. `members_expected` remains the supervisor's
current desired process-family size; empty reserve capacity is not reported as
a missing member.

Lifecycle and execution outcome stay orthogonal. An interval task waiting for
its next tick is `Active` with `in_flight == 0`; it may simultaneously retain a
failed `last_execution`. Disabled, ineligible, missing-runner, inactive,
stopping, and finished are distinct lifecycle states. Replacement
runner generations share a small always-on runtime record, so `in_flight` and
the last terminal outcome do not depend on the optional metrics feature.
Snapshots deliberately omit command arguments/environment, retry details, raw
errors, logs, join handles, and cancellation tokens.

TaskService stays alive until global shutdown even when the initial task table
is empty. This is required so later management commands, snapshot publication,
and shutdown cleanup retain one lifecycle owner.

---

## 7. Provider vs Task — two different concepts

The most common confusion when adding background work to claviron
is asking "should this be a Provider or a Task?". They look similar
from the outside (both end up running async code), but they sit on
different axes:

> **`Provider` is `runtime`'s DI / registry primitive.
> `Task` is a unit of supervised background work. A `Provider` is
> *not* a `Task`, and a `Task` is *not* a `Provider`.**

A `Provider<AppState>` is **a typed singleton in the runtime
registry**. It has a `name()`, a `boot()` hook that runs once at
startup with priority ordering, and an opt-in trio of capabilities
the runtime checks via `as_reloadable` / `as_runnable`. Other code
asks the registry for a `Provider` by type
(`registry.resolve::<MyService>()`) — that's the DI part. Whether
or not a `Provider` runs anything in the background is independent
of being a `Provider`. Most providers (HttpConfigProvider,
VhostResolver, AcmeProvider) just hold an `ArcSwap` snapshot and
serve reads; they never become `Runnable`.

A `Task` (in this crate's vocabulary) is the opposite: it has a
schedule (cron/interval/daemon), it has retry/backoff/concurrency,
it has `TaskScope` placement and `TaskDispatch` cardinality, it
participates in `TaskService`'s draining shutdown. It does **not**
live in the registry — `TaskService` does. Tasks are addressed by
string name (`"acme_renew"`) inside `TaskService`'s map, not by
Rust type.

A `Provider` *can* additionally implement `Runnable<AppState>` to
say "the runtime should also drive my long-running future". That
makes it a Provider that **also** runs — but it's still a
Provider, not a Task. `claviron-tls::CertificateListener` and
`claviron-edge::listener::VhostAliasListener` are exactly this
shape:

```rust
impl Provider<AppState>   for CertificateListener { /* registry presence */ }
impl Reloadable<AppState> for CertificateListener { /* signalled reconnect */ }
#[async_trait]
impl Runnable<AppState>   for CertificateListener { /* the LISTEN loop */ }
```

The runtime spawns the `Runnable::run()` future when it walks the
registry; both providers and `TaskService` (which is itself a
provider with `Runnable`) ride the same global shutdown token. So
"the cert listener runs" and "the renewal task runs" come out of
the same shutdown ladder — but they got there through different
mental models.

### 7.1 When to use a Task

Choose a `#[task]`-annotated `async fn` (or a `command:` block
in `tasks.yaml` for external processes) when **all** of these
hold:

- The work has a **schedule** (cron / interval) **or** is restart-
  tolerant in a daemon loop.
- You want **retry / backoff with attempt counting** out of the
  box.
- You want `TaskScope` filtering (master / workers / fleet) and optional
  single-owner or pooled fleet arbitration for free.
- The work is **stand-alone**: it doesn't need other code to
  resolve it from the registry. State it touches comes via
  `AppState` and registry lookups inside the body.
- It's fine if there's **no per-instance handle** to it — once
  registered, the only way to interact with the task is by name
  through `TaskService` (cancel-all-on-shutdown, restart-on-reload).

Examples: `acme_gc` (file cleanup, hourly interval, master),
`cache_gc` (per-worker GC, hourly), `sync_domains` (cron, one worker per tick).
A Task is the right shape when the work is a *verb* — "do this
periodically".

### 7.2 When to use a Provider (with `Runnable`)

Choose `impl Provider + Runnable` when **any** of these holds:

- Other code needs to **resolve the thing by type** from the
  registry. Example: a control-plane API that reads the
  listener's reconnect generation. A Task can't be resolved this
  way — it's a string-keyed entry in `TaskService`.
- The component owns **persistent per-instance state** that
  outlives a single attempt: an `ArcSwap` snapshot, a
  `tokio::sync::watch` channel, a connection pool, a buffered
  ring of recent events.
- The work has a **custom lifecycle** that doesn't fit the
  attempts/backoff loop: a `LISTEN` reactor that wants to
  reconnect on signal but otherwise runs forever; an mDNS
  responder that announces on boot, refreshes on a non-cron
  cadence, and unregisters on shutdown; a websocket fan-out hub
  that holds many client conns.
- You want **per-instance reload semantics** — a `Reloadable`
  impl that does something specific (e.g. "drop the active
  PgListener and reconnect against the refreshed env URL")
  rather than the generic re-read-and-respawn cycle Tasks
  inherit from `TaskService`.

Examples: `CertificateListener` (NOTIFY/LISTEN reactor with
reconnect signal), `VhostAliasListener` (same shape for vhost
aliases), `H3Server` (binds UDP, accepts QUIC connections —
clearly a service). A Provider is the right shape when the work
is a *noun* — "this thing exists and runs".

### 7.3 Decision shortcut

Ask three questions:

1. **"Does anyone need to resolve me from the registry?"**
   Yes → Provider. No → keep reading.
2. **"Do I have meaningful per-instance state others might
   inspect or signal?"**
   Yes → Provider (with Runnable if it runs continuously).
   No → keep reading.
3. **"Does my work fit the schedule + retry + mode-filter
   model?"**
   Yes → Task. No → reconsider Provider; if neither fits, talk
   to someone before inventing a third concept.

The trap to avoid: writing a `#[task]` for something that's
really a stateful service. The Task gets re-spawned on reload
losing its state; you can't resolve it; scope filtering doesn't
help because there's only one of you. If you find yourself
fighting `TaskService`, you wanted a Provider.

The opposite trap (writing a Provider for a periodic GC sweep)
is rarer because it's obviously over-engineered for the use case
— but it happens when someone wants "a Reloadable hook" and
hasn't realised that reload-on-signal is something `TaskService`
already does for them per-task.

---

## 8. Public surface

Re-exports from `lib.rs`:

```rust
pub use claviron_macros::task;                // the proc-macro
pub use error::{Error, Reason, TaskResult};  // task result types
pub use manager::{TaskManager, TaskManagerError, TaskManagerResult, TaskRunId, TaskRunOutcome};
pub use registry::{TaskDispatch, TaskRegistration, TaskScope};
pub use runner::TaskRunner;
pub use snapshot::{TaskListSnapshot, TaskStatusSnapshot, TaskStatus, TaskPlacementStatus, ...};
pub fn register(...);                         // bootstrap entry
pub mod registry;                             // TaskScope/TaskDispatch etc.
pub mod runner;                               // TaskRunner internals
pub mod service;                              // TaskService
```

Most consumers only need `task`, `TaskResult`, and occasionally the
scope/dispatch enums.
The `service` module is for the bootstrap layer; the `runner` module
is for advanced cases that build their own runners (e.g. tests).

`_macros::inventory` is re-exported `#[doc(hidden)]` for the macro's
generated code; consumer crates **must not** depend on `inventory`
directly.

---

## 9. Components

| File | Approx LoC | Role |
|---|---:|---|
| `lib.rs`       |  50 | Public surface and bootstrap entry; re-exports `task`, instantiates `TaskService`, and walks `inventory`. |
| `registry.rs`  |  70 | `TaskRegistration`, `TaskScope`, and `TaskDispatch`. |
| `manager.rs`   |  — | Named enable/disable/reload/run commands plus list/status fleet snapshot aggregation. |
| `snapshot.rs`  |  — | Public status model, generation-independent execution state, and atomic kind `226` per-member Orbit snapshots. |
| `pool/controller.rs` | 350 | Coordinated `one` / `pool` slot acquisition, renewal, and release state machine. |
| `pool/reload.rs` | 130 | Task-scoped coordinated controller generation and reload ownership. |
| `runner.rs`    | 477 | `TaskRunner` — automatic trigger loops, bounded explicit runs, retry/backoff with jitter, and occurrence-local timeout cancellation. `TaskRuntime` per-attempt context. |
| `service.rs`   | 460 | `TaskService` — Provider + Reloadable + Runnable. Owns the loaded `TaskTable`, dispatches `start_tasks` / `cancel_all` / `wait_all_stopped`, and the reload coalescing path. |
| `process/mod.rs` | 173 | External-process supervision loop and completion/retry classification. |
| `process/spawn.rs` | 177 | Clean command environment, reserved `CLAVIRON_TASK_*` vars, spawn, and instance publication. |
| `process/child.rs` | 150 | Uniform live-child wait, signal, graceful termination, kill, and reap primitives. |
| `process/instance.rs` | 359 | Stable instance path, inherited kernel lock, PID/start identity record, and command fingerprint. |
| `output.rs` | — | Shared `LogConfig` / `LogTarget` vocabulary plus task-local writers for embedded executors. |
| `process/logger.rs` | — | Applies the shared output policy to child-process stdout/stderr. |
| `process/spec.rs` | 210 | `CommandSpec` and external-process restart, exit, reload, and signal policies. |
| `process/tests.rs` | 194 | Process lifecycle, action, and persistent reattachment tests. |
| `config.rs`    | 665 | `TaskServiceConfig`, `TaskConfig`, timing, attempts, pool, and metrics policy, plus parsing tests. |
| `error.rs`     |  68 | `Error`, `Reason`, `TaskResult` — task-specific result types. |

The `#[task]` proc-macro lives in a sibling crate (Rust requires
proc-macros in their own crate). It's re-exported as
`claviron_tasks::task` and **consumers never depend on the
proc-macro crate directly** — its existence is purely an
implementation detail of how Rust ships proc-macros.

---

## 10. Configuration shape

```yaml
# config/tasks.yaml

# Service-wide knob — applied during shutdown to bound the time we
# spend waiting for in-flight tasks to drain. Distinct from per-task
# `graceful_timeout` (which bounds a single task's stop). Omit or set
# to null to wait indefinitely.
graceful_timeout: "30s"

# Management status exists independently from the optional metrics feature.
snapshot:
  publish_interval: 1s
  max_age: 5s

metrics:
  enabled: true
  publish_interval: 1s
  max_age: 5s
  rss_sample_interval: 5s

# Shared daemon slots fail over after this lease if their owner disappears.
# Live owners renew every third of the duration. Default 15s.
pool:
  lease_ttl: "15s"

# Defaults for every external `command:` task below.
#
#   path — PATH for a command that sets no PATH of its own. A spawned
#          command gets a cleared environment, so this is the whole
#          answer to where this deployment's executables live. `$VAR`
#          and `${VAR}` expand against the supervisor's environment when
#          the child is spawned; a segment whose variable is unset is
#          dropped rather than half-resolved. Default (Linux):
#            $HOME/.local/bin:/usr/local/sbin:/usr/local/bin:\
#            /usr/sbin:/usr/bin:/sbin:/bin
#          macOS additionally carries /opt/homebrew/bin. Per-task
#          `env: { PATH: ... }` overrides it.
command:
  path: "$HOME/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

tasks:
  # Rust task — body lives in a #[task(name = "cache_gc")] fn somewhere.
  cache_gc:
    timing: { kind: interval, enabled: true, every: "1h", skip_on_boot: true, execution_timeout: "30s" }
    scope: workers

  # Cron-scheduled Rust task.
  sync_domains:
    timing:
      kind: cron
      enabled: true
      expression: "0 0 * * * *" # standard 6-field cron (sec min hr day mon dow)
      execution_timeout: "2m"
    attempts: 3
    backoff: "2m"
    scope: workers
    dispatch: one

  # External-process task — the default exit action is the SIGTERM ladder.
  queue:
    timing: { kind: daemon, enabled: true }
    scope: workers
    dispatch: pool
    command:
      program: php
      args: [ "artisan", "queue:work", "--sleep=1", "--tries=3" ]
      cwd: /sites/example
      env_file: sites/example/.env
      env: { APP_NAME: queue }
      restart: on-failure      # default; never | on-failure | always
      exit_action: terminate   # default
      reload_action: restart   # optional; restart | { signal: hup|kill }
      log:
        stdout: /var/log/queue.log
        stderr: /var/log/queue.err
    concurrency: 4
    attempts: unlimited        # explicit unlimited restarts
    graceful_timeout: "20s"

  # Embedded executors use the same output policy without redirecting the
  # Claviron process's global file descriptors. Relative paths use task cwd.
  octane_queue:
    timing: { kind: daemon, enabled: true }
    scope: workers
    php:
      cwd: ../web/octane-app
      script: artisan
      args: [ "queue:work", "--sleep=1", "--tries=3", "--no-ansi" ]
      log: worker.log
```

`log` has the same string or `{ stdout, stderr }` shape for `command:`,
`php:` and `js:`. Each stream goes to one of:

| Target | Where |
|---|---|
| `inherit` (also `journal`; the default) | The server's own log — the system log file, journald or the console, wherever the server writes — one line per event, `target: task`, with `task` and `stream` fields. |
| `console` | The process's raw stdout/stderr, untouched: a command's child is handed the server's descriptors. |
| `null` | Discarded. |
| a path | Appended to that file. A relative path resolves against the block's `cwd`, else the process working directory. |
| `{ channel: <name> }` | Each line a record in a channel `logging.yaml` declares (`log-pipeline` feature); a whole-log form, not per stream. |

`inherit` used to mean the raw descriptors; it now follows the server's log,
so a task's lines carry the same time and role as the server's and land
wherever the operator sent the server's output. `console` keeps the old,
zero-cost passthrough. For a command, `inherit` and `channel` pipe the
child's stream and read it line by line on the runtime; `console`, `null`
and paths give the child a descriptor. Embedded PHP and JavaScript tasks
receive task-local writers that cut their output into lines, so concurrent
task threads never touch process-global stdout/stderr. A file that cannot be
opened, or a channel that is not declared, is said and goes to the server's
log (commands: the console) instead.

Per-task fields (full docs in `config.rs`; process-command fields in
`process/spec.rs`):

- `scope` (`master` / `workers` / `fleet`, default `fleet`) — eligible
  runtime roles. The removed legacy `mode` key is rejected.
- `dispatch` (`each` / `one` / `pool`, default `each`) — local execution,
  one fleet-wide automatic owner, or fleet-shared daemon capacity.
- `timing` (optional enum, default manual) — manual, daemon, interval, or cron;
  see §3.
  Daemon, interval, and cron own their live `enabled` trigger state (default
  true). Interval timing owns `every` and `skip_on_boot` (default true); cron
  timing owns `expression`. Manual, interval, and cron each own
  `execution_timeout` (default 30s) and accept `timeout` as its concise config
  alias; daemon has no such field. Invalid fields, or a missing required field
  inside the selected variant, fail config parsing.
- `concurrency` (u8, default 1) — local workers for `each`, fixed `1` for
  `one`, and scope-wide desired slots for `pool`.
- `attempts` (positive integer or `unlimited`, default 1) — maximum total
  attempts for the current execution. Zero is rejected; unlimited behavior
  must always be requested explicitly. For pool dispatch the execution is one
  slot ownership session: exhaustion releases the slot for re-acquisition, and a
  new owner (possibly the same runtime) starts again at attempt 1.
- `backoff` (duration, default 1s) — base for exponential backoff.
- `graceful_timeout` (duration, optional) — per-attempt grace window.
- `silent` (bool, default false) — suppress per-attempt info logs;
  errors still log normally. Use for sub-second / chatty tasks.
- `command` (CommandSpec) — when present, this is an external-process
  task; the Rust body slot is unused. Unknown command fields are rejected,
  including the removed `retry_on_nonzero` boolean.
  - `env_file` retains an environment source path separately from inline
    `env`. Task config preparation does not read or merge its contents.
    Dotenw reads it at execution start. Absolute paths are used verbatim; relative
    paths resolve against `state.config_dir()`, matching PHP config.
    Inline `env:` values override file values.
    The deployment variant `<env_file>.<state.environment()>` overlays the
    base file before inline values. Bounded occurrences only read; daemon
    executions hold an execution-owned `EnvNotification` until their future
    ends. One store-level watcher shares paths between these notifications.
    The existing child/executor wait selects on `notified()`; there is no
    per-execution watcher task. Invalid replacements log and retain the live
    execution. Equal effective values do not renew it.
    A valid change drains only that daemon's execution before replacing it
    in the same task slot and attempt, without reloading TaskService or
    releasing pool/controller ownership. Commands use the configured
    SIGTERM/grace/reap ladder even when process exit policy permits detachment;
    embedded executors receive a child cancellation token and must join owned
    work before returning. Failed embedded drain refuses a replacement.
    Neither evaluation nor renewal writes to process environment. The raw
    task recipe remains unchanged; evaluated values live only in the current
    execution spec. PHP/JS adapters consume those values; Lua's existing task
    adapter does not yet expose the environment to its script.
  - `restart` classifies the command's own completion. `never` makes start
    failure or any unsuccessful exit terminal. `on-failure` (default) sends
    start failures, non-zero exits, and externally delivered signals through
    `attempts`/`backoff`; exit code zero completes. `always` also restarts
    after exit code zero and is valid with daemon or manual timing.
  - `exit_action` controls normal Claviron shutdown through `ExitAction`;
    optional `reload_action` controls task-service reload through the distinct
    `ReloadAction`. Omitted reload action is a no-op for an unchanged command.
    `restart` sends SIGTERM, waits for `graceful_timeout`, then SIGKILLs and
    reaps before the candidate plan starts. `{ signal: kill }` sends SIGKILL
    immediately and reaps. All other reload signals notify an unchanged daemon
    in place under `dispatch: each` or `pool`, preserving supervision and leases.
    `exit_action: keep-running` remains an exit-only choice.
  - Persistent commands must remain foreground processes and retain inherited
    file descriptors. Daemonising again or closing arbitrary descriptors would
    release the kernel instance lock and violate reattachment guarantees.

---

## 11. Interesting properties worth knowing about

A few things that aren't obvious until you've stared at the code:

- **Inventory + DCE.** `inventory::submit!` in the macro keeps the
  registration alive against linker dead-code-elimination — a `#[task]`
  fn referenced only via the macro will still register at startup.
  No `crate::tasks::*` import incantations needed in the consumer.
- **Token reset on cancellation.** `start_tasks` calls
  `runner.reset_token()` only after its active-supervisor table proves that no
  previous generation still owns the old token. A cancelled token must be
  replaced before spawn, but replacing it while the old scheduler is alive
  would sever that scheduler from cancellation.
- **Backoff jitter and reset window.** ±20% jitter avoids retry
  storms across instances. The 5-min reset window means a task that
  ran successfully for 5+ minutes before failing gets the gentlest
  possible retry, not the harshest — bias toward "this is a
  transient failure of a stable task".
- **Reload coalescing.** Multiple SIGHUPs while a reload is in flight
  collapse to one. Prevents reload storms from double-bouncing tasks
  during noisy config-management edits. Parsing and policy validation happen
  before the candidate snapshots are published or old runners are cancelled;
  an invalid reload leaves the last valid task set running unchanged.
- **External-process env hygiene.** `env_clear()` first, then layered
  defaults (`PATH`, `LANG`), then operator allowlist, then reserved
  `CLAVIRON_TASK_*`. Children **do not** inherit the supervisor's
  environment — secrets in the parent process don't leak by accident.
- **Same retry for Rust and external bodies.** A `php artisan`
  command and a Rust async fn both ride the same
  `run_with_retries` loop, observe the same `attempts` /
  `backoff`, and report failures via the same `Error` enum. One
  mental model.
- **Cancellation source, not signal number, selects process policy.** Normal
  Claviron shutdown applies `exit_action`; a task-service reload only applies an
  explicitly configured `reload_action` unless a structural rebuild is needed.
  Owner-driven cancellation returns terminal `Reason::Shutdown` and bypasses
  restart policy. An in-place reload signal does not cancel the owner. If it
  causes the process to exit, that exit goes through `restart`, just like a TERM
  or INT observed without owner cancellation or a non-zero exit.
- **Skipped ticks log, but never queue.** With `timing.overlap: skip`, interval / cron mode under
  load logs a `⏸️  skipped — previous run still active` warning and
  drops the tick. No silent build-up of a tick queue that could
  monopolise the runtime once the slow run finally finishes. Missed cron
  occurrences coalesce at the scheduler's next poll; skipped overlap
  admissions and fail-closed arbitration increment
  `claviron_task_skipped_total`; a normal `dispatch: one` lock yield does
  not, because another runtime executes that occurrence successfully.
- **Fleet task gauges describe the system, not process sums.** The formatter
  derives `claviron_tasks_running` from the merged per-task `in_flight` map,
  so a task active in two workers counts once. Static `task_info` metadata is
  carried compactly inside the existing kind `218` per-task record and
  deduplicated by task name; it does not require another ring.
- **RSS platform code lives in `sys`.** The task sampler only tracks
  live command PIDs, calls `claviron_sys::rss::read_rss`, and aggregates
  successful readings by task and role. It has no OS-specific branches.
  Linux uses `/proc`, FreeBSD uses `sysctl`, and macOS uses the existing cheap
  `proc_pid_rusage` path. Unsupported platforms return `None`, which simply
  leaves that PID out of the sample. macOS support keeps development output
  useful; it does not add a production-platform guarantee or a heavier
  sampling mechanism.

---

## 12. Status & remaining work

The crate is in production use. Open items:

- **Scheduled overlap scope.** `timing.overlap: skip` uses one permit across a
  runner's local trigger slots; `allow` admits concurrent occurrences.
  Each occurrence has its own child cancellation token, so its execution
  timeout does not cancel sibling occurrences or the schedule. Interval
  owners drain all admitted bodies before ending; cron registrations track
  pending admission through completion before declaring a generation stopped.
- **Richer cluster task metrics.** This crate owns `TASK_METRICS`, its snapshot
  and Prometheus formatter, Orbit kinds `217`-`219`, publication, collection,
  freshness, and managed `Reuse` / `Preserve` lifecycle. Task-info rows travel
  with kind `218`; duration
  histograms remain local until a bounded cluster transport is needed.
- **Listener pattern as a first-class option.** The Provider +
  `Runnable` shape is flexible but verbose; a small helper macro or
  builder for "spawn a long-running task that's also registered as
  a Provider" would lower the boilerplate for cluster listeners.
- **Richer execution phase.** Management snapshots expose supervisor lifecycle,
  current in-flight bodies, and the last terminal execution outcome. They do
  not yet distinguish an active scheduler wait from retry backoff or publish a
  next-wake timestamp/current attempt.

[`TaskRunner`]: src/runner.rs
[`TaskRegistration`]: src/registry.rs
[`process::run_command`]: src/process/mod.rs

## Completion barriers

`completion.rs` retains shared join futures for local supervisors and pool
controllers. Lifecycle callers snapshot joins under the owning registry lock,
then await without holding locks. Completed joins remain observable to late or
concurrent observers; dropping a waiter or reaching its explicit grace deadline
never aborts the execution. Reload collects only selected generations. Explicit
runs publish completion after registry retirement. Cron registrations expose a
shared status waiter; it registers before checking closed/pending state and
broadcasts on retirement or occurrence drop, so concurrent observers cannot
consume one another's wake. Drain/reload have no progress polling timer.
