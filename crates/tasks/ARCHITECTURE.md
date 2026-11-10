# Tasks — Architecture

`claviron-tasks` runs the work that lives outside the request path: Rust
bodies compiled into the binary, external commands, scripts on an embedded
engine, and the processes of an Orbit fleet. Every one of them is a named
**task** with one entry in the task table, one supervisor in each runtime that
is eligible to run it, and one stop contract.

This document describes the current behavior. Code comments carry the
reasoning for individual decisions; the configuration types in
[`src/config.rs`](src/config.rs) and [`src/process/spec.rs`](src/process/spec.rs)
carry the field-level documentation.

---

## 1. Tasks, bodies and the task table

A task is a name, an entry (`TaskConfig`) and a body. The entry says when the
body runs (timing), where (scope), how many and how they are shared
(dispatch, concurrency), and how failures and stops are handled. The body is
one of:

| Body | Declared by | Runs as |
|---|---|---|
| builtin | `#[task]`, `TaskService::register_fn`, `register_builtin` | A Rust future in the runtime's Tokio runtime |
| command | `command:` | A child process |
| php / js / lua | `php:`, `js:`, `lua:` | A script on an engine the binary carries |
| orbit | `orbit:`, `TaskService::register_orbit` | The processes of one Orbit fleet (§11) |

One entry declares at most one body block. `TaskConfig::executor()` is the
one place that decides which body an entry has.

**The task table** is what runs. It is the prepared `tasks.yaml`, plus the
entries that registered tasks carry for themselves (a `#[task(config = …)]`
or `register_builtin` default, and every `register_orbit` entry). A file entry
under the same name replaces a registered one whole; nothing is merged. A
Rust body with no entry in the table is registered but never started, and an
explicit run of it reports `TaskNotFound`.

**Rust bodies.** `#[task(name = "…")]` turns an
`async fn(AppState, CancellationToken) -> TaskResult<()>` (or one that also
takes `TaskRuntime`) into an `inventory` registration. Its arguments are
`name` (required), `scope`, `dispatch`, `feature` (gates the registration on
a Cargo feature), `config` (the entry, in `tasks.yaml` words) and
`crate_path`. `register()` walks every registration at startup; nothing lists
them by hand. Consumers import the macro as `claviron_tasks::task` and never
depend on the proc-macro crate or on `inventory` directly.

Runner names are `Arc<str>` shared by the runner and every `TaskRuntime`
handed to an attempt. Registration names remain `&'static str`.

---

## 2. TaskService

`TaskService` is a runtime `Component` registered by `claviron_tasks::register`
together with `TaskManager` (§8), the fleet and lock services it needs, and,
outside standalone, the kind `226` snapshot ring. It is:

- **Configurable** from `config_dir/tasks.yaml`. Congif prepares the document;
  the service reads the prepared snapshot and never opens the file. An absent
  file is the empty document.
- **Bootable**: it registers the engine tasks of the installed executors.
- **Reloadable**, priority `10` (lower runs earlier), and a **ConfigObserver**:
  a SIGHUP and a prepared change of `tasks.yaml` both reach the same reload
  (§7).
- **Runnable**: `serve` starts the table's tasks, publishes snapshots on
  their cadence, and waits on its run context. When the context ends it stops
  everything (§6). An empty table still keeps it running, because management
  commands, snapshot publication and shutdown need one owner.

Preparing a candidate table, at boot or reload, folds in the registered
entries, the scheduler policy and the service's `command.path`, settles Orbit
entries (§11), and validates the whole table before anything is published. A
failed boot or reload never exposes a partly accepted table.

**Starting a task.** Only an entry whose automatic trigger is enabled is
started; manual and disabled entries are not. A runtime outside the entry's
scope skips it. Then by timing and dispatch:

- `each` + daemon or interval: `TaskRunner::run_supervisor` runs
  `concurrency` local slots.
- `each` + cron: one registration with the builtin scheduler (§3.1),
  whatever `concurrency` says.
- `one` or `pool`: a task-scoped slot controller (§4.2).

TaskService keeps one active supervisor record per task, holding the exact
runner generation with its join (or, for cron, its schedule handle). A runner's
token is reset only once the record proves the previous generation has ended.

---

## 3. Timing

`timing` is the trigger policy; it says nothing about how long the body lives.

| `timing` | Trigger |
|---|---|
| omitted, or `{ kind: manual, execution_timeout }` | None. `TaskManager::run` executes one occurrence. |
| `{ kind: daemon, enabled }` | Once while enabled. The body may be long- or short-lived; its completion goes through the retry policy (§5). |
| `{ kind: interval, every, skip_on_boot, overlap, enabled, execution_timeout }` | Every `every`. |
| `{ kind: cron, expression, overlap, enabled, execution_timeout }` | At each occurrence of a six-field (seconds first) UTC cron expression. |

- `enabled` defaults to true and exists only where there is a trigger.
- `execution_timeout` (alias `timeout`, default 30s) bounds one whole
  occurrence, retries and backoff included. When it fires, a Rust body is
  dropped at once and a command is killed and reaped, whatever its
  `exit_action`. Daemons have none: enable, disable, reload and shutdown own
  their lifetime.
- Interval under `each` ticks from the runtime's own clock: the first tick
  after one interval, or at once with `skip_on_boot: false`. Under `one` it
  ticks on UTC buckets aligned to the Unix epoch, so runtimes that booted at
  different times agree on the occurrence; `skip_on_boot: true` then waits for
  the next bucket boundary.
- `overlap` (interval and cron; default `skip`). `skip` refuses an occurrence
  while the previous one of this runner still runs, retries included; it logs
  a warning and counts it as skipped. `allow` starts it concurrently, so an
  interval task with `concurrency: N` then runs N copies per tick. Neither
  queues an occurrence.

### 3.1 The builtin scheduler

`register()` installs the `scheduler` task: a builtin daemon with scope
`any` and dispatch `each`, so every runtime runs one. Its only setting is the
service-level `scheduler.poll_interval` (default 60s, positive). It wakes on
that declared cadence, takes the due registrations from one deadline heap and
starts their bodies; there is no timer per cron task. Consequences:

- Cron fires only while the scheduler runs. An occurrence may be up to one
  poll late, and an expression finer than the poll fires at most once per
  poll. Missed occurrences coalesce into one.
- The next occurrence is computed after both the poll time and the consumed
  deadline, so a backward clock step cannot replay one.
- `tasks.scheduler` is rejected; the scheduler is managed by its name like any
  task. Stopping it pauses cron admission without removing registrations or
  cancelling bodies already running. A change of `poll_interval` restarts
  only the scheduler.

---

## 4. Placement: scope, dispatch, concurrency

Claviron runs as a master with workers, or as one standalone process.
`TaskScope` and `TaskDispatch` are Continuo's placement types, re-exported
through `claviron_orbit::placement`.

**Scope** says which runtimes may run a task: `master`, `worker` (default) or
`any`. `workers`, `fleet` and `both` are accepted spellings of `worker` and
`any`. A standalone process plays every role.

| `scope` | master | worker | standalone |
|---|:-:|:-:|:-:|
| `master` | ✅ | | ✅ |
| `worker` | | ✅ | ✅ |
| `any` | ✅ | ✅ | ✅ |

**Dispatch** says how execution is shared among the eligible runtimes, and
gives `concurrency` (default 1, never 0) its meaning:

| `dispatch` | Behavior | `concurrency` |
|---|---|---|
| `each` (default) | Every eligible runtime runs the task itself. | Local slots per runtime |
| `one` | Eligible runtimes contend for one slot; its owner runs the trigger. | Must be 1 |
| `pool` | Eligible runtimes share daemon slots and run the ones they hold. | Slots across the scope |

`pool` requires daemon timing and a scope other than `master`. Outside
standalone, `one` and `pool` need the Orbit fleet.

**Resolution order.** The entry's `scope`/`dispatch`, else the body's default
(command, php, js and lua: `worker` and `each`), else the Rust runner's
registration. The filter is applied whenever a task starts, at boot, reload
and management start, and again on every explicit run. The reserved child
variable `CLAVIRON_TASK_SCOPE` keeps the older spellings
`master|workers|fleet`.

### 4.1 Examples

- `acme_gc`: `scope: master` — challenge cleanup lives beside ACME, which runs
  on the master.
- `cache_gc`: `scope: worker` — each worker cleans its own memory cache.
- `sync_domains`: `scope: worker, dispatch: one` — any worker may sync; one
  lease owner runs each cron occurrence.
- A Laravel queue: `dispatch: pool, concurrency: 3` — three consumers shared
  by the workers.

### 4.2 Slot controllers (`one` and `pool`)

Each eligible runtime runs one lightweight controller per coordinated task.
Slots are Orbit locks named `task:<name>:pool:<slot>` in the
`claviron.tasks.pool` namespace; `one` is slot 1. Standalone uses the same
controller without locks.

- The controller acquires at once, then reconciles and renews every
  `pool.lease_ttl / 3` (default lease 15s). A slot whose renewal fails is
  cancelled locally. A killed owner cannot release, so peers take its slot
  after the lease expires.
- One pass acquires at most about `ceil(desired / eligible runtimes)` slots,
  so an early runtime cannot take every slot before its peers join. Eligible
  runtimes are counted from current fleet membership on every pass; an empty
  or unavailable membership defers acquisition while held leases still renew.
- A slot runs the runner's normal attempt loop and keeps its lease through
  retries and backoff. When the body ends — success, terminal exit, panic or
  spent attempts — the lease is dropped and the slot can be acquired again by
  any eligible runtime, the former owner included.
- On stop or reload the controller keeps renewing until its bodies have
  joined, then releases. A replacement generation never overlaps the old owner.

---

## 5. Attempts and backoff

`attempts` (1–255 or `unlimited`, default 1) bounds the attempts of one
execution; the default performs no retry, and unlimited must be written.
Only a body's ordinary error (`Error::Other`) is retried; a terminal exit or
I/O error ends the execution at once. Between attempts the runner waits:

- before attempt 2: `backoff` (default 1s), exactly;
- after that: `backoff · 2^(n-1)` with the exponent clamped at 10, capped at
  60s, then jittered by ±20% (so up to 72s).

An attempt that ran for at least 300s before failing makes the following
attempt count from 1 again and waits only `backoff` before it: a stable task
that fails is treated as a transient failure. The limit is checked first, so
the default single attempt still ends; with a finite limit above one, a body
that always fails after more than 300s retries without end.

What follows spent attempts:

- `each` daemon: that slot stays stopped (its siblings keep running) until a
  reload that selects the task, or a management start, restart or reload.
- interval and cron: the occurrence is over; the next one starts at attempt 1.
- `one` and `pool`: the lease is released and the slot is acquired again with
  a fresh count (§4.2).

A panicking body ends its slot without retry and records a failed execution.

---

## 6. Cancellation and stop

Every execution runs under a `CancellationToken`. A runner holds one token per
generation; an attempt runs under a child of its slot's token (the runner's
for `each` daemons, the schedule's for occurrences, the lease's for
controller slots). Explicit runs hang from the runtime's shutdown token and
are tracked separately, so they can be cancelled by name.

The runner's shared stop state carries the cause (`Shutdown` or `Reload`; an
occurrence's own `ExecutionTimeout` is kept per occurrence), the selected
reload action, the in-place reload signal and the pid each slot's command runs
as.

**How a body stops.** A Rust body is a future the runner owns: after
cancellation it gets the task's `graceful_timeout`, or unlimited time when
that is not set, and is then dropped. Commands and engine scripts stop
themselves: a command through its exit or reload action (§9), a script
through its engine's cancellation, and the runner does not add a second
deadline.

**Shutdown.** `cancel_all` marks every supervisor stopping, cancels runners,
cron schedules, slot controllers and explicit runs, and `serve` then awaits
the shared completion of all of them, bounded by the service-level
`graceful_timeout` (unset: no bound). Nothing polls a counter. When the bound
passes, the service logs which work was still running, publishes a final
snapshot and returns; it does not abort that work. Joins are shared futures:
late or concurrent observers see the same completion, and dropping a waiter
never cancels the work.

---

## 7. Reload

A reload reads and validates the candidate table before touching anything,
classifies each task against the active one, drains the selected tasks within
one deadline (the candidate's `graceful_timeout`), and only then publishes.

- **One at a time.** A reload takes the service's reload lock, which
  management commands also take. A reload that finds it held waits, then
  reads the newest configuration; a change that lands during another reload
  is applied by the reload it triggered.
- **Classification.** Each task is unchanged, started, stopped, rebuilt
  (executor, Orbit block, scope, dispatch, concurrency, overlap or timing kind
  changed), rescheduled (other timing fields) or reconfigured (command, Orbit
  declaration, attempts, backoff, graceful timeout, silent). A disabled entry
  counts as absent. Every class but unchanged drains and restarts the task;
  the names are reported in the log. Changing only `reload_action` or the
  service `command.path` is unchanged. A changed `pool.lease_ttl` selects
  every coordinated task.
- **Unchanged tasks** are left alone, unless their command declares a
  `reload_action`: `restart` and `{ signal: kill }` drain and restart them,
  another signal is sent in place (§9).
- **Success** publishes the table, rebuilds the command and engine runners,
  starts the selected tasks, and then sends the in-place signals.
- **A missed deadline** fails the reload with "candidate config was not
  published". The selected generations were already asked to stop; once they
  have, they start again from the table that stayed published. A later reload
  or command that started them first wins.

Explicit runs of a task that a reload selects are cancelled with it.

---

## 8. Management

`TaskManager` is a domain handle resolved from `AppState`. It publishes typed
commands as Orbit fleet events; every member applies them within its own scope
eligibility, and the manager never picks a member. Commands are validated
when submitted and again when applied, and are refused during shutdown.

- `start` / `stop` switch an automatic trigger on or off by changing the live
  `enabled` of the member's timing; `stop` drains the supervised bodies, even
  commands whose `exit_action` would leave them running at exit. Manual tasks
  have no trigger to switch.
- `restart` drains and replaces an enabled daemon generation; every local
  slot drains before its replacement starts.
- `reload` applies the task's `reload_action`, or restarts a daemon that
  declares none; for a non-daemon it does nothing.
- `run` executes one occurrence in the runtime that receives it, without an
  event and without changing trigger state: scope still applies, dispatch and
  concurrency do not, and independent runs may overlap. Daemons refuse it. It
  waits for the result within the timing's `execution_timeout` or a one-call
  override; a caller that goes away drops only its waiter. A disabled interval
  or cron task may still be run.
- `list` and `status` are observations (§13); `processes` reads the
  persistent command inventory (§10).

Commands report acceptance, not fleet convergence. A member that boots later,
or reloads, reads the desired state from `tasks.yaml` again; there is no
durable command store.

---

## 9. Command processes

`register_commands` turns every `command:` entry into a runner whose body is
`process::run_command`.

**Environment.** The child starts from a cleared environment:

1. `PATH` from the service's `command.path`, unless the task's own `env` sets
   one. `$VAR` and `${VAR}` expand against the server's environment at spawn;
   a segment naming an unset variable is dropped whole.
2. `PWD` when `cwd` is set, and `LANG=C.UTF-8` unless `LANG` or `LC_ALL` is
   declared.
3. The declared environment: `env_file` (used as written), its deployment
   variant `<env_file>.<environment>` when present, then inline `env`, which
   wins. A missing base file fails the attempt. Only declared values reach the
   child; the server's own values are used for expansion only.
4. The reserved identity, which declared values cannot override:
   `CLAVIRON_TASK_NAME`, `_SLOT`, `_ATTEMPT`, `_SCOPE`, `_DISPATCH`,
   `CLAVIRON_RUN_MODE`, `CLAVIRON_PROCESS_ID`, and on a master or worker
   `CLAVIRON_WORKER_COUNT` (a worker also gets `CLAVIRON_WORKER_NUMBER`).

A daemon keeps watching its declared environment while it runs. A valid change
drains the child with the terminating ladder (never detaching it) and starts
the replacement in the same slot and attempt, without a reload and without
releasing a lease. An invalid change is logged and the live child kept.

**Completion.** `restart` classifies the child's own exit: `never` makes any
failure terminal; `on-failure` (default) retries start failures, non-zero
exits and signals; `always` also retries exit code zero and needs daemon or
manual timing.

**Exit action** applies when the server stops:

- `terminate` (default): SIGTERM, then SIGKILL after `graceful_timeout`; with
  no grace it waits for the child as long as it takes.
- `{ signal: kill }`: SIGKILL and reap.
- `{ signal: <other> }`: send it and stop supervising.
- `keep-running`: send nothing and stop supervising.

The last two are refused for scheduled tasks and for `one`/`pool`.

**Reload action** applies on a reload or a management reload of an unchanged
command: omitted does nothing, `restart` drains and restarts, `{ signal: kill }`
kills and restarts, and any other signal (`hup`, `int`, `quit`, `term`,
`usr1`, `usr2`) is sent to the live child without cancelling its runner or
releasing its lease. If the child exits in response, `restart` decides what
follows. In-place signals are refused for scheduled tasks.

An owner's cancellation always ends with `Reason::Shutdown` and never enters
the restart policy.

**Orphans.** On Linux a child with a terminating exit action gets
`PR_SET_PDEATHSIG(SIGKILL)`, and exits at once if its parent already changed.
Elsewhere a child may outlive its owner; its inherited instance lock (§10)
then lets the next owner attach to it instead of starting a duplicate.

---

## 10. Persistent commands

A daemon command is a persistent instance: one stable identity per slot that
a later runtime can attach to.

- **Where.** A private directory (`0700`, owned by the effective user) under
  `$RUNTIME_DIRECTORY`, `$TMPDIR` or `/tmp`, named after the application and
  its configuration directory. Each instance is a hash of
  `each:<role>:<task>:<slot>`, `pool:<task>:<slot>` or `one:<task>`; that key
  is what the inventory reports.
- **`.instance`**: the child inherits a kernel `flock` on it and holds it as
  long as it lives. The record holds pid, kernel start time and a fingerprint
  of program, arguments, cwd and the evaluated environment. A later runtime
  that finds the lock busy checks pid, start time and fingerprint and attaches
  instead of spawning. A live instance with another fingerprint fails the
  attempt. An attached daemon that exits is refilled at once unless
  `restart: never`.
- **`.controller`**: a close-on-exec lock held for the whole supervision
  session, so two runtimes never act on one command. Acquiring it waits only
  for a short rendezvous with inventory readers.
- **`.presence`**: an atomic record of the command's identity, task, dispatch,
  slot, birth owner, last controller and last phase (`Running` when spawned
  or attached, `Stopping` before a terminate, kill or environment drain).
  Failing to publish it is logged and never drops supervision.

Persistent commands must stay in the foreground and keep their inherited
descriptors; daemonising again would release the lock that makes them
recoverable.

`TaskManager::processes()` scans the whole application namespace, whatever
the current table or topology says, and never starts, adopts, signals or
deletes anything. A free controller lock reports `Detached`; missing,
inconsistent or unverified metadata reports `Unknown`, as does a busy lock
whose recorded controller is gone. Corrupt or unreadable records are listed as
issues, not dropped. Older records without presence remain readable and
attachable with unknown owner fields. Hand commands from an older binary over
only after stopping it: it does not take the controller lock.

---

## 11. Orbit tasks

An `orbit:` entry runs the processes of one Orbit Link fleet:

```yaml
tasks:
  c-link:
    orbit:
      fleet: c
      kind: task
      workers: { policy: on_pressure, min: 4, max: 16, step: 1, retire_idle: off }
      command: { program: /path/to/c-link, args: [--execution, current] }
```

`TaskService::register_orbit(name, OrbitTask)` registers the same entry in
code. The entry settles into a command task: its `command` moves to the
task's, and it runs on the master as one daemon (`scope: master`,
`dispatch: one`). Scope, dispatch, timing and concurrency are therefore not
written. One task runs each fleet, and never the fleet hosting the service.

The processes create and join their fleet and declare its geometry; the
service never does.

- `kind: runner` starts the one process. Its workers belong to the runner's
  master, so the entry declares no `workers`.
- `kind: task` requires `workers` and takes a host-wide lock on the fleet. It
  starts `max(workers.min, retained)` processes in stable pool slots, where
  retained is the highest slot whose command still holds its instance lock
  (more than `workers.max` refuses to start). It then waits for the Link the
  first process creates (`ApplicationSegment::adopt_when_created`, woken by
  the creator's bell), publishes its growth policy through `FleetGrowth`, and
  watches the fleet without a lane.

It counts the processes it started, never lanes or application names: a
process may claim several lanes, serve several names, or only invoke. A
sender that finds no receiver free reports pressure once per episode; one
episode starts one process, and it ends when that process's pid claims a lane
or its slot ends. Each slot publishes its command's pid in the runner's stop
state, so nothing polls. Growth stops for good once any slot spends its
attempts, and the policy is withdrawn at `workers.max`. On stop the policy is
withdrawn and every slot is drained.

---

## 12. Engine tasks and output

**Engines.** `php:`, `js:` and `lua:` share one block (`script`, `args`, `cwd`,
`env`, `env_file`, `log`, `restart`). The crate that owns an engine installs
it with `TaskService::set_executor`; a task whose engine is not installed has
no runner and reports `MissingRunner`. Engine tasks default to `worker`/`each`,
renew their environment like commands (cancel, await the script, start it
again in the same slot), and have no instance, presence, pid or orphan
handling.

**Output.** `log` is one target or `{ stdout, stderr }`, the same for
commands and engines:

| Target | Where |
|---|---|
| `inherit` (default; also `journal`) | The server's log: one line per event, `target: task`, with `task` and `stream`; stdout at INFO, stderr at WARN |
| `console` | The server's own stdout/stderr, unprocessed |
| `null` (also `none`, `/dev/null`) | Discarded |
| a path | Appended to the file; a relative path resolves against the block's `cwd`; parent directories are created |
| `{ channel: <name> }` | Each line a record in a `logging.yaml` channel (`log-pipeline` feature); whole-log only |

A command gets a descriptor for `console`, `null` and paths, and a pipe read
line by line for `inherit` and channels. Engines get task-local writers, so no
task touches the process's global descriptors. A target that cannot be used
goes to the server's log, except that a command's unopenable file goes to the
console.

---

## 13. Observation

**Snapshots.** `TaskManager::list()` and `status(name)` return
transport-neutral observations; gRPC and the CLI map them. The local table
supplies each task's policy identity (name, body kind, scope, dispatch,
timing, concurrency). Each member publishes its whole local view as one atomic
batch on Orbit kind `226` every `snapshot.publish_interval`, and a reader
accepts only complete batches younger than `snapshot.max_age`. `list()`
publishes the local view first, then collects the current fleet members;
`members_expected` and `members_reporting` make a missing member visible.
Standalone reads the service directly and opens no ring.

A placement's lifecycle is, in this order, `Stopping`, `Disabled`,
`Ineligible`, `MissingRunner`, or the runtime state: `Active` (also a pending
interval or cron, and a manual task with an explicit run in flight) or
`Inactive`. It carries `in_flight`, the last terminal outcome, the owned
slots of a coordinated task, and a monotonic generation, so a client can tell
a replacement from the snapshot taken before it. Lifecycle and outcome are
independent: a waiting interval task is `Active` with a failed last run.
Snapshots never carry arguments, environment, raw errors or logs. At most 1024
tasks with names up to 468 bytes fit a batch; the table is checked against it
when loaded.

**Metrics** (`metrics` feature). `TASK_METRICS` publishes on Orbit kinds `217`
(service totals), `218` (per task: runs, in flight, skipped, attempts and the
static task info) and `219` (per task and role: slot gauges and RSS). Workers
format the fleet aggregate for Prometheus; duration histograms stay local.
The RSS sampler runs while `metrics.enabled` and `metrics.rss_sampler` hold,
samples live command pids every `rss_sample_interval`, and leaves the platform
reading to `claviron_sys::rss`. Without the feature the `metrics` section is
accepted and ignored.

---

## 14. Configuration

```yaml
# config/tasks.yaml
graceful_timeout: 30s          # whole-service drain bound; omitted or null: none
scheduler: { poll_interval: 60s }
snapshot: { publish_interval: 1s, max_age: 5s }
pool: { lease_ttl: 15s }       # one and pool slots
metrics:                       # metrics feature
  enabled: true
  rss_sampler: true
  rss_sample_interval: 5s
  publish_interval: 1s
  max_age: 5s
command:
  # PATH for commands that set none. Defaults:
  #   Linux: $HOME/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
  #   macOS: $HOME/.local/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin
  path: "$HOME/.local/bin:/usr/local/bin:/usr/bin:/bin"

tasks:
  cache_gc:                    # a #[task] body
    timing: { kind: interval, every: 1h }
  sync_domains:
    timing: { kind: cron, expression: "0 0 * * * *", execution_timeout: 2m }
    dispatch: one
    attempts: 3
    backoff: 2m
  queue:
    timing: { kind: daemon }
    dispatch: pool
    concurrency: 3
    attempts: unlimited
    graceful_timeout: 20s
    command:
      program: php
      args: [artisan, queue:work, --sleep=1, --tries=3]
      cwd: /srv/example
      env_file: /srv/example/.env
      env: { APP_NAME: queue }
      restart: on-failure
      exit_action: terminate
      reload_action: { signal: hup }
      log: { stdout: queue.log, stderr: queue.err }
  octane_queue:
    timing: { kind: daemon }
    php: { cwd: ../example-app, script: artisan, args: [queue:work], log: worker.log }
```

Every section and entry refuses unknown fields. Per-task fields and defaults:

| Field | Default | |
|---|---|---|
| `command` / `php` / `js` / `lua` / `orbit` | none | At most one; none is a Rust body |
| `scope` | body default, else the runner's | §4 |
| `dispatch` | body default, else the runner's | §4 |
| `timing` | manual, 30s | §3 |
| `concurrency` | 1 | Never 0 |
| `attempts` | 1 | 1–255 or `unlimited` |
| `backoff` | 1s | §5 |
| `graceful_timeout` | none | Rust bodies after cancellation; commands' terminate ladder |
| `silent` | false | Suppresses per-attempt start/exit info lines |

Validation also rejects: a zero or unrepresentable duration, an unparsable
cron expression, `restart: always` on a scheduled task, `pool` without daemon
timing or with `scope: master`, coordinated dispatch without a fleet outside
standalone, and the non-terminating exit actions and in-place reload signals
listed in §9.

---

## 15. Source map

| File | Role |
|---|---|
| `lib.rs` | Public surface; `register()` wires the fleet, lock, snapshot ring, scheduler, manager and metrics |
| `registry.rs` | `TaskRegistration`; re-exports `TaskScope`, `TaskDispatch` |
| `config.rs` | `TaskServiceConfig`, `TaskConfig`, timing, attempts, body selection, Orbit settling |
| `service.rs` | `TaskService`: table preparation and validation, start, reload, management commands, serve loop |
| `runner.rs` | `TaskRunner`, `TaskRuntime`, `StopContext`; trigger loops, attempts and backoff, overlap |
| `scheduler.rs` | The builtin cron scheduler |
| `pool.rs`, `pool/controller.rs`, `pool/reload.rs` | `one`/`pool` slot controllers and their generations |
| `completion.rs` | Shared join futures for stop and reload barriers |
| `manager.rs` | `TaskManager` |
| `snapshot.rs` | Status model, runtime state, kind `226` batches |
| `orbit.rs` | The Orbit `kind: task` controller |
| `executor.rs` | Engine tasks: `ScriptSpec`, `Executor`, `set_executor` |
| `environment.rs` | Execution environment and its renewal |
| `output.rs` | `LogConfig`, `LogTarget`, task-local writers |
| `process.rs` | `run_command`: supervision, renewal, signals, presence |
| `process/spawn.rs` | Environment construction, spawn, parent-death signal |
| `process/child.rs` | Waiting, signalling and the terminate ladder |
| `process/instance.rs` | Instance lock, record and attach-or-spawn |
| `process/presence.rs` | Controller lock, presence record, inventory |
| `process/spec.rs` | `CommandSpec` and its restart, exit and reload policies |
| `process/logger.rs` | Output policy for a child's stdio |
| `metrics.rs`, `metrics/*` | Task metrics store, Orbit kinds 217–219, RSS sampler |
| `error.rs` | `Error`, `Reason`, `TaskResult` |

Tests live under `tests/` and are included by each module through `#[path]`.
