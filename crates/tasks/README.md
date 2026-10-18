# claviron-tasks

Lifecycle-managed background work for Claviron applications.

Rust functions, external programs, and registered language executors share
trigger, retry, placement, cancellation, and management contracts. A task can
run on demand, on a schedule, or as a long-lived daemon without becoming an
unmanaged shell process beside the application.

Applications normally use `claviron::tasks` through the root crate's `tasks`
feature, alongside their runtime composition. A task is a unit of work, not a
process topology or an HTTP application. The aggregate `TaskService` owns its
execution within Claviron's service lifecycle.

## Register work, then choose its trigger

The `task` attribute discovers compiled Rust task bodies through `inventory`.
For a consumer using the root facade, set `crate_path = "claviron"` so generated
references use that facade rather than a direct implementation-crate dependency:

```rust
use claviron::runtime::AppState;
use claviron::tasks::{TaskResult, task};
use tokio_util::sync::CancellationToken;

#[task(name = "refresh_index", scope = "worker", crate_path = "claviron")]
async fn refresh_index(
    _state: AppState,
    cancellation: CancellationToken,
) -> TaskResult<()> {
    if cancellation.is_cancelled() {
        return Ok(());
    }
    // Refresh application-owned state here; observe cancellation during work.
    Ok(())
}
```

This example requires the facade's `runtime` and `tasks` features and
`tokio-util`'s cancellation-token support. Import the macro through the public
task surface; consumers do not need a separate macro or inventory dependency.
`TaskService::register_fn` also supports explicit registration.

Task definitions live under `tasks:` in `tasks.yaml`. Without automatic timing,
a configured body remains available for explicit execution:

```yaml
tasks:
  refresh_index:
    timing:
      kind: manual
```

`register(state)` composes the service, discovered tasks, built-in cron
scheduler, management handle, and fleet support. Configuring an unknown built-in
name does not create a Rust function. A hosted-language definition without its
engine remains unavailable rather than silently selecting another executor.

## Trigger policy and execution lifetime

| Timing | Admission behavior |
| --- | --- |
| `manual` or omitted | No automatic trigger; run one occurrence explicitly. |
| `interval` | Admit occurrences at the declared `every` cadence. |
| `cron` | Admit due UTC cron occurrences through the shared local scheduler. |
| `daemon` | Start when enabled; the body and restart policy determine its lifetime. |

Manual, interval, and cron timing have an `execution_timeout` for one logical
run, including retries and backoff. Its schema default is 30 seconds; declare
it explicitly when the work needs another budget. Daemon timing has no such
execution deadline. Neither choice is the graceful-drain policy.

Cron uses one local deadline heap and the service-level
`scheduler.poll_interval`, rather than a timer per cron task. Missed occurrences
coalesce. Interval timing has its own timer and `skip_on_boot` policy.
Scheduled `overlap: skip` (default) drops admission while an earlier occurrence
runs; `allow` admits concurrently. Neither policy queues a backlog.

`attempts` counts total attempts and defaults to one: no automatic retry.
`unlimited` is explicit; zero is invalid. Retried failures use the runner's
exponential, capped, jittered backoff. Exhausting retries does not make a
side-effecting task transactional: idempotency belongs to the task body.

## Placement across a fleet

Scope selects eligible runtime roles; dispatch selects distribution:

- `each`: local execution in each eligible runtime; `concurrency` is local.
- `one`: one fleet owner of the automatic trigger; concurrency must be one.
- `pool`: a declared total number of daemon slots across eligible runtimes.

The service owns coordination through [Orbit](../orbit/README.md), including
leases and ownership recovery. This is bounded same-host fleet coordination,
not a durable distributed job queue or an exactly-once side-effect guarantee.
Applications must protect external mutations independently.

An explicit `TaskManager::run` is local to the receiving runtime. It does not
broadcast a run or participate in the automatic trigger's overlap/concurrency
limit; a task requiring serialization must own that invariant itself.

## Executors and execution environments

`command:` declares a program, argument vector, working directory, environment
sources, restart policy, process exit/reload actions, and output routing. It
spawns that program directly rather than treating the declaration as shell
syntax. The child starts with a cleared environment plus its prepared values,
configured executable search path, and reserved runtime task identity fields.

`php:`, `js:`, and `lua:` declare script execution through an engine installed
with `TaskService::set_executor`. The task crate owns when and where; the engine
owns how. A script names the script, not a second interpreter executable.
Declaring an executor block does not compile or install that engine, and a task
cannot declare several executor blocks at once.

Environment preparation uses the [runtime](../runtime/README.md)'s shared
[Dotenw](https://crates.io/crates/dotenw) context. Finite runs pin their prepared
environment. Persistent owners can observe changes, drain/join their old
execution, then replace it. An update does not rewrite a running process's
environment or expose every server configuration secret to it.

## Stop, reload, and management

Task bodies receive cancellation and must stop admitting and drain only their
own work. A task's `graceful_timeout` and the service-wide drain budget are
separate; `None` means indefinite drain, not an invented termination deadline.
External-process signals, keep-running policy, and bounded execution deadlines
are distinct contracts. Hosted engines must acknowledge owned execution has
drained before replacement.

Reload compares task plans and reschedules, reconfigures, or replaces affected
generations instead of restarting everything. An unchanged external command is
not signalled merely because configuration reloaded; `reload_action` selects
its explicit behavior.

`TaskManager` exposes lifecycle commands, local execution results, fleet status
snapshots, and recorded command-process inspection. Fleet lifecycle command
submission reports acceptance, not synchronous completion on every peer.
External APIs must add their own authorization; this crate exposes no public
management listener by itself.

Optional implementation features add `metrics` instrumentation and
`log-pipeline` routing to declared log channels. The latter does not replace
task execution or turn output into a job queue.

## Related crates

- [Runtime](../runtime/README.md): service state, configuration, and cancellation.
- [App](../app/README.md): hosted application lifecycle, distinct from task timing.
- [Supervisor](../supervisor/README.md): runtime process ownership and supervision.
- [Embedded PHP](../php-embed/README.md): PHP task executor and native engine.

## Status and license

Part of Claviron's alpha development series; APIs may change. This README
describes the current source tree, not the placeholder package on crates.io.

Licensed under either [MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.
