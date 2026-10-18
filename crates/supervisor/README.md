# claviron-supervisor

Master/worker process supervision for Claviron servers and application runners.

The supervisor starts workers, observes their lifecycle, applies declared
recovery and rotation policies, and coordinates graceful shutdown and rolling
restart. It manages one same-host process family. It is not a network listener,
load balancer, distributed scheduler, or replacement for the host's service
manager.

Applications normally select the root `claviron` crate's `supervisor` feature and
use `claviron::supervisor`. The server composition owns registration; users do
not need to construct a parallel worker registry.

## Process model

Claviron distinguishes master, worker, and standalone execution. In supervised
mode the master owns process control and does not serve application requests.
Workers execute the application services. Standalone execution has no child
worker fleet to control.

Workers re-execute the application's binary with prepared bootstrap information.
The configured worker count expresses desired topology; it is not the compiled
fleet capacity or the currently observed number of running workers.

`WorkerFleet` owns the process state. `WorkerControl` exposes control capability,
and `WorkerManager` is a management handle over that state, not another provider
with a second lifecycle model.

## Control and observation

`WorkerManager` offers list, start, stop, reload, restart, rolling restart, and
worker-resize operations. For example, a management handler can request a rolling
restart:

```rust
use claviron::{
    runtime::AppState,
    supervisor::{CommandKey, WorkerManager, WorkerManagerResult},
};

fn restart_workers(state: &AppState) -> WorkerManagerResult<CommandKey> {
    WorkerManager::new(state)?.restart_workers()
}
```

This requires an already configured supervised runtime. Returning a `CommandKey`
means the command was accepted, not that replacement has completed. Correlate
the command with operation state and worker snapshots. `ServerManager` provides
the corresponding server-level reload and shutdown entry points.

Boot acknowledgement, heartbeat, reload acknowledgement, and metrics are
separate observations. A boot-confirmed worker is not necessarily ready on
every application protocol, and fresh metrics are not a substitute for a
heartbeat. Process incarnation and operation identity keep stale observations
from being mistaken for the current worker.

## Recovery and rolling restart

Recovery uses configured attempt limits, windows, and backoff. Rotation can be
driven by process age or resident memory, independently of HTTP request counts.
These are declared process policies, not heuristics added by a request handler.

Rolling restart requires at least two running, stable workers. It drains and
reaps an old worker before reusing that worker's identity and shared lanes, then
waits for replacement boot confirmation before advancing. Intentionally stopped
slots remain stopped. The operation does not promise application-level
availability under every failure mode.

Orbit fleet ownership is scoped to this process family. Independently launched
masters cannot share one lane allocation scheme merely by using the same fleet
name. Master or host failure recovery still needs external supervision; retained
shared resources also follow their own reconciliation contracts.

## Configuration

The supervisor reads `supervisor.yaml` from the configured directory. Runner
integrations can supply a `SupervisorConfigSource` instead. Policy groups are
explicit:

- `heartbeat`: publication interval and missing-heartbeat timeout.
- `monitor`: process observation interval.
- `snapshot` and `metrics`: publication cadence and observation freshness.
- `startup` and `reload`: acknowledgement deadlines.
- `recovery`: attempt budget, window, and backoff.
- `rotation`: optional memory and age thresholds.
- `shutdown`: permission for bounded termination, if the operator chooses it.

For shutdown without a termination deadline:

```yaml
shutdown:
  timeout: null
```

An explicitly finite shutdown timeout authorizes escalation for the
supervisor's own children after drain. Without one, shutdown waits; dropping a
management handle is not permission to kill a process. Configure other timing
fields for the deployment rather than copying arbitrary values from examples.

## Features and scope

The implementation's optional `metrics` feature adds fleet metrics integration.
Metrics remain independent from lifecycle and heartbeat correctness. Process
control uses platform-specific facilities; this is not a portable process API
that conceals all operating-system differences.

## Status and license

Part of Claviron's alpha development series; APIs may change. This README
describes the current source tree, not the placeholder package on crates.io.

Licensed under either [MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.
