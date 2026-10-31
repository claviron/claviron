# Worker Supervisor Architecture

`supervisor` turns `RunMode::Master` and `RunMode::Worker` into a
managed process topology. It spawns identical worker processes, proves worker
liveness, restarts failed workers, applies process-level rotation policy, and
coordinates reload and shutdown.

The crate is a control plane. It owns its narrow process-management metric
family, but it is not an HTTP subsystem, domain metrics aggregator, or generic
application event bus.

## Boundary

The supervisor owns:

- the master-side worker process table;
- worker spawn/re-exec and the private bootstrap envelope;
- the Orbit heartbeat record and its liveness policy;
- the Orbit event acknowledgement of worker boot completion;
- the Orbit event notification of worker shutdown completion;
- the Orbit event choreography used for worker reload completion;
- worker-slot and logical fleet-size management commands and status snapshots;
- process-level RSS and age rotation;
- master, worker, and standalone process RSS/uptime metrics;
- worker restart, rotation, configured-count, and alive-count metrics;
- restart backoff, graceful shutdown, and forced-kill escalation.

The supervisor does not own:

- HTTP, PHP, cache, TCP, TLS, vhost, task, or upstream metric families;
- domain-specific request-count or connection-idle rotation policy;
- Orbit cache policy or application event topics.

Domain metrics stay in their producing crates. Losing a domain metric must not
change the supervisor's ability to keep the worker fleet alive.

Heartbeat is vital control data; metrics are observational data. This boundary
also applies to the supervisor's own process metrics, not only domain metrics.
Control state may be exported as metrics; metric snapshots must not become
inputs to worker readiness, liveness, restart, or rotation decisions.

## Runtime Modes

### Domain and service ownership

There is no `supervisor.rs` and no all-purpose supervisor Runnable.

| Owner | Responsibility | Lifetime |
|---|---|---|
| `fleet::WorkerFleet` | Process table, exact identity, phase/intent clocks, operation ownership, shutdown latch | Plain domain state; no runtime/transport hooks |
| `management::WorkerControl` | Narrow API and shared capabilities for named use cases | Non-Runnable provider; public `WorkerSupervisor` alias preserves Rust callers |
| `SupervisorPolicy` | Validate and atomically publish the nested config | Configure and reload; no process effects |
| `MasterIdentityProvider` | Establish master identity and recover topology once | Boot; post-drain non-running cleanup in `Finalizable` |
| `WorkerReconciler` | Initial spawn/adoption and boot convergence, then unclaimed-slot reconciliation | One restartable generation; resuming never duplicates existing slots |
| `WorkerMonitor` | Evaluate health, heartbeat, recovery and rotation policy | One restartable generation; submits operations, never owns their drain |
| `WorkerExitObserver` | Poll the exact kernel-exit futures of tracked processes | One restartable generation; no per-process detached tasks |
| `WorkerOperationRunner` | Execute accepted slot/fleet operations | One generation polls concurrent futures and drains all accepted work before returning |
| `WorkerConfigurationReload` | Apply an accepted policy to workers and open a recovery epoch | Reload hook; work belongs to `WorkerOperationRunner` |
| `WorkerReloadRunner` | Serialize worker-local registry reload cycles | Worker-only generation; event callbacks only submit |
| `WorkerSnapshotPublisher` | Publish the process-table read model | Independent restartable generation |
| `SupervisorMetricsPublisher` | Publish optional process metrics | Independent restartable generation |
| `HeartbeatProvider` | Publish worker heartbeat and typed presence | Independent restartable generation |

`config/`, `fleet/`, `process/`, `lifecycle/`, `monitor/`,
`management/actions/`, `events/`, and `observation/` are private ownership
groups. Public wire/domain exports remain at the crate root. State mutation
belongs to named startup, recovery, rotation, drain, restart, resize and reload
use cases, not a replacement god object.

Stopping a monitor, reconciler, exit observer or publisher stops only that
generation. It does not kill workers, reset policy/identity/clocks, clear the
table or unlink SHM. Initial convergence failure is a distinct boot rollback,
not the meaning of ordinary service cancellation. The monitor does not recover
initial slots until the reconciler has confirmed initial boot.

An operation runner closes admission atomically when stopped, then polls every
already-accepted future to completion without a timeout or abort. A blocked
operation can therefore keep that service stop pending. Its successor cannot
start while the previous generation still owns work. The operation table is
shared with monitoring and management throughout this transition.

`Finalizable` does not drain or signal live workers. It releases exited handles
and local heartbeat observations after runtime drain, idempotently. Live spawned
workers make finalization fail without destroying evidence. Live adopted workers
permitted by the origin shutdown policy retain their shared state. No supervisor
finalizer unlinks a ring still used by surviving processes.

`runtime` defines the topology:

```rust
pub enum RunMode {
    Master,
    Standalone,
    Worker(WorkerId),
}
```

`RunMode` is process identity only. `AppState::worker_count()` is the
supervisor's mutable desired count, while `AppState::fleet_capacity()` is the
immutable Orbit lane geometry established at boot. The process table remains
the authority for which worker children are actually running.

The private worker bootstrap also carries the master's exact `fleet_name`.
Its v6 envelope carries `environment` too: workers keep the master's validated
deployment selection rather than falling back to the re-executed binary's CLI
default. This is a boot selection, not an environment-variable value.
This is independent of the application/process-family instance name and is
copied unchanged into worker `AppState`; workers never derive an Orbit SHM
namespace from their binary, hostname, or application identity. The current
supervisor is still single-family and reserves node zero plus its worker range.
Two independently launched masters must not join one fleet until a common fleet
directory allocates disjoint node ranges; merely sharing a fleet name does not
allocate node ids.

Supervisor registration is shared by master and worker. The embedding registration path
installs the Fleet and Orbit event registry before supervisor registration:

| Mode | Supervisor registration |
|---|---|
| `Master` / `Worker` | The same policy, identity/control capabilities, observers and runnables; `Scoped` selects runnable execution |
| `Standalone` | No supervisor runtime provider |

Standalone is not a one-node supervisor fleet. It has no worker children or
supervisor heartbeat SHM. A standalone process
may still register Orbit explicitly for cache or application event consumers;
that is independent of this crate.

When the optional metrics feature is enabled, standalone registers only the
ordinary supervisor metrics family. It does not gain a supervisor, heartbeat
publisher, or metrics runnable.

## Orbit Is Required

Orbit is a normal dependency of `supervisor`, not a Cargo feature.
There is one liveness implementation:

```text
worker HeartbeatProvider
  -> kind 221, one zero-payload lane per fleet node (presence rides on kind 239)
  -> master HeartbeatObserver folds fresh per-node records
  -> PID-correlated HeartbeatObservation
  -> supervisor timeout policy
  -> targeted drain request, then operator-authorized SIGKILL or an unbounded wait
```

Each worker is the sole cross-process writer of its node-owned lane and
publishes a monotonically increasing record containing its node id and capture
epoch. After validating a `WorkerStarted` event against the tracked child PID,
the supervisor initializes the liveness entry and records the current head of
that worker's lane. Samples older than that point cannot refresh the replacement
process's liveness. Initialization starts the heartbeat grace period with zero
observed beats; duplicate startup events do not reset it. Heartbeat records never
complete boot or promote `Starting` to `Running`. A writer advances
its lane head only after the slot is committed, so a process that dies midway
through a write does not hide an earlier committed heartbeat or block another
node's lane.

Hung-worker signals must match the liveness entry's PID to the current running
child. An old entry cannot terminate a replacement awaiting boot confirmation.

The heartbeat ring is supervisor control data. It is not a metric family and
is not rendered through Prometheus. Heartbeat publication and observation must
work without the optional `metrics` feature or a metrics consumer. A fresh
metric snapshot cannot substitute for a heartbeat, and a missing or stale
metric snapshot cannot mark a worker hung.

Supervised registration requires an already registered Fleet/event
bridge because startup and reload coordination depend on that transport.
Standalone remains unaffected. Application crates may register additional
typed bridges on the same event registry.

## Supervisor Metrics

The optional `metrics` feature exposes only facts owned by process management:

- RSS and uptime for the master and each worker;
- successful worker restart and rotation counters;
- configured and currently-capable worker counts.

Master and worker processes publish one 49-byte process snapshot through
Orbit kind `223`, with a 1,024-slot lane per fleet node. The metrics family
reads the latest fresh snapshot from every node lane and renders one fleet view
from whichever worker receives the Prometheus scrape. Process rows may carry
`role`, `node_id`, `worker_id`, and `pid` labels because fleet lane identity and
process-family worker identity are separate facts owned by this family.

Standalone is a separate role. It renders its own local RSS and uptime without
Orbit and does not pretend to be either master or worker. Restart, rotation,
and worker-count metrics are absent in standalone mode.

`SupervisorMetricsPublisher` publishes at `metrics.publish_interval`, independently of
`HeartbeatProvider`. The collector accepts samples up to `metrics.max_age` old.
Older samples are omitted from the metrics view; this is a read-time freshness
filter, not a wait timeout, SHM deletion, or ring-retention policy. Missing data
is not a zero measurement or evidence that a process is dead.
Kind `223` is registered through Claviron's managed SHM inventory with
`Reuse` / `Preserve`; boot never resets it because freshness already separates
the current observation from retained bytes.

Metrics must not reuse heartbeat intervals, timeout values, tickers, or
publication callbacks. The reverse is also forbidden: heartbeat policy must
not depend on metrics configuration or activity. Both may use the same Fleet
and `SupervisorConfig` type, but they own separate records, runnables, and
timing fields. Identical numeric defaults do not make the fields aliases.

Metrics publication or collection failures never participate in readiness,
liveness, restart, or rotation decisions. Process rotation uses direct OS RSS
and the master's process-start clock, not the exported metrics snapshots.

Other owners must not add worker labels merely because processes are split.
HTTP means total fleet HTTP, PHP means total fleet PHP, and so on. Their
owner-local Orbit families perform that aggregation independently of the
supervisor process family.

## Process Ownership and Heartbeat Observation

[INCARNATION_MASKING](../../docs/design/INCARNATION_MASKING.md) names the
existing separation: worker numbers identify continuing slots; exact process,
incarnation and master identities authorize actions on their current owners.
Replacement retains the slot, while adoption retains the living process too.

`WorkerFleet` owns the authoritative process table: process states,
restart history, and one `process::OwnedProcess` per live slot. An
`OwnedProcess` is the `Child` handle paired with the
`claviron_sys::process::ProcessIdentity` (PID plus kernel start stamp) captured
the moment the child was spawned, and the master-local spawn clock that
rotation age is measured from. A PID is read from that identity, never from
the handle, so a reused PID cannot be mistaken for the process this master
accepted. The spawn clock is the master's; the worker's own start time as an
operator sees it is a separate, worker-written fact that the master never
derives from it. An `Unclaimed` slot holds no process and carries its own
`lifecycle::LaneWatch` (lane head and the time it was last looked at) instead of
borrowing a process clock.

Each slot carries two separate facts from `fleet::model`: a `WorkerIntent`
— what the master wants (`EnsureRunning`, `Stopped`, or `Drain { reason,
after_exit }`) — and a `WorkerPhase` — what it has observed of the process
(`Unclaimed`, `Starting`, `Running`, `Reloading`, or `Exited { outcome }`). A
drain changes only the intent; the process is still there, so its phase stays
what it was. When the process is reaped, the exit is classified from the intent
that stood at that moment: a planned drain gives `ExitOutcome::Planned`, a
process that left while wanted — or that had to be drained because it hung —
gives `ExitOutcome::Failed` and enters the recovery budget. The `Drain` then
yields to its `ExitDisposition` (`StayStopped`, `Replace`, `RemoveSlot`,
`FinishFleetShutdown`), which becomes the slot's next intent. The
operator-facing `WorkerLifecycle` is a projection of the pair, derived on every
read and stored nowhere: a live phase under a `Drain` intent reads `stopping`; an
`Exited` phase reads `stopped` when the intent is `Stopped`, otherwise `crashed`
or `exited` by outcome. Reload is an operation on a running slot, not an intent:
it moves the phase to `Reloading` and back. Only process-control actions spawn,
signal, wait/reap, or change a slot's phase or intent. Startup, reload, and
shutdown event contracts remain in their own action modules.

A process handle never leaves the table. Every `OwnedProcess` carries one
`ExitObservation`, filled by its shared kernel-exit future — which waits on the
kernel identity through `claviron_sys::process::ProcessHandle::wait` and proves
only that the exact process is gone — and by the reap, which happens where the
handle lies, under the table lock, and records the exit status. Rotation, manual
stop, rolling restart, resize and heartbeat recovery all follow the same shape:
set the slot's intent, signal the process in its row through its
identity-validated handle, await the observation (`await_slot_exit`), then reap
and classify under the lock. Fleet shutdown and `wait_workers` await the same
observations and reap afterwards. No waiter polls a handle of its own, several
waiters on one identity see one record, and the row shows its PID for as long
as the process exists — including while it is draining, which is when the
2026-09-13 master lost sight of Worker #1. `shutdown.timeout: Some(d)` is
applied by `await_slot_exit` and the fleet shutdown to the still-live identity;
nothing else in this crate creates a deadline.

The same observation performs Link stream recovery once, before publishing
exit to its waiters. Spawn and adoption bind the exact PID/incarnation and
the existing transport service; they do not retain `AppState` in the callback.
Recovery inspects the local fleet and the configured/previously reached Link
destinations, reclaiming only matching ownership stamps under their kernel
holds. It ends that incarnation's body sides and wakes their holders, not
other workers and not the whole edge. Sending a signal, missing a heartbeat,
or withdrawing a lane from routing is not an exit proof. No ApplicationEvent,
new deadline, polling loop, or SHM record field is involved.

`observation/heartbeat_observer.rs` owns the master-local `HeartbeatObserver`. Its single locked map
holds one `HeartbeatObservation` per tracked worker: PID, the boot/liveness
anchor, first accepted heartbeat time, observed-beat count, and lane-counter
bounds. Counters and observations change under the same lock. There is no second
process-status map. The PID correlates evidence with an incarnation; it is not
ownership of a child or proof that the process is alive.

Startup initializes an observation only after validating the exact child/PID.
A matching shutdown-completed event closes it without marking or reaping the
child. Old-PID shutdown events cannot erase replacement observations. The
shutdown receiver outlives the monitor's policy loop, until master cancellation.

The observer is an internal master-scoped capability provider, not a runnable
or a config owner. It has no boot hook. `MasterIdentityProvider` attaches the
live Fleet once, before topology recovery and worker spawning; the observer
keeps that process-lifetime handle in a `OnceLock`, not a reloadable slot.
The monitor reads the ring
through `stale_workers(timeout)` immediately before evaluating heartbeat policy,
including its stale-result recheck; there is no separately scheduled reader or
cached timeout verdict. It returns identities, and the supervisor revalidates
each PID against the current running child before signaling.

Initial convergence belongs to `WorkerReconciler`, health policy to
`WorkerMonitor`, and process effects to named use cases. Registry-installed
startup and shutdown listeners outlive either generation. Heartbeat observation
does not inherit spawn/restart/rotation responsibilities.

There is no supervisor UDS client, server, wire-message enum, socket path,
connect retry, bind/rebind, or socket cleanup. Restart, rotation, and scale-up
have no network prerequisite. Worker boot, reload, and shutdown completion use
typed Orbit events; heartbeat uses its separate ring. Removed transport code
is recoverable from Git, not retained as dormant scaffolding.

## Worker Presence

The heartbeat on kind `221` is the pulse — a counter and a time, in the exact
zero-payload shape the app-side Orbit status tooling also reads. Presence, on
kind `239`, is the name. On every heartbeat tick the worker also publishes one
`WorkerPresence` on its own lane: its complete `WorkerIdentity` (slot, PID and
kernel start stamp, incarnation, the `MasterIdentity` from its v3 bootstrap),
`runtime_started_at_unix_ms` (the wall-clock start of its Claviron runtime,
written once and never reset — display data, distinct from the kernel start
stamp that liveness is judged by), its own phase (`Booting`, `Running`,
`Draining`), its current parent PID as a diagnostic (`1` once its master has
died), the operation whose drain it accepted, the capture time, and the lane
counter assigned by Orbit. Only the worker writes its lane; the master's view
of the same slot stays in its own snapshot (`197`).

Presence is evidence, never authority: a reader asks the kernel whether the
exact process a record names is alive (`exact_process_alive`, PID *and* start
stamp) before acting on it, and heartbeat liveness policy does not read it.
Boot reconciliation (adopting a live worker of a dead master, or failing closed)
is its consumer.

The configured worker count is a boot floor, not permission to hide a live
higher slot left by a previous manager resize. Before constructing its process
table, a new master scans typed-presence lanes through the immutable worker
capacity. The highest exact live identity raises the effective worker count;
all intervening ids become normal contiguous slots. This reserves the runtime
topology without granting ownership: every surviving process still passes the
ordinary freshness, phase, previous-master, generation and explicit claim
handshake. Dead presence does not raise the count. Boot recovery never shrinks
the recovered topology or drains a higher live slot; only an explicit
`remove-worker` operation may reduce it. Consequently management snapshots and
service membership cannot report `2/2` while a verified worker #3 remains
alive and routable.

## Worker Startup Events

`startup.rs` owns boot acknowledgement, its codec, and the initial wait action:

```text
master installs Orbit inbound cursor during provider boot
  -> master completes all provider boot hooks
  -> supervisor subscribes to WorkerStarted before spawning any child
  -> worker completes provider boot and submits runnables
  -> worker bootstrap emits local ProcessStarted
  -> supervisor worker adapter adds worker_id and pid
  -> private WorkerStarted envelope crosses the Orbit event ring
  -> master matches the exact expected (worker_id, child PID) set
  -> master emits local WorkersBootConfirmed { count }
```

`ProcessStarted` is an identity-free local event owned by `runtime`.
Master, worker, and standalone bootstraps all emit it. Local consumers do not
need supervisor topology. Only a worker installs the one-shot translation to
`WorkerStarted`; neither master-local startup nor a remote acknowledgement
produces a worker-local `ProcessStarted`. `WorkerStarted` is a private protocol
type with a five-byte binary payload (one worker-id byte and a big-endian PID).

The wait action accepts out-of-order acknowledgements, ignores duplicates,
untracked ids and retired PIDs, and checks child exit before confirming the
set. Shutdown, stream loss/closure, early child exit, or deadline expiry cannot
emit `WorkersBootConfirmed`. `startup.timeout` configures the initial boot wait,
defaulting to 30 seconds independently of heartbeat, metrics, and reload policy.
The wait snapshots this duration when it starts, after initial child spawning;
a config reload does not extend or shorten an already active deadline. A short
timer checks OS child exit only; boot acknowledgements are not inferred by
polling any ring state.

The registry-installed handler remains active independently of monitoring. Replacement
workers also become `Running` only on their own matching `WorkerStarted` event.
The aggregate `WorkersBootConfirmed` is emitted once for initial boot, not
again for every replacement. The master preserves the shared event ring and
captures its inbound cursor at the current head before child spawn; retained
events from a previous master lifetime are not replayed into this
acknowledgement stream, while adopted workers remain on the same bus. This is
the `NO_IMPLICIT_TERMINATION: EVENT_SHM_SURVIVES_MASTER_REPLACEMENT` contract.

## Worker Drain Protocol

`lifecycle/shutdown.rs` owns the drain protocol between master and worker: three typed
Orbit events, each naming one exact worker.

```text
master: slot intent -> Drain { reason, after_exit }
  -> WorkerDrainRequested { operation, target: WorkerIdentity, reason }
  -> the worker compares every field of `target` with its own identity
  -> WorkerDrainAcknowledged { operation, worker }   (runtime shutdown initiated)
  -> the worker drains its runnables and finalizes its providers
  -> WorkerTeardownCompleted { operation?, worker, outcome }
  -> the master's exit observer proves the process gone; the slot reaps it
```

`WorkerIdentity` is `WorkerId` + `ProcessIdentity` (PID and kernel start stamp)
+ incarnation + the owning `MasterIdentity`. A worker acts only when all of it
is its own; a request that differs in any field names some other process and is
ignored with a debug line saying which field. A worker leaves once: the first
operation whose request it accepts owns the teardown, a repeat of that request
is acknowledged again, and a different operation's request is ignored. The
worker learns its identity from the v3 bootstrap through the
`WorkerIdentityProvider` the facade registers before providers boot; it never
takes it from anything a later record says about it.

On the master the acknowledgement moves the slot's phase to `DrainAcknowledged`
and the completion to `TeardownCompleted`, both still live phases: the runtime
has agreed to go, then has finished its managed teardown, and the process still
exists until the kernel says otherwise. A record is applied only when its
identity matches a slot's process exactly and its operation owns that slot (the
slot operation or the fleet-wide one); a completion without an operation is
believed only for a slot that was told to drain or during fleet shutdown.
Completion also closes the heartbeat observation. Nothing in the event
protocol reaps; OS process origin decides the fleet-shutdown exit barrier. A
spawned worker is this master's child and remains in that barrier; with
`shutdown.timeout: None` the master waits indefinitely for it, as
NO_IMPLICIT_TERMINATION requires. An
adopted worker is logically supervised but remains another parent's process at
the kernel boundary. It receives the same exact drain request, but under an
unbounded shutdown a non-exiting adopted worker is logged with PID and start
stamp, left alive, and does not pin the master. A configured finite timeout
keeps the existing wait and authorized escalation path for adopted workers.
An unowned identity is never signalled or used as an exit barrier. These are
origin decisions, not inferred timeouts; leaving an external-origin process
alive does not create implicit termination authority. `wait` remains the
authority for exit, and `record_exit` classifies it from the
intent.

Every managed drain — manual stop, targeted restart, rolling restart, rotation,
resize and heartbeat recovery — publishes a request instead of sending
`SIGTERM`. Direct POSIX signals are not the lifecycle transport, because an
embedded runtime may replace the process-wide handler: on 2026-09-13 a worker
never saw the `SIGTERM` its master sent. There is no signal fallback. A request
that a spawned worker never acknowledges leaves its slot draining and visible
in the snapshot and blocked-shutdown report. The adopted/unowned exception for
an unbounded fleet shutdown is the origin policy above. `shutdown.timeout:
Some(d)` still authorizes one `SIGKILL` after `d`; that is the operator's
policy, not a transport. Full shutdown is the same protocol: one request per
live identity under the fleet shutdown operation, which the operation table
numbers without owning anything — it outranks every operation in it, and those
end under it. The whole-fleet broadcast of the first protocol version is gone.

Two abnormal paths still send `SIGTERM` directly, because the control ring
cannot carry a request on them: startup rollback, when the children may not
have installed their listeners yet, and the bootstrap's fallback after the
master runtime has already stopped. Neither is the normal shutdown path.

Cancellation requests teardown; it does not confirm its completion. Worker
bootstrap publishes the completion once, after managed runnable drain (or
abort-and-join) and provider finalization, with the number of finalizers that
failed. It calls the synchronous Orbit publisher directly, not
`state.events().emit`, so process exit cannot cancel a pending forwarding task.
There is no master ACK wait or process-local `ProcessExit` event. Completion
does not prove unmanaged/detached tasks finished or that the process has exited;
providers remain responsible for joining their own work.

The master subscribes before child spawn, independently of the normal monitor.
Stopping that monitor for fleet shutdown does not stop these receivers. A slot
keeps its process in the table throughout a drain, so matching is by exact
identity, never by lifecycle state. `SIGKILL`, crashes, publication or stream
loss can prevent acknowledgement or completion delivery; none blocks reaping.
The receivers warn on lag rather than inventing missing records.

## Worker Reload Events

Reload uses the ordinary Orbit event stream rather than a private request/reply
transport:

```text
master publishes ServerReloading { generation }
  -> fleet workers run their local registry reload cycle
  -> each publishes WorkerReloaded { generation, worker_id }
  -> master waits for the exact expected worker-id set
```

The generation rejects stale or concurrent completion events. A worker starts
its Orbit cursor at the current ring head, so a worker booted after publication
does not replay an earlier reload event. `WorkerReloaded` means the local
registry reload cycle finished; provider errors remain in that worker's logs
and do not cross the Orbit event ring.

The event codecs are compact binary records, not JSON. `ServerReloading` carries
the 8-byte big-endian generation and a 1-byte target worker id; zero targets the
whole active worker set. `WorkerReloaded` carries that generation and the worker
id in 9 bytes.

Only workers currently marked `Running` enter the acknowledgement set. If the
configured reload range contains a missing, starting, draining, or dead worker,
the supervisor warns with the unavailable ids and reloads the active subset
without waiting for an impossible acknowledgement. That is a successful
`ReloadOutcome::Partial`, not a coordinator error. Worker-reported failures and
Acknowledgement timeouts are partial outcomes; event-stream failure is an actual
reload coordination error.

`events/reload.rs` owns the codecs and request correlation.
The registry installs `WorkerReloadObserver` before any operation; each request
holds only its generation-scoped acknowledgement evidence. There is no listener
registration inside an operation or Runnable. Worker callbacks validate target
and submit to `WorkerReloadRunner`; the runner serializes reload cycles while
the dispatcher keeps progressing. Targeted runner stop drains accepted work;
global process shutdown may interrupt reload and emits no false acknowledgement.
Native Orbit notification drives the common event
registry on Linux and FreeBSD, so reload adds no supervisor polling interval or
private SHM kinds.

## Worker Bootstrap

Event-listener and bridge registration errors map to `Error::Bootstrap`, which
retains the original registry error. Runtime service/worker failures remain
separate; being an event error alone does not make a runtime failure a boot error.

The master re-executes the current binary with a private validated envelope:

```text
CLAVIRON_INTERNAL_WORKER_BOOTSTRAP={
  "version":6,
  "worker_id":2,
  "fleet_node_id":2,
  "worker_count":4,
  "fleet_capacity":256,
  "incarnation":3,
  "master":{"pid":1234,"started_at":1757780000000000,"generation":5},
  "instance_name":"shop-blue",
  "fleet_name":"clv",
  "environment":"production",
  "config_dir":"/srv/shop/config"
}
```

`master` is the spawning master's `lifecycle::MasterIdentity`: its exact
process (PID plus the raw kernel start stamp — platform-specific, never a wall
clock) and its generation. `incarnation` is which process of its slot this
worker is, in the master's count, so the worker can later name itself exactly.

The worker validates version, identity, non-zero one-based worker id, immutable
fleet capacity, desired worker count, a non-zero incarnation and generation, and
its parent: the parent PID must equal the master PID *and* the parent's kernel
start stamp must equal the master's, so an envelope under a process that merely
inherited the master's PID is refused. A malformed envelope is a startup error;
it never falls back to primary or standalone mode.

The master establishes its identity once, in `MasterIdentityProvider::boot`, by
publishing one frame carrying its process identity on master lane `0` of the
master generation ring (Orbit kind `238`); the generation is that frame's
counter plus one. The ring is never reset by the master, so the count survives
restarts for as long as the SHM segment does, and `last_master_identity` can
read the previous master back. A cleared segment starts the count again, which
is why the generation qualifies the process identity and never stands alone.
Kinds `238` (master generation), `221` (heartbeat), and `239` (typed presence)
are registered as managed `Reuse` / `Preserve` resources. The same exact
presence evidence is exposed to SHM lifecycle owners; when exact presence is
unavailable, one configured heartbeat interval of lane motion distinguishes a
live legacy publisher from retained bytes.

## Boot Completion And Liveness

Startup acknowledgement means that the worker reached the end of provider
boot and submitted its runnable tasks. It does not prove that every runnable,
listener, PHP executor, or application dependency is currently ready.

Only `WorkerStarted` completes this boot contract. A heartbeat cannot replace
it. The first valid acknowledgement initializes liveness tracking, but does
not count as an observed heartbeat. Duplicate acknowledgements do not refresh it.

During normal monitoring, `heartbeat.timeout` determines when the last
accepted heartbeat is stale. The supervisor rechecks after a short delay to
filter scheduling blips, then drains the exact PID-correlated worker incarnation.
It sends SIGTERM and applies the same `shutdown.timeout` policy as every other
worker drain. The normal process-health pass then restarts the exited worker
with configured backoff.

## Server Management

`ServerManager` is an owned domain handle for whole process-family lifecycle
commands. It preserves the existing Unix control boundary by sending `SIGHUP`
for reload and `SIGTERM` for graceful shutdown to `AppState::control_pid()`.
In worker mode that PID is the master; in master and standalone modes it is
the current process. The main-process signal listeners remain the sole entry
into `registry.reload_all()` and shutdown coordination, so gRPC, the shell,
and the OS service manager cannot drift into separate implementations.

Reload creates a temporary `ServerReloaded` subscription before signalling, so
the main process cannot complete the cycle before the caller starts observing
it. After `registry.reload_all()` returns, the main signal listener publishes
the terminal event with its reload revision. Its boot-time Orbit bridge returns
that event to a request handled by any worker; standalone delivery remains
process-local. Dropping or cancelling the request drops its receiver without a
permanent callback or task.

`ServerReloading` and `WorkerReloaded` coordinate the worker phase inside that
cycle. Shutdown remains asynchronous after signal acceptance and uses the
ordinary fleet drain plus the operator's explicit `shutdown.timeout` policy.
An authenticated, explicit whole-server shutdown is not implicit termination.
`ServerManager` owns no provider table, worker table, or completion snapshot.

## Worker Management

### GRACEFUL_OWNERSHIP: `NO_IMPLICIT_TERMINATION` — process-level application

The canonical rule is
[GRACEFUL_OWNERSHIP: NO_IMPLICIT_TERMINATION](../../docs/design/GRACEFUL_OWNERSHIP.md).
What follows describes this crate's process-level application of that contract.

The worker that accepted work owns its graceful lifetime until it exits. The
supervisor may send SIGTERM to begin that drain, but it gains authority to send
SIGKILL only from an operator-supplied `shutdown.timeout: Some(duration)` after
that duration has elapsed.

This rule applies uniformly to initial-boot rollback, heartbeat recovery,
rotation, manual stop, scale-down, and full shutdown. Dropping the supervisor
does not signal children: process teardown is an explicit asynchronous lifecycle
operation, while the OS service manager remains the final external authority.
Choosing an unbounded drain can therefore block startup rollback, recovery,
reload, or shutdown indefinitely; that availability risk belongs to the
operator who chose it.

### `FLEET_CONTINUITY`

> **Workers may come and go. The fleet must live on.**

Claviron's operational promise is **always up**: routine management may reload,
stop, start, rotate, or eventually replace every worker without ending the
fleet's identity or surrendering its control plane. Worker lifetime is never
fleet lifetime. The master-side supervisor preserves configured slots and their
management state while worker processes are treated as replaceable capacity.

This promise describes the managed fleet lifecycle. Recovery from a master
process crash or host reboot remains the responsibility of the external init
system until the control plane itself becomes replicated.

`restart-workers` is the executable form of this invariant. It snapshots the
stable running slots, moves the worker that originated the command to the end
of the order, then drains, reaps, spawns, and boot-confirms one replacement at
a time. The next worker is never touched until the exact replacement PID has
published `WorkerStarted` and the authoritative process table marks it
`Running`. At least two workers must be running when the operation begins;
stopped slots retain their operator-selected state. A drain or replacement
failure stops the rolling operation without disturbing the remaining workers.

The gRPC acknowledgement reports command acceptance, not completion. Each
slot carries a monotonically increasing `incarnation` in the management
snapshot so clients can prove that every targeted process was replaced instead
of mistaking the unchanged pre-operation `Running` state for success.

`WorkerManager` is an owned domain handle, not a provider or a second
supervisor. It validates currently configured worker ids and bounded resize
plans, publishes typed management commands through Orbit, and reads the latest
management snapshot. `list()` returns the complete configured worker table;
`status(id)` narrows that same authoritative observation to one slot without
introducing a second state source. Only the master installs the command
listener. All mutations continue through
`WorkerControl`, whose shared fleet process table remains the sole authority for child
handles, lifecycle, crash-recovery attempts, start clocks, and completed reload
revisions.

Start and stop change the desired state of an existing worker slot; they never
change `WorkerCount`, add an id, or remove an id. A manually stopped process is
retained as `Stopped` in the process table and is excluded from capacity and
automatic restart. Starting that slot replaces its reaped child handle with a
new incarnation.

Every mutation is a `lifecycle::LifecycleOperation` with a scope — one slot, or
the fleet — recorded in the supervisor's `OperationTable` by check-and-set
before the operation awaits anything. A slot has one owner at a time and a
fleet operation (`restart-workers`, `reload-workers`, resize, the SIGHUP config
reload) owns every slot while it runs. The `WorkerCommand` listener only
submits: it takes ownership, hands the future to `WorkerOperationRunner`, and returns.
A command for an owned target is answered at once — the same command again
joins the running operation, a different one is refused with the operation that
owns the target — and never queues. The monitor submits rotation, heartbeat
recovery and crash recovery the same way, so a drain that `shutdown.timeout:
None` lets run indefinitely holds one slot, not the supervisor. Refusals are
logged with the owning operation until the snapshot carries them (C7). There is
no management lock.

`restart-worker <id>` composes those same slot operations under one operation. The
master waits until the old child has drained and been reaped before spawning a
new incarnation, but does not hold the management command open for application
readiness. Clients prove completion by observing that slot return to `Running`
with an `incarnation` greater than the pre-command snapshot. Restarting the last
running worker is rejected by `FLEET_CONTINUITY`.

`add-worker` and `remove-worker` are fleet-size operations owned by
`resize.rs`, not aliases for start and stop. Growth assigns the next contiguous
worker id and is rejected at `fleet_capacity - 1`; shrink retires only the
highest configured id and refuses to remove the final worker. Physical Orbit
capacity never changes. A worker's private-bootstrap count describes the
topology at that process's birth; it is not a reason to replace a retained
process during scale-down. The master snapshot is the live management topology.
Stopped configured slots remain stopped. A removed high slot remains as an
internal stopped tombstone so adding the same id later preserves its monotonic
`incarnation`, but snapshots expose only ids within the current fleet size.

During growth only the added member is spawned and boot-confirmed; a failed
boot rolls back only that added slot. During shrink only the outgoing highest
member is drained and reaped. Both directions preserve every retained worker's
PID, incarnation, ownership and operator-selected stopped state, whether
spawned or adopted. The mutable master-side `AppState::worker_count()` is
committed only after the added process is boot-confirmed or the removed process
exits. The supervisor process table remains the authority for actual child state.

Non-restarting resize does not refresh worker-local bootstrap identity.
Targeted `WorkerManager` commands validate against the current snapshot, not
the relaying worker's birth count. Task inventory/acquisition budgets, upstream
probe handoff and SAPI statistics resolve current `FleetMembership` instead.
Unavailable membership defers new task acquisitions (not lease renewal) and
does not manufacture successful fleet observations. PHP invalidation opens
against immutable capacity even in a one-worker fleet and consumes all peer
lanes, including final invalidations from removed workers. Existing execution
environment values remain birth-context metadata, not live topology.

Until a master-owned management listener exists, at least one confirmed
running worker must remain available to host the gRPC control plane.
`WorkerManager` rejects the ordinary last-worker stop from its fresh snapshot,
and the master action repeats the check under the management lock against the
authoritative process table. The second check is the safety invariant when two
clients race on an older snapshot.

Reload remains an in-process registry reload, not process rotation. The same
reload event carries either target `0` for the existing fleet broadcast or one
worker id for management. A worker ignores targeted reloads for other ids. The
master increments `reload_revision` only after the existing correlated
`WorkerReloaded` acknowledgement arrives; clients can therefore poll for a
revision increase without adding a gRPC acknowledgement protocol.
`reload-workers` takes one master-side snapshot of the currently running
slots, broadcasts one generation to that set, and leaves stopped slots stopped.
It is distinct from `restart-workers`, which replaces process incarnations and
waits for each replacement boot barrier before advancing.

`WorkerSnapshotPublisher` is a separate master runnable. It derives each
snapshot directly from the process table and publishes an atomic batch on
Orbit kind `197`, master lane `0`. The snapshot is a disposable read model,
never an input to supervision. Its wire header carries the configured freshness
window so worker-side readers need no duplicate supervisor configuration; the
master's `BuildIdentity` — the short git revision and dirty flag that
`build.rs` recorded — so a binary that is behind its source tree is visible
from any client; the master's own `MasterIdentity`; and the `FleetMode`. The
header also carries `MasterRuntimeSnapshot`: the runtime start as UTC Unix
milliseconds, monotonic runtime uptime at capture, and the master reload
revision. AppState records start once; reload and publisher restart do not
reset it. The observation belongs to the exact master identity in this same
atomic batch, including after master replacement/adoption. Kernel start stamps
remain opaque identity qualifiers, never dates. The master states the same
build identity, with the crate version, in its `master
identity established` boot line.

Kind `197` is likewise a managed `Reuse` / `Preserve` resource. Complete batch
publication and the wire freshness window reject stale prior-run state without
mutating a mapping that an adopted process may still hold.

Each row carries the two facts a slot's label is derived from — the observed
`WorkerObservedPhase` and the desired `WorkerIntent`, each with how long it has
stood — and not the label: `WorkerStatus::lifecycle()` is a method, computed by
the same `from_observation` the master uses, so no row can carry a label that
disagrees with its facts. With them travel the exact `process`
(`ProcessIdentity`: PID and kernel start stamp) — the handle this master owns
or, for a slot it is claiming or has refused, the worker-written presence that
occupies the lane, so `Unowned` with a process reads exactly as "boot is blocked
by this PID" — its `incarnation`, `WorkerOwnership`, the `WorkerOperation` acting
on the slot with its id, kind, `OperationOrigin` and age, the age of the last
accepted heartbeat (`None` before the first beat: the grace anchor is not a
heartbeat), and for a `ReconciliationBlocked` slot the `WorkerBlockReason` boot
refused it for.

The operation on a slot is chosen by authority: the fleet shutdown outranks a
fleet-wide operation, which outranks the slot's own. Once shutdown has latched,
a slot whose stop or restart is still running reports the shutdown, which is
also the operation the master has since named in its drain request to that
worker. `OperationOrigin` says who asked — a management command with the
submitter's `CommandKey` and the worker whose control plane relayed it, the
supervisor's own monitor, or a process signal — and is distinct from the kind:
a rolling restart is one whether the GUI, `claviron-client` or the restart signal asked.

The slot's two clocks live on the row in the table, private to it and moved
only by `set_phase`/`set_intent`. `set_phase` restarts the clock only when the
phase changes as an outside reader sees it — same published state and same
block reason keep it — so a blocked slot whose presence evidence refreshes on
every worker heartbeat still reports how long it has been blocked. An age is
therefore never computed from a clock that could have drifted from its fact.
This is what lets a GUI or CLI explain a blocked fleet without the master's
log — the observation the 2026-09-13 incident record could not make.

The row is 171 bytes on a 192-byte payload, on kind `197` since 2026-10-07.
It replaces the 147-byte row / 160-byte payload of kind `240` (2026-09-14).
Growing that existing segment would reject a new binary while earlier peers
still held it, so the incompatible shapes coexist on distinct kinds. Kind
`240` is never resized or automatically removed. Operator cleanup requires a
stopped fleet and the canonical [SHM kind index](../orbit/ARCHITECTURE.md#shm-kind-id-index).
The first snapshot layout used `227`, which is now a text-cell table; that
historical number is not permission to clear a current resource.

`WorkerServiceMembership` is a non-runnable capability bound to Orbit's
`FleetMembership` port in master and worker processes. It reads that same
fresh complete worker snapshot; no additional worker table, polling task, or
metric-based membership is introduced. Master is always expected to report its
own services, but this never registers a worker-only service in master.
`Stopped`, `Exited`, and `Crashed` slots without a PID are excluded from service
reporting membership. Starting, running, reloading, and stopping slots remain
expected, so temporarily missing service reports do not masquerade as stopped
workers. Invalid/stale worker observations fail closed. The current supervisor
adapter owns the worker-id-to-node-id mapping; the Orbit collector consumes only
explicit fleet member identities. A stopped worker's lingering service rows are
filtered out immediately when the authoritative snapshot reflects its stop.

## Process Rotation

### Runnable placement

Supervisor runnables expose `Provider::as_scoped` through the shared placement
contract. `WorkerExitObserver`, `WorkerOperationRunner`, `WorkerReconciler`,
`WorkerMonitor`, and `WorkerSnapshotPublisher` use `Scope::Master`;
`HeartbeatProvider` and `WorkerReloadRunner` use `Scope::Worker`.
`SupervisorMetricsPublisher` uses `Scope::Any` because both master and workers
publish their own process measurements. Dispatch remains the default `Each`.

Master and workers register the same supervisor composition and configuration
owner. Standalone keeps its existing no-supervisor composition. Scope selects
runnable execution before the runtime creates local service entries. The
master-only capabilities (`MasterIdentityProvider`, `WorkerControl`,
`HeartbeatObserver`, `WorkerConfigurationReload`, `SupervisorShmActivity`) also
expose `Scoped`; effectful boot/reload/finalize hooks retain explicit role
guards because Continuo currently filters runnable execution, not those hooks.
Worker commands may be bridged everywhere but only the master handles them.
Metrics registration is shared; its source is the current process's role.
Configuration snapshots are still process-local, not distributed over Orbit.

Heartbeat publication is the first independently configured consumer: it declares
the shared supervisor source but projects only `heartbeat.interval` into its
Congif snapshot. Its runnable retains the applied interval as runtime state;
reload wakes the ticker through `Notify`, rather than polling the full policy
on every tick. Other consumers still use the shared policy during this migration.

Rotation uses only data the master owns or can read directly:

- `rotation.rss_threshold`: read from the worker PID through
  `claviron_sys::rss::read_rss(Some(pid))`;
- `rotation.max_age`: measured from the `Instant` stored when the master
  spawned that worker process.

RSS is the real process RSS. The supervisor does not subtract HTTP cache or
any other domain allocation from it.

When a policy triggers, the supervisor removes the old process from capacity,
publishes a targeted drain request, and fully reaps it before starting a replacement with the same
worker id. This deliberately leaves the REUSEPORT pool one worker smaller
during the drain: two process incarnations must never publish concurrently to
the same node-owned Orbit lane. `shutdown.timeout` controls SIGKILL escalation
for rotation as well as every other worker drain. Without that explicit value,
rotation waits indefinitely. The worker's own runnable services are responsible
for draining in-flight work during graceful shutdown.

Request-count rotation and HTTP request/connection idle gates do not belong in
this crate. A future owner-specific policy may request rotation through an
explicit control contract, but it must not reintroduce HTTP snapshots into the
supervisor.

## Reload And Shutdown

Which signal does what is the binary's choice (`ServerSignals` from
`claviron-sys`, set on its `Server`); the defaults follow the common convention:
HUP reloads, USR1 reopens the log files, USR2 restarts the workers. Only `hup`,
`usr1` and `usr2` may be chosen, one role each.

Master alone installs a listener on the restart signal for rolling worker
restart. It calls `WorkerManager::restart_workers()` just like the CLI, logging
command submission at INFO or validation/submission failure at ERROR. It does
not emit reload events, implement a second restart loop or report submission as
completion. `management/actions/restart.rs` retains ordered
drain/reap/spawn/boot-confirmation and its terminal logs. A standalone process
has no workers and keeps the restart signal's default disposition.

The master and a standalone process listen on the reopen signal and emit
`ReopenLogs`; the fleet event bridge (claviron-orbit) carries it to every
worker, so a worker never needs to know which signal was chosen. Workers catch
every operator signal and ignore it — `systemctl kill` signals the whole unit,
and a user signal with no listener would end a worker. Registration
installs the OS handler synchronously before spawning its listener task;
requests before Fleet boot fail through the ordinary Manager validation.
The listener exits on shutdown and its task is aborted/joined by `SignalHandles`
after shutdown coordination. No automatic process kill or extra timeout is added.

SIGHUP causes the master to reload its providers, publish `ServerReloading`,
and wait for correlated terminal `WorkerReloaded` events from the applicable
workers. Workers do not install a SIGHUP reload listener; the bridged event
callback is their reload entry. When the complete main-process reload cycle
returns, the signal listener publishes `ServerReloaded { revision }`.

An operator reload also opens a new worker recovery epoch. After the replacement
`supervisor.yaml` snapshot loads successfully, the master clears the retained
restart-attempt count and backoff timestamp before checking for dead workers.
This lets a corrected configuration recover a worker that exhausted the prior
epoch's `recovery.max_attempts`. A rejected supervisor configuration leaves the existing
restart state and running snapshot untouched.

Master shutdown latches first and then has three phases:

0. Latch `FleetMode::ShuttingDown` — an atomic in the supervisor, committed
   before any event is emitted, any Orbit record is published or any lock is
   taken — and mark every live slot as draining for shutdown. From this point
   no operation is accepted (`SubmitResult::Rejected(ShuttingDown)`, and the
   monitor submits nothing), and an exit caused by the shutdown is a planned
   exit whether or not the control event has reached the master's own view
   yet. This is `SHUTDOWN_HAS_PRECEDENCE`: on 2026-09-13 the latch sat behind a
   lock a rolling restart held, and the workers that obeyed the control event
   were counted as crashes and queued for restart.
1. Emit `WorkersDraining`, then one targeted drain request per live worker
   identity under the fleet shutdown operation.
2. Wait for graceful exit, escalating after `shutdown.timeout` when configured.
3. Emit `WorkersExited` and initiate process-wide provider shutdown.

A slot that was already draining when the latch came — for a rotation, a
rolling restart, a resize or heartbeat recovery — keeps its drain: the same
reason, the same identity, the same waiter. Only its `ExitDisposition` changes,
to `FinishFleetShutdown`, so the process that was going to replace it is never
spawned; `spawn_worker` refuses after the latch regardless of which operation
asks. `WorkersExited` is emitted when `wait_workers` returns, and
`wait_workers` re-collects live identities after every wait. Spawned workers
must be gone; with an unbounded timeout the established origin policy may
leave adopted workers alive after a best-effort drain request. `WorkersExited`
marks that policy completion, not proof that every adopted writer has exited.

While an unbounded shutdown waits, the master says every thirty seconds who it
is waiting for: one `BlockedWorker` line per live identity with the worker id,
PID and kernel start stamp, phase, intent, the operation that owns the slot and
how long the master has owned the process, plus the fleet-wide operation if
one is running. The report is diagnostic; no elapsed time in it creates
termination authority.

`WorkersBootConfirmed` is emitted after every initially spawned worker has
passed the Orbit event boot contract. It confirms provider boot and runnable
submission, not readiness of every individual runnable.

## Configuration

`config/supervisor.yaml` owns the operational policy:

```yaml
heartbeat: { interval: 1s, timeout: 10s }
monitor: { interval: 10s }
snapshot: { publish_interval: 1s, max_age: 5s }
metrics: { publish_interval: 10s, max_age: 30s }
startup: { timeout: 30s }
reload: { timeout: 5s }
recovery:
  max_attempts: 10
  window: 10m
  backoff: { initial: 2s, multiplier: 2, max: 60s }
# Rotation is disabled by default; these are opt-in example triggers.
rotation: { rss_threshold: 512 MiB, max_age: 12h }
shutdown: { timeout: null }
```

The timing fields have distinct owners and meanings:

| Field | Consumer | Meaning |
|---|---|---|
| `heartbeat.interval` | Worker `HeartbeatProvider` | How often the worker publishes its vital heartbeat. |
| `heartbeat.timeout` | Master liveness policy | How long an accepted heartbeat may be absent before the worker is treated as stale and the hung-worker policy applies. |
| `monitor.interval` | Master worker monitor | How often process health and control policies are evaluated; not a publisher interval or sample-age limit. |
| `snapshot.publish_interval` | `WorkerSnapshotPublisher` | How often the master publishes its complete worker process view. |
| `snapshot.max_age` | `WorkerManager` reader | Maximum accepted age encoded by the master into the status snapshot. |
| `metrics.publish_interval` | `SupervisorMetricsPublisher` | How often a process captures and publishes observational metrics. |
| `metrics.max_age` | `SupervisorMetricsFamily` collector | Maximum accepted age of a metric sample; does not trigger any worker action. |
| `startup.timeout` | Master initial boot coordinator | Maximum wait for the initial child set's boot acknowledgements, measured after child spawning; defaults to 30 seconds. |
| `reload.timeout` | Master reload coordinator | Maximum wait for correlated worker reload completion events. |
| `shutdown.timeout` | All supervisor-owned worker drains | Maximum graceful wait before forced-kill escalation; `None` means no deadline and no force kill. |

The metrics config group is accepted with or without the `metrics` feature;
only its runtime consumers are feature-gated. It has independent defaults. Do not derive them from
heartbeat fields, use heartbeat values as fallbacks, or infer liveness from
the metrics freshness window. Tuning metrics must leave the vital heartbeat
cadence and failure threshold unchanged, including across config reloads.

The file is optional for supervised boot. `SupervisorPolicy`, heartbeat and
snapshot-publication projections declare their absent-source values through
Congif's `Configurable::absent_source`, using `SupervisorConfig::default()`.
With `config-watch` enabled, the missing file remains a watched source: creating
it applies its policy, and deleting it restores the defaults. Invalid existing
files still fail preparation and retain the last valid snapshot on reload.
Every group has a typed struct and
explicit defaults; all groups reject unknown keys, including obsolete flat
keys. Serialization emits only the nested form. There is no second legacy
runtime config. Recovery defaults remain ten attempts in a ten-minute window,
with 2s exponential backoff capped at 60s; rotation triggers default to absent.

`SupervisorPolicy` is the Congif configuration owner for each process. Continuo's
configure phase prepares and validates it before provider boot. Reload uses the
same reader in Continuo's central batch preparation and preserves the last valid
snapshot on rejection. Provider reload hooks consume that prepared generation;
they never call `initialize_owners` or reopen the file. The shared policy
handle references Congif storage, not another active config copy. Heartbeat,
monitoring, snapshots and metrics read their own section from that snapshot;
they do not independently parse the file or retain divergent policies.
Failed validation leaves the old snapshot active and prevents the effectful
`WorkerConfigurationReload` hook from resetting budgets or reloading workers.

File-backed policy and heartbeat use `ConfigFile { format: Yaml, path }` and
receive prepared typed values in `configure`; `ConfigObserver` opts both into
central source watching. Heartbeat deserializes the shared schema into a small
interval projection, so unrelated policy changes do not reset its ticker.
Both readers bind the application's frozen configuration environment.

`WorkerSnapshotPublisher` follows the same contract with an independent typed
projection containing `snapshot.publish_interval` and `snapshot.max_age` only.
It keeps those two applied publication values, not the shared supervisor
configuration. Source changes and explicit reload update the publication state
and wake its existing loop through `Notify`; it does not re-read shared policy
on each publication. This changes neither process-table ownership nor the
ring's lifecycle. A restart retains its configured publication values.

Runner's existing `SupervisorConfigSource` remains a separate code-supplied
policy path, not a fabricated file for Congif. Registration validates its
initial policy before consumers boot; explicit reload validates and atomically
replaces the supplied policy. Only that path retains a supplied policy slot;
file-backed policy continues to use Congif storage. It installs no file watch
and adds no provider boot hook.

Regression checks for this boundary:

- With metrics disabled, worker startup and heartbeat-based hung detection
  still function.
- Boot acknowledgements succeed without a heartbeat or pre-existing liveness entry;
  a heartbeat never completes boot by itself.
- Startup rejects wrong PIDs, duplicates as extra workers, early exit, stream
  loss and shutdown; missing acknowledgements hit the independent boot deadline.
- Both shorter and longer `startup.timeout` values control that deadline without
  changing heartbeat or reload policy; omitted values preserve the 30-second default.
- Omitted or null `shutdown.timeout` values preserve an indefinite graceful
  worker drain; only an operator-supplied duration authorizes SIGKILL escalation.
- Remote boot acknowledgements never emit a local `ProcessStarted` marker.
- Shutdown completion is published directly to Orbit.
- Cancellation alone does not publish completion; final notification requires
  the explicit post-drain/finalization call.
- Shutdown notifications ignore wrong worker ids, retired PIDs, and duplicates;
  they do not reap a still-live child or alter its process status.
- The shutdown receiver remains active after monitor stop, and missing events
  do not prevent OS reaping. Direct publication needs no executor turn or ACK.
- Missing, stale, or malformed process metrics do not change worker status or
  trigger restart/rotation; fresh metrics do not excuse a stale heartbeat.
- Changing only metrics timing leaves heartbeat timing unchanged, and changing
  only heartbeat timing leaves metrics publication and freshness unchanged.
- Rotation continues to use the worker PID and the master's start clock even
  when no metric snapshot is available.

## Lifecycle Ordering

- `FleetProvider` boots before `FleetEventRegistry` and the master's identity
  preparation barrier. Heartbeat publishing starts only after provider boot.
- `FleetEventRegistry` preserves the shared event ring across master
  replacement and captures an inbound cursor at the current head during boot.
  Its managed runnable starts polling after provider boot; typed outbound
  bridges use the runtime's local event dispatcher, not detached per-type
  forwarding tasks.
- `register()` installs the shared supervised composition first, then the startup, reload,
  and shutdown event bridges/callbacks. Registration is synchronous and requires
  no Tokio executor. The callbacks begin only when the local dispatcher runs.
- Only workers translate local `ProcessStarted` (once) and handle reload events.
  The master's shutdown completion callback outlives the supervisor monitor.
- `spawn_main_signal_handlers` and `spawn_worker_signal_handlers` start only
  OS signal tasks; they neither register event types nor run event receive loops.
  These signal tasks still start before provider boot to keep startup interruptible.
- Which signal does what is the binary's default, or `supervisor.yaml`'s
  `signals` (`reload`, `reopen_logs`, `restart`: `hup`, `usr1` or `usr2`,
  one each) when written. The facade reads that section with
  `configured_signals` before the listeners start — from a registered
  `SupervisorConfigSource` if there is one — so a change takes effect on a
  restart, not a reload.
- The master's preparation barrier attaches `HeartbeatObserver` to the Fleet
  before topology recovery and worker spawning;
  workers have no supervisor transport client to connect during boot.
- `HeartbeatProvider` is a registry-managed worker runnable.
- Capability-only providers and publishers have no boot priority when they
  perform no boot work. Their dependency order remains explicit; publisher
  execution priority belongs to the run phase.
- `WorkerReconciler` follows `WorkerOperationRunner`; all master provider boot
  hooks finish before any Runnable is submitted. Provider ordering is submission
  order, not proof that another Runnable has reached readiness. Event listeners
  are already installed, and initial convergence has its own explicit completion.
- The supervisor's `WorkerStarted` subscription precedes every initial spawn.
- Server bootstrap emits local `ProcessStarted` after runnable submission and
  before `Runtime::wait_until_shutdown`, only on successful, uncancelled boot.
- Worker bootstrap directly publishes `WorkerShutdownCompleted` after managed
  drain and provider finalization, before returning its runtime/drain result.

## Invariants

- Worker ids are one-based; node zero belongs to master/control identity.
- Worker bootstrap preserves the master's explicit fleet name; instance name
  and fleet name are separate identity domains.
- Standalone never registers supervisor providers.
- The supervisor has no network transport or socket lifecycle.
- Worker boot, reload, and shutdown completion use distinct typed Orbit events.
- Heartbeat observations own no child handles and cannot change process status.
- The heartbeat reader has no independent runnable or policy clock.
- Process-local `ProcessStarted` has no identity and is never bridged directly.
- Boot completion, heartbeat liveness, and reload acknowledgements are separate
  contracts; no one substitutes for another.
- Supervisor decisions never depend on HTTP or Prometheus availability.
- Heartbeat is vital control data; metrics are optional observational data.
- Heartbeat and metrics never share timing policy or substitute for each other.
- Domain counters never enter a supervisor record.
- Only supervisor process metrics may expose worker identity.
- The master reads process RSS by PID and owns the process start clock.
- A worker's PID is read from the `ProcessIdentity` captured at spawn, never
  from the child handle, and no slot field carries two meanings: the spawn
  clock measures rotation age and nothing else, and an unclaimed slot's lane
  watch has its own clock.
- Desired state and observed phase are separate slot fields; whether an exit
  was expected is decided from the intent that stood when the process was
  reaped, never from a status label, and the operator-facing lifecycle is
  derived from the pair rather than stored.
- Old heartbeat records cannot satisfy a new worker incarnation.
- Rotation fully reaps the old process before reusing its worker id and Orbit
  node lane.
- `NO_IMPLICIT_TERMINATION`: `shutdown.timeout: None` is never converted into
  a deadline, and supervisor drop never sends a process signal.
- `SHUTDOWN_HAS_PRECEDENCE`: the fleet shutdown latch is committed atomically
  before any await, event publication or lock on the shutdown path; after it
  nothing starts and no expected exit is a crash.
- `FLEET_CONTINUITY`: rolling management never drains the final running worker,
  and never advances to the next replacement before the previous one is
  boot-confirmed as `Running`.
- Multi-writer heartbeat, event, and process-metric transports use node-owned
  lanes; their capacities are per node and they do not claim a total order
  across nodes.
- Desired worker count comes from master `AppState`; running count is derived
  from the supervisor-owned process table. Neither is inferred from Orbit
  capacity.
- If all workers exhaust restart policy, the supervisor fails its runnable so
  the outer service manager can restart the process tree.
