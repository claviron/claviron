# Claviron Runtime Architecture

`runtime` is the Claviron-facing runtime facade.

The generic runtime kernel lives in `continuo`:

- `registry`: `Registry`, `Provider`, `Reloadable`, `Runnable`, `Finalizable`, lifecycle outcomes and errors, priorities, ordering.
- `events`: `ProcessEventBus`, `Event`, `ShutdownInitiated`.
- `runtime`: `Runtime<S>`, the runnable task manager.
- `gate`: `Gate`, `Permit`, and `GateDrainOutcome`, the listener capacity and graceful-drain primitive.
- `guard`: generic guard helpers.

This crate re-exports those primitives so workspace crates keep importing
through `claviron_runtime`, while the implementation and maintenance boundary
stays in `continuo`.

## Local Responsibilities

The opt-in `metrics` module owns `MetricsFamily`, `MetricsProvider` and
registration, formerly a separate package. It uses `AppState` directly and
introduces no HTTP, application, supervisor or Orbit dependency. Concrete
stores, snapshots, codecs, collectors and publication remain in their owning
subsystems; [metric composition](docs/metrics/ARCHITECTURE.md) only renders
registered families. The facade preserves `claviron::metrics`.

`Register<S>` is re-exported from Continuo for application-owned registration
before provider boot. It registers provider instances without resolving their
dependencies. Tuple implementations preserve declaration order and stop on the
first error without rolling back earlier registrations. Configuration is a
separate contract owned by Congif; runtime keeps no local `Configurable` copy.

The local code is intentionally Claviron-specific:

`RunMode` converts into Continuo's transport-neutral fleet `Scope`. Standalone
maps to `Any` because it combines master and worker roles. The optional
`Provider::as_scoped` hook exposes scope metadata. AppState supplies its process
role through `SharedState::process_scope`; Continuo filters local runnable
management entries at submission without changing provider boot/configuration.

Continuo also owns the transport-neutral `DispatchPolicy` data contract.
`Dispatched::dispatch_policy` supplies an optional service-owned policy, captured
in `DispatchRequest` for a runnable generation. Lease acquisition and renewal
remain in the Orbit adapter; no Orbit or SHM dependency enters this crate or
Continuo. `Each` needs no policy, and TaskService retains its own controller.

- `state.rs`: `AppState`, identity-only `RunMode`, process-family-local
  Congif `ConfigReader` ownership (also exposed through `HasConfigReader`),
  `WorkerId`, fleet-wide `FleetNodeId`, mutable
  supervisor-desired `WorkerCount`, immutable `FleetCapacity`, config directory,
  logical process-instance identity, its independently selected Orbit fleet name,
  hostname, shutdown token, registry handle, process event bus, reload counter,
  and env snapshot. A worker receives the current desired count
  and the shared fleet capacity through the supervisor bootstrap; neither is
  encoded into its process role. Hosting database identity is owned by
  `HostingService`, not generic runtime state.

`AppState::name()` is the embedding application's logical process-family name.
`ServerName` validates explicit server options and CLI input: one to twenty
Unicode characters, without whitespace or control characters. It has no
implicit default and is distinct from the shorter Orbit namespace. Literal
`From` conversions reject invalid programmer input by panicking; external
input uses the fallible constructor or `FromStr`.
`FleetName` is re-exported from Orbitive rather than duplicated here; it is the
same-host namespace primitive, not a Claviron process identity.
It may identify an application, systemd unit, or database placement and may be
the same on different physical servers. `AppState::fleet_name()` is instead the
same-host Orbit sharing domain used in `/orbit-{fleet}-{kind}-{uid}`. It defaults
to `clv`, is independently configurable, and is copied unchanged through the
master-to-worker bootstrap. Different process families may intentionally join
one fleet; independently named fleets remain isolated. Hostname, PID, binary
name, and application instance name do not implicitly alter the fleet name.

`FleetCapacity` is allocation geometry, not membership evidence. It fixes the
number of physical Orbit node lanes at boot and never changes during a process
fleet generation. Every process family joining that fleet must agree on it.
`WorkerCount` is the supervisor's mutable desired state.
Running worker count is derived from the supervisor process table; it is not a
third mutable count in `AppState`. A count transition reconciles only the added
or removed slot, without restarting retained workers. Each retained worker's
bootstrap count describes its birth, not live fleet membership; consumers of
current membership use the supervisor-backed membership capability.

`FleetNodeId` and `WorkerId` are intentionally different domains. The current
single-family supervisor assigns master node zero and initially maps a worker's
node id to its local worker id, but consumers must not depend on that equality.
A future fleet directory may allocate lanes across independently launched
process families without changing `RunMode` or local worker identity.
- `env.rs`: frozen process environment snapshot and process env scrubbing helpers.
- `events.rs`: AppState's local callback registration and managed dispatcher,
  built on Continuo's `ProcessEventBus`. Orbit transport and remote listener
  registration remain owned by `orbit`. This module also owns the
  Claviron server bootstrap's local `ProcessStarted` milestone, emitted after
  provider boot and runnable submission in every run mode. It
  carries no worker id or PID, and does not imply individual service readiness.
  It is not an Orbit codec. Cross-process adapters use distinct envelope types
  and must never turn a remote process acknowledgement into this local marker.

Internal `CLAVIRON_INTERNAL_*` process-bootstrap keys are excluded from the
frozen application env snapshot. They are supervisor protocol fields, not
operator configuration and must never participate in `${ENV_KEY}` expansion.

The default state constructor obtains the physical short hostname through
`sys`. Server boot may capture it earlier and use
`initialize_with_hostname()` so host-specific boot inputs and `state.hostname()`
share exactly one observation. Dotenv discovery and precedence remain server
policy. `ConfigEnvironment` prepares that policy through Dotenw before state
construction; `initialize_with_environment_in_fleet` retains its store/handle.
`state.environment()` exposes the boot-time deployment selection, not a dotenv
variable or hostname. Configless constructors keep a literal inherited snapshot
with selection `production` and do not discover files.

`AppState::control_pid()` is process-family control metadata:
master and standalone processes report their own PID, while workers
report their parent PID captured at state initialization. It identifies the
PID that receives process-family control signals from `ServerManager` and is
not a persisted config value.

`AppState` implements both continuo state traits:

- `continuo::SharedState` for shutdown, registry access, and process event access.
- `continuo::ReloadState` for the registry reload path.

`Registry::reload_all()` returns Continuo's `ReloadOutcome`. State reload and
lifecycle-plan failures remain the outer `Result`; individual reloadable
provider failures are retained with their provider names while the broadcast
continues. Continuo performs no reload logging. Claviron's signal and worker
coordination boundaries decide how to report those failures.

`AppState` supplies its catalog through `ReloadState::reload_config_reader`.
Continuo's `Registry::reload_all` prepares every registered Congif owner as one
batch before state/provider hooks. Those owners consume prepared snapshots
rather than reopening sources. Successful application acknowledges that exact
candidate in Congif; failed application does not. An invalid preparation batch
leaves the catalog and service runtime values unchanged. State owns identity
and the reload counter, not a second configuration orchestration loop.

`Runtime::configure` binds AppState's `HasConfigReader` accessor after initial
configuration succeeds. Targeted `ServiceManager` reloads use that same catalog
for preparation and application acknowledgement even without native watching.
Watching provides source invalidation only; it does not own the manual reload
capability. Configurable services reject reload when no catalog has been bound,
rather than silently applying a stale snapshot.

With the facade's `config-watch` feature, server bootstrap enables this common
watcher after provider registration and before initial configuration, in both
edge and application-server compositions. Without that feature, configuration
preparation and manual reload remain available, but bootstrap creates no native
configuration watcher. The feature forwards `claviron-runtime/config-watch` to
Continuo's existing capability; it is independent of `runner-watch`, which
observes application source code. AppState still owns one reader and one event
bus; HTTP providers add source declarations, not per-provider watcher tasks.

AppState installs one Dotenw environment handle in the ConfigReader's environment
catalog. Server bootstrap prepares the inherited base and selected dotenv
recipe before logging, without process-env writes. Registrations use
`bind_config_environment::<Provider>()` to select that same handle. Congif's
central watcher and reload preparation refresh its registered files; there is
no second Dotenw watcher. Captured base values are not re-expanded. The legacy
`env_snapshot()` accessor remains the frozen startup view; consumers migrating
to live configuration must use the reader binding, not that legacy copy.

Program environment declarations are separate from configuration snapshots.
Relative program `env_file` paths resolve from the process working directory,
not `config_dir` or the executor's `cwd`. Dotenw normalizes the source identity,
including symlinks, for shared reads and watcher registrations.
`AppState::read_program_environment` evaluates files, deployment variants and
inline overrides for one occurrence without registering a catalog node.
`observe_program_environment` returns Dotenw's execution-owned notification;
its drop removes that execution's watch membership. Neither operation writes
or scrubs process environment. Task execution now adopts these APIs: bounded
occurrences read once; daemons await notifications inside their existing
supervision future and drain before replacement. `program_recipe` lets
`claviron-app::environment` carry the same base recipe without retaining
AppState. PHP/JS/Lua application owners now await execution notifications;
embedded SAPI pins a route-owned snapshot before each request dispatch.
Worker-to-master environment delivery remains future work;
these entrypoints currently evaluate in the calling process.

The same boundary applies to validation and finalization. After boot,
`Registry::validate_all()` checks the complete lifecycle plan and returns every
provider failure in `ValidationOutcome`; bootstrap reports all failures and
refuses to start runnables when the outcome is invalid. After runnable drain,
`Registry::finalize_all()` returns `FinalizeOutcome`; bootstrap reports every
failure while preserving best-effort reverse-order cleanup. Continuo does not
log either outcome.

Duplicate concrete provider registration is an error. Registration helpers
must propagate it, or explicitly test for an existing provider when repeated
feature-level registration is intentionally idempotent.

## Public Shape

Management can read `runtime_started_at` and monotonic `runtime_uptime` from
AppState. Both describe the responding process's runtime and survive reload;
neither is a worker-side estimate of the master's lifetime. `ApplicationBuild`
is optional, application-registered version metadata. `runtime::VERSION`
identifies Claviron itself, never the application.

The intended public split is:

```rust
use claviron_runtime::{
    AppState, RunContext, RunMode, Runtime, ServiceManager, ServiceStartPolicy,
    registry::{Finalizable, Provider, ReloadOutcome, Reloadable, Runnable},
    gate::{Gate, GateDrainOutcome, Permit},
    guard::GuardGroup,
};
```

`Runtime` is a type alias for `continuo::Runtime<AppState>`.
Claviron registers `runtime.manager()` before provider boot. Transports and
future fleet adapters resolve that command capability; the runtime remains the
only owner of live service generations and their status.

Each `ServiceSnapshot` reports whether its provider implements `Reloadable`,
the provider-owned `ServiceStartPolicy`, the current start generation, and a
completed targeted-reload revision. `Automatic` creates an initial generation,
`Manual` remains explicitly startable without creating one at process boot,
and `Unavailable` rejects explicit starts on that runtime. Continuo asks the
provider for its current policy instead of copying configuration into the
manager. Both a successful and a failed reload advance the reload revision;
`last_reload_error` retains the latest failed outcome and is cleared by the
next success. This lets an external observer distinguish command completion
from a stale running snapshot without moving reload state into its transport.

Controller lifecycle and execution are independent facts. `ServiceSnapshot`
also carries dispatch and `active_executions`: a `One`/`Pool` controller can be
running while waiting for a lease, whereas only members with a positive census
are executing the runnable. Each execution is counted at entry and released
when its future ends, including graceful drain, errors and abort. Counters are
generation-owned and require no request-path work or additional polling.

## GRACEFUL_OWNERSHIP And `NO_IMPLICIT_TERMINATION`

The canonical contract is
[GRACEFUL_OWNERSHIP: NO_IMPLICIT_TERMINATION](../../docs/design/GRACEFUL_OWNERSHIP.md).
It defines service and process grace, foreign-execution boundaries, and the
limits of adoption continuity. This section describes the runtime's application
of that contract.

Claviron submits every `Runnable::run()` future to Continuo's `Runtime` and,
after initiating shutdown, awaits `Runtime::drain()`. The runtime adds no
deadline of its own: a runnable completes its owned shutdown and drain protocol
before its `run()` future returns.

`Gate::wait_all_done()` makes the boundary observable. It returns
`GateDrainOutcome::Drained` after every accepted permit is released, or
`GateDrainOutcome::Forced { remaining }` when an explicitly configured grace
period expires and the hard-shutdown signal is sent. Continuo does not log the
decision; the owning transport reports it.

### Adoption

Process survival is not a service-state continuity guarantee. See
[adoption is not service-state adoption](../../docs/design/GRACEFUL_OWNERSHIP.md#adoption-is-not-service-state-adoption)
for the distinction between retained processes, provider finalization and
resource-specific continuity contracts.

## Configuration checks: `ConfigCheck`

A process started for `--check-config` (`Server::check_config`, through
`Server::initialize_for_check`) carries `ConfigCheck` in its registry. It
registers and boots every provider — boot is where most of them read their
files — and validates them, and does nothing else: no runnable runs, nothing
binds, no worker is spawned, nothing is finalized, and no log file is opened.
A check may run beside the live service, as its user and on its fleet, so:

- the managed shared-memory executor never resets, recreates or unlinks a
  segment in a check, whatever a resource's policy says, and skips finalize;
- a check never takes the master's role, whatever the worker count;
- a provider whose boot writes anything outside its own objects — which the
  boot contract already forbids — must not do so when `ConfigCheck` is
  present. Third-party providers are held to the same rule.

This is the interim shape. A check still half-builds the server; reading and
validating configuration without building it belongs to a configuration layer
of its own.

## Local Event Listeners

Each AppState owns an `EventDispatcher` and inserts that same instance as a
provider into its registry. `state.events().listen::<E, _>(callback)` registers
a typed callback synchronously, without starting a task or resolving another
provider. The callback receives `(AppState, E)` when executed and returns an
async registry result. Capture a provider Arc if needed; capturing AppState
itself would create a reference cycle through the listener registry.

Registration subscribes immediately, so events emitted after registration but
before runnable execution are buffered by the existing ProcessEventBus. There is
no replay of earlier events. Runtime submission seals registration, including
when there are no listeners. A configuration reload does not add listeners.

The dispatcher exposes one restartable Runnable when callbacks exist. Its
explicit `Scope::Any` admits this process-local infrastructure in master,
worker, and standalone. The default worker scope must not exclude master
startup acknowledgements or management
callbacks. Listener registration still determines whether a runnable exists.
It polls one future per listener through `FuturesUnordered`; there is no
per-listener or per-event Tokio spawn. Each listener processes events serially,
while different listeners
can make progress independently at await points. Callback errors are logged and
processing continues. Panics fail the dispatcher runnable. Blocking callbacks
block every local listener, so blocking/CPU work needs its own appropriate owner.
Receivers remain registered across a targeted dispatcher stop; their bounded
backlog is therefore available to the next service generation without replaying
events from before listener registration.

The existing `emit` and `subscribe` remain available through
`Deref<Target = ProcessEventBus>`; `events().bus().clone()` clones the primitive.
Continuo's SharedState still exposes that same
underlying bus, so generic publishers and existing Orbit bridges reach the same
local subscribers. The primitive's existing bounded channel capacity and lag
semantics are preserved; dispatcher listeners log lag instead of inventing
delivery or acknowledgement. No event type is automatically registered with
Orbit by this local layer.
Explicit `orbit` bridges register their outbound work as ordinary local
callbacks here; Orbit's inbound cursor and managed poller remain in that crate.

`State::initiate_shutdown` synchronizes token cancellation and publication of
`ShutdownInitiated` with dispatcher shutdown. Listeners finish their current
callback, snapshot their retained backlog, then process that fixed snapshot.
The receiver is retained for a possible later service generation, but events
emitted by drain callbacks cannot extend the current drain. Draining does not promise a total order between event types or
delivery of recursively emitted teardown events. Runtime drain waits for all
callback futures; abort-and-drain drops them. Callbacks must complete their own
teardown and cannot wait for provider finalization, which happens after drain.
Master fleet draining still precedes global cancellation, so local listeners
stay active throughout worker drain/reap.

## Boundary Rule

Routine lifecycle diagnostics use INFO in master and standalone processes and
DEBUG in workers. Providers make that choice at the existing log call, using
the owning process role; this is not a blanket worker filter. Errors and warnings
keep their original severity. Process-local listener binds and application loads
are reported only where they occur; no master readiness acknowledgement is
invented for work performed by a worker. Provider lifecycle and execution
placement are unchanged.

Do not reintroduce local copies of the generic primitives here.

Reusable transport and lifecycle primitives remain in `continuo`. The local
callback dispatcher is an AppState-specific runtime binding maintained here;
it composes ProcessEventBus and Runnable rather than copying either primitive.
It has no Orbit dependency. Cross-process codecs, publication, and remote
listeners belong to `orbit`, including the fleet adapter over the
process-local `ServiceManager`.

## Environment Tests

`tests/env_scrub.rs` keeps its process-env mutations in one integration test
with sequential scenarios. Do not split them into concurrent `#[test]`
functions: environment state is shared across threads. Each scenario preserves
the inherited PATH. Other test targets use their normal parallel runner; no
workspace-wide `--test-threads=1` setting is required.
