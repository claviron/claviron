# Claviron Architecture

`claviron` is the public library/facade crate for the Claviron runtime.
This repository may contain many internal crates under `crates/`, but
external repositories must treat those crates as hidden implementation
modules.

## Publish Boundary

The publishable public surface of this repository is the root
`claviron` crate.

Downstream repositories such as `claviron-full`, `claviron-extra`, and
small runtime-only experiments
must depend on Claviron like this:

```toml
claviron = { version = "0.2", default-features = false, features = ["server"] }
```

`server` is the transport-neutral standalone application-server runtime.
`supervisor` adds the optional master/worker process model. An HTTP edge
application selects `edge`, which includes both and adds the HTTP, Hyper,
vhost, host-config, and static-file layers:

```toml
claviron = { version = "0.2", features = ["edge"] }
```

`http` exposes framework-neutral HTTP primitives; `axum` adds its Rust router
API. HostPipeline preparation, file steps, embedded PHP/Lua, Upstream and Link
use native request/response ownership. Axum is an optional Rust application
adapter, not their host compiler. Embedded PHP does not select Axum by itself.

`files` exposes static-file serving and its cache lifecycle independently of
`edge`. Application servers provide a `files::FileSite`; virtual hosts are not
required. The `edge` feature includes `files` for its existing host pipeline.

For configless micro-runtime binaries that only need the provider
kernel, use the narrower runtime facade:

```toml
claviron = { version = "0.2", features = ["runtime"] }
```

They must not depend on paths like:

```toml
claviron-edge = { path = "../claviron/crates/claviron-edge" }
claviron-hosting = { path = "../claviron/crates/claviron-hosting" }
```

If an external crate needs a core subsystem, expose that subsystem
through a `claviron` feature and import it through the facade:

```rust
use claviron::runtime::AppState;
use claviron::edge::VhostResolver;
use claviron::hosting::{HostingStore, VhostImport};
```

## The server module

Custom host directives use `claviron::edge::extension::RouteExtension` and
`Server::with_edge_extension`, without Axum. The builder registers their
compilers in HostAppRegistry before provider boot. Configuration is decoded
while preparing host snapshots; request dispatch uses only prepared conditions,
native HostPipeline actions and Rust contributions. Concrete application
directives belong to consumers; unresolved names fail publication.

`claviron::server` is the transport-neutral process and lifecycle host. It
creates the process `AppState`, selects standalone/master/worker identity,
configures logging and dotenv policy, registers the built-in provider
composition selected by Cargo features, and runs the Continuo lifecycle.

`ServerOptions::build()` prepares a `Server` without I/O or execution.
Applications append registrations with `.register(app::bootstrap)` or
`.with_services::<(FirstService, SecondService)>()`. The typed form uses
Continuo's `Register<AppState>` contract; it does not require a default
provider constructor. Both forms compose in declaration order after built-in
registration in each process, stop on the first failure, and retain previously
registered instances. They neither configure nor start services. The builder's
callback type is inferred, so callers use `let server = ...` rather than fixing
it to the default `Server` type.

The order is: load identity, register providers, acquire the process-family
lock, install signals, configure through the shared Congif reader, boot and
validate providers, spawn runnables, wait,
drain runnables, then finalize providers. It preserves the
[GRACEFUL_OWNERSHIP: NO_IMPLICIT_TERMINATION](docs/design/GRACEFUL_OWNERSHIP.md)
contract — nothing here adds a hidden drain deadline.

Applications may explicitly register `orbit::placement::OrbitDispatcher` before
boot. Each coordinated component supplies its prepared `DispatchPolicy` through
`Dispatched::dispatch_policy`; the adapter has no policy file or name-keyed map.
Bootstrap installs that adapter on
Continuo's Runtime; `Each` stays direct and `One`/`Pool` use the adapter for the
whole managed generation. Without registration, coordinated dispatch is an
error, never an implicit per-process execution. The adapter owns leases and
local child drain, while Continuo remains the owner of service management.

It owns no application-language engine and no development HTTP adapter. A
language runner composes one on top of this lifecycle from its own
repository; the edge preset composes production HTTP transports and routing
through the same host.

It lives in the facade rather than under `crates/` because it is the one place
those crates meet, and because a crate boundary here bought nothing: every
feature and every optional dependency had to be declared in the facade,
forwarded, and declared again beside the same dependency, with the two lists
kept equal by hand.

## Internal Crates

HTTP transport ownership is split by driver, not security policy:
`hyper` owns TCP/Hyper H1/H2 (cleartext and TLS modules), while
`h3` owns Quinn/H3. `http` remains the shared HTTP contract
and config layer, and `edge` owns application routing. Redirect
and HTTP-01 are handlers inside the TCP transport, not a separate server
crate. See [HTTP-REFORM.md](docs/HTTP-REFORM.md) for the staged listener migration.

The crates under `crates/claviron-*` own the implementation and their
own invariants. They may depend on each other inside this repository
when that is the local layering contract.

That permission does not cross the repository boundary. Once code lives
in `claviron-full` or `claviron-extra`, it is outside the core library
and sees only the facade.

## Feature Contract

`clients` exposes Orbitive's existing code-first `PoolOptions` and client-pool
runtime alongside Claviron-owned `clients::PoolConfig`. This generic Serde
declaration is independent of a transport or manager and converts into
`PoolOptions` via `Into`; the options API remains unchanged. All capacities
must be declared, and omitted/null durations remain `None`. Configuration
validation reports the runtime pool's existing capacity/duration constraints
before boot. Sharing a declaration type does not share runtime pools or their
connections. `clients-fleet` continues to expose the separate fleet-pool API;
no manager, runnable, file path or deployment policy is added by this config.

Facade features are the public API contract for optional subsystems.
The files dependency selects no Axum feature by itself. The explicit facade
axum feature weakly forwards to selected Edge, Files and Runner adapters. In
particular, Edge's TCP driver no longer supplies the Axum application feature
implicitly. The edge preset selects http, not axum; consumers opt into the
Axum application API separately. Files without that choice retains its native
HTTP boundary, while files + axum preserves the public router/mount adapters.
`runner-cli` adds command-line parsing to an already-selected `runner` through
weak forwarding. It does not select a serving composition by itself; a CLI-only
consumer can use it without pulling in the runner shell.
When an external crate needs DB-backed vhost import, TLS certificate
management, runtime providers, HTTP route bodies, or hosting operations, add
or reuse a `claviron` feature that enables and re-exports the required
surface.

`runtime` is intentionally smaller than `server`: it exposes
`runtime` without process bootstrap, logging, or Orbit. `server` adds
a standalone application-server lifecycle — `claviron::server`, this
crate's own module rather than a crate beneath it — but deliberately does not
pull the supervisor, HTTP, Hyper, vhosts, host configuration, or static-file
serving. `supervisor` adds master/worker detection, signals, and
child-process lifecycle on top of `server`. An application runner may
therefore compose `supervisor` without becoming an edge server, while
`server` alone remains a supervisor-free standalone composition.

The composition lives here because it is the one place every crate under
`crates/` meets. As a crate of its own it had to redeclare each feature and
each optional dependency that this file already declares, forward them, and
keep the two lists equal by hand — a second copy of the feature surface with
nothing of its own to say.

A language runner is a separate repository rather than a feature here: it
compiles the application's binary, and a library does not compile the things
built on it. `gufi` runs a Python application this way, reaching this crate
through the facade like any other consumer.

Gufi owns CPython 3.15 selection, libpython/stdlib pairing, venv initialization,
PyO3 and ASGI lifespan. The shared runner/edge remains language-neutral and
adds no Python dependency, ABI gate or fallback interpreter. A Python upgrade
does not change Orbit framing, ingress or the host's drain contract. Mixed
Python/native profiling needs frame pointers through the final application and
all native dependencies: select the Rust flag in the application's shared build
configuration, not in a library build script or per-command environment.

`php_embed` exposes the embedded PHP runtime independently of `edge`.
It enables PHP edge dispatch only when that optional dependency is already
selected. A standalone PHP runner can compose `server`, `axum` (for the
shared HTTP contract), and `php_embed` without booting vhosts or listeners
owned by the edge preset.

`upgrade` and `broadcast` are also independent of the edge preset.
They expose native upgrade hand-off and the live broadcast service to
application-owned listeners. Their edge routing bindings are enabled only when
the edge dependency is selected separately. `laravel` can therefore provide
its native PHP broadcaster against a local Broadcast service without booting a
second HTTP edge. Pusher wire types live under
`claviron::broadcast::protocol::pusher`. The broadcast crate's own optional
`pusher` feature adds the external HTTP compatibility adapter. Edge broadcast
bindings select it internally; the facade exposes only `broadcast`. The service loads `broadcast.yaml`. The previous
`soketi`/`pusher` facade features and `soketi.yaml` have been replaced by this
capability naming; WebSocket event names and signatures remain Pusher-compatible.

`websocket` enables configured Lua H1/H2 endpoint registration when `edge`
and `lua` are already selected, as well as the engine's WebSocket support.
It does not implicitly select Lua or Edge for other WebSocket consumers.

`edge` preserves the former `server` composition and boots cleartext H1 without
certificate configuration. TLS termination is an explicit `tls` facade
capability (which enables Hyper's TLS driver
and weakly enables the upstream TLS connector when `upstream` is selected);
`http3` implies `tls`. Selecting `tls` boots the certificate source provider and
requires its normal `tls.yaml` configuration, even without a TLS listener.
The source publishes identity lookup and public trust independently of SNI.
Only `edge`/`upstream` composition or explicit resolver registration builds the
SNI map and requires a default identity. `hosting` remains DB-managed persistence
without selecting Edge: HostingService can supply server, client and CA records
to a non-HTTP service. Public CA reads never load signing keys; issuance explicitly
obtains a separate CertSigner. See [TLS sources](crates/tls/ARCHITECTURE.md#4-sources-and-sni-consumption).
This is feature-level composition, not lazy, configuration-driven TLS startup.
The HTTP provider validates every boot/reload candidate against the facade's
installed transport capabilities on all process roles before publication.
TLS-related types may remain transitive dependencies of shared contracts;
disabling termination does not promise a TLS-free dependency graph.
There is no facade `redirect` feature: cleartext hosting belongs to Hyper,
while `transport.non_ssl: serve | redirect | reject` chooses per-host behavior.
`server` compiles Orbit because application services may use its same-host
primitives even without child processes. `supervisor` turns those primitives
into a managed master/worker fleet, and `edge` adds the platform's reuse-port
HTTP transports. Standalone, master, and worker are runtime modes selected by
the embedding composition. `ServerOptions::workers` and `Server::workers` are
available only with `supervisor`; a `server`-only build stays standalone without
an explicit worker-count setting. A non-server
tool that directly uses `claviron::orbit` must select the `orbit` facade
feature. The `metrics` feature controls instrumentation only and does not
decide whether Orbit is available.

`server` also installs one process-local `claviron-app::ApplicationManager`
after Orbit registration and before execution-engine providers. The manager is
present on master, worker, and standalone roles, but only serving roles register
PHP, JavaScript, Python, or future Rust application controllers. It consumes
the Orbit capability initialized by Claviron; it never joins or resets the
fleet itself. When `supervisor` is selected, the server also binds
runtime-qualified application management commands to the fleet event registry;
a command is ignored on placements that do not host its runtime rather than
inventing application state on the master.

`resolver` exposes the shared DNS client contracts/configuration through
`claviron::resolver`; `resolver-hickory` adds the built-in Hickory adapter.
Neither implies server/bootstrap registration, Orbit or upstream. Custom
implementations consume only the contracts. Query TTL/cache policy belongs to
this lower layer; origin membership, fleet election and pool/balancer updates
belong to upstream. See [resolver architecture](crates/resolver/ARCHITECTURE.md).

`auth` exposes schema-independent identity contracts through
`claviron::auth`. Concrete users, roles, permissions, password policy,
and ORM entities remain downstream. JWT and other optional services
resolve the application's dynamically bound `UserProvider` instead of
depending on its model crate.

`users` exposes user CRUD through `claviron::auth::users`. The independent
`authorization` feature exposes role/permission and assignment contracts through
`claviron::auth::authorization`
for application-owned user, role, and permission administration. It selects
neither a DB nor a serving composition. The downstream manager supplies scoped
projections, schemas, validation, and mutations; transport adapters consume it
without extending `UserProvider` or defining a common users table.

JavaScript is an independently consumed workspace member: applications depend
on `claviron-js` directly, not a facade `js` feature. It retains path
dependencies on the internal Claviron crates, matching their shared contracts.
Neither the facade nor Edge depends on this engine, so its optional Deno
dependencies do not enter unrelated downstream lockfiles. Keeping the crate
in this workspace does not make it a dependency of the facade.

Consumers select the engine's own features, register its service explicitly,
and compose `claviron_js::routing::build_router` or their route extension.
Edge no longer installs JavaScript routes or WebSocket bindings implicitly.

`php_embed` exposes `claviron::ephp::{PhpEmbedRuntime, RequestPool,
DedicatedPhpProgram}`. It does not implicitly register PHP during server
bootstrap. Applications call `claviron::ephp::register(state)` like other
configured subsystems; the embed crate reads `php.yaml`, resolves dedicated
`env_file` entries and constructs the typed service internally. Inline environment
values take precedence. Alternatively, explicit builder composition remains
configuration-free and can register an optional request pool and named programs
without any YAML file. Operator files and script paths remain application-owned,
but their schema, parsing, defaults and validation belong to the library.
Providers may contribute definitions during boot via
`state.php_embed()?.register_dedicated(program)?` (`PhpEmbedStateExt`). The
catalog closes on the first start, after all boot contributions. One process
shares a SAPI module; request-pool slots and dedicated OS/ZTS threads have
separate ownership.

`PhpEmbedRuntime` (also named `PhpEmbedRuntime`) is the shared, non-runnable
provider. Registration adds the independent `PhpScriptService` and
`PhpApplicationService` runnables only on workers/standalone, retaining common
provider/master infrastructure without PHP execution on master, and installs
the `php:` task executor on `TaskService`. `PhpScriptService`
aliases the request runnable for HTTP adapters. Each service stops, joins and
restarts only its own threads over the retained process SAPI, without rereading
configuration or reinitializing PHP modules. Managed execution failures leave
the other service running; native process crashes are not isolated.

A named background PHP program is a `php:` task (`docs/PHP-TASK-EXECUTOR.md`), so its
placement, retry and drain are the task's: `scope`, `dispatch`, `concurrency`,
`attempts`, `backoff` and `graceful_timeout`, read by `TaskService` exactly as
for the `builtin` and `command` kinds. Master is never eligible, because the
engine is not there — `TaskExecutorKind::Php` defaults `scope` to `workers`.
`php-embed` supplies only the execution: one closure, installed
through `TaskService::set_php_executor`.

`tasks` owns an unconditional internal Orbit dependency because
`dispatch: one` and non-standalone `dispatch: pool` have no alternate
coordination transport. Selecting the facade's `tasks` feature therefore
compiles that implementation automatically; callers do not select a second
`orbit` feature merely to use task APIs. The facade's `orbit` feature only
controls direct access through `claviron::orbit`.

`supervisor` requires Orbit. Supervised application registration must
provide the Fleet and Orbit event registry before supervisor registration;
there is no Unix-socket heartbeat fallback.

`AppState` owns a single managed local event dispatcher. Providers register
callbacks before runnable submission; callbacks run only after provider boot.
Orbit owns its separate inbound runnable and bridges only explicitly registered
event types into that same local dispatcher. OS signal tasks only initiate
process control; they do not register event callbacks.

Server bootstrap emits the identity-free, process-local `ProcessStarted`
after successful provider boot and runnable submission in every run mode.
The supervisor's worker adapter translates it into a private, identity-bearing
Orbit acknowledgement. The master confirms the exact child set and emits
local `WorkersBootConfirmed`. Neither marker asserts readiness of individual
runnables. Heartbeats remain a separate liveness mechanism.

Bootstrap validation and finalization failure messages include both the provider
name and the underlying error in the message text, while retaining structured
fields. Plain journal output must show the diagnostic without JSON field
inspection. Downstream config-inspection entry points use the same convention.

After worker runnable drain and provider finalization, bootstrap publishes a
private `WorkerShutdownCompleted` envelope directly to Orbit. This synchronous
write needs no local forwarding task or master ACK. The master removes only
the matching worker/PID liveness entry; OS child reaping still proves exit.
There is no local `ProcessExit` marker or supervisor UDS transport. An internal
`HeartbeatObserver` owns PID-correlated liveness observations; `WorkerFleet`
retains process ownership and lifecycle state. `WorkerControl` is the non-Runnable
API. Monitoring, reconciliation, operation execution and publication have
separate registered Runnable generations; stopping the monitor does not stop
workers. See `claviron-supervisor/ARCHITECTURE.md` for owners and finalization.

Do not make external crates spell internal dependency features such as
`claviron-edge/db` or `claviron-hosting/server`. Those details belong
inside `claviron`'s own feature graph.

## Smoke Checks

SQLx defaults are disabled; database drivers are selected explicitly by the
owning backend features. Cargo defaults `SQLX_OFFLINE` to `true` for this
workspace, keeping query-macro checks independent of a live database. A caller
may explicitly override it. This setting does not disable runtime database
connections, and a downstream workspace must declare its own Cargo setting.

`Justfile` is the local and CI smoke entry point. `just smoke` runs workspace
tests with all features, including doctests, and passes `--test-threads=1` to
the test harness. This serializes tests within each binary, not Cargo builds.
Recipes set `SQLX_OFFLINE=true` so SQLx macros use the tracked query metadata.
Live database tests still follow their own environment-variable opt-in rules.

`just smoke-transports` separately checks the empty facade, runtime-only,
managed server, cleartext edge, TLS, H3, upgrade, WebTransport, hosting and
GeoIP combinations. It runs the edge boot test without TLS/DB and the focused
transport suite. Handler binding capabilities remain separate from listener
activation: `webtransport` provides vhost bindings; `http3` installs the QUIC
driver. Neither optional handler bindings nor redirects force a TLS listener
into the cleartext `edge` composition. Existing internal crates remain
available to each other, while downstream users select public facade
capabilities.

## Boot Environment

### Primary process exclusion

`bootstrap::start_services()` acquires a name-based `ProcessLock` before runtime
manager registration, provider boot, Orbit ring resets or worker spawning.
Master and standalone contend for the same `AppState::name()` across different
config directories and binaries; workers do not acquire it. `Server::initialize()`
remains a registration/config-inspection entry without a serving-lifetime lock.
Registration callbacks must not start serving or mutate shared live state.

The bootstrap future owns the guard through reload, worker rotation, shutdown,
runnable drain and provider finalization. Contention reports `InstanceInUse`;
other OS errors fail startup rather than bypassing exclusion. The sys primitive
uses a fixed host-local namespace, not the app cwd, UID or `TMPDIR`, and never
treats a leftover file as an active process. See `claviron-sys/ARCHITECTURE.md`
for filesystem namespace and cleanup requirements.

This protects primary identity, not the entire surviving process tree. A crashed
master releases its lock; orphan workers do not inherit it across re-exec and
may still own reuse-port listeners. Acquiring the primary lock is therefore not
proof that all old workers exited.

During initial convergence, `WorkerReconciler` reconciles every occupied slot from the
worker-written presence ring. It validates the worker's PID plus kernel start
stamp, incarnation, original master identity and presence freshness. A dead
exact worker permits slot reclamation. A live worker whose known original
master is dead must explicitly accept a claim from the newer live master; only
that acknowledgement, or subsequent worker-written presence naming the same
master, installs `ManagedWorkerHandle::Adopted`. Adopted handles can observe and
signal the exact process but cannot invent an exit status that belongs to the
old parent. Ambiguous, stale or conflicting evidence leaves the slot blocked
with its PID and identity visible: the master neither spawns over it nor kills
it. Legacy heartbeat motion can reserve a slot during migration, but cannot
authorize adoption or termination.

The management snapshot publishes runtime age and ownership as separate facts.
`WorkerOwnership` is derived from the current process handle on every snapshot:
a child created by this master is `Spawned`, a handle accepted from an earlier
master is `Adopted`, and a slot without an owned live handle is `Unowned`.
Rotation or restart replaces an adopted handle with a new child, so the next
snapshot changes to `Spawned` automatically instead of retaining stale origin
state. Runtime age remains the worker's age and is not reset by adoption.

### Environment loading

Server setup applies inherited process values → `.env` →
`.env.<environment>` from the application working directory. `ServerOptions`
selects the environment, defaulting to `production`; an embedding CLI may
expose it as `--environment`. Worker bootstrap preserves that selection even
though workers do not inherit the application's CLI. Hostname remains physical
identity, not deployment selection. Existing optional files are read through
Dotenw; invalid/unreadable files reject startup, missing layers are skipped.
The same policy applies under systemd: inherited `EnvironmentFile=` values are
the base, not a reason to skip dotenv layers. Values never go back into process
env. Congif bindings share the retained recipe and central watcher/reload path;
legacy `env_snapshot()` consumers retain their startup view.

`.gitea/workflows/smoke.yml` runs on push and manual dispatch. It uses the
existing `rust-ci-native` and `FreeBSD` runner labels. Native hosts run the
tests; the Linux-hosted FreeBSD runner only cross-builds all workspace targets
and features and requires `CARGO_BUILD_TARGET=aarch64-unknown-freebsd`.
Cross-build success is not evidence that FreeBSD runtime tests passed.

Runners must provide Rust, Cargo, Just, Bash, and the native build dependencies
required by the enabled subsystems, including compatible PHP embed development
files, `php-config`, and libclang. Cross runners also need the matching FreeBSD
toolchain, sysroot, and target libraries. The workflow does not provision an app,
load an app's `.env`, or install those dependencies. `Cargo.lock` is not tracked
in this library workspace, so these recipes do not use `--locked`.
