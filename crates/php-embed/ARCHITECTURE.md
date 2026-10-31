# claviron-php-embed — Architecture

In-process PHP runtime: `libphp.so` linked into the claviron worker via
`ext-php-rs`, driven by claviron's own SAPI module. Per-vhost selection
between this runtime and the FastCGI/FPM runner happens in the vhost
router at boot.

The framework-neutral Composer package in the repository's `php/sapi/` is named `claviron/sapi`.
Laravel and Symfony Composer adapters depend on this package. The Rust crate
remains `claviron-php-embed`; source paths and PHP namespaces are unchanged.
Consumers must refresh their Composer lockfile and installed package metadata
when adopting the renamed package; no old-name package alias is provided.

## Native SQLite bridge

The `sqlite` feature exposes named SQLx pools through `claviron_sqlite_*`
functions and an opaque `Claviron\Runtime\SqliteConnection`. Calls run on PHP's
OS thread using the host's Tokio handle.

Each PHP work unit retains one connection per pool. Explicit transactions also
retain it between calls outside HTTP. At a work boundary, closing the lease
rolls back unfinished transactions and discards temporary tables and PRAGMA
changes. Cleanup waits for close to release the write lock.

After each operation, the bridge reads SQLite's autocommit flag under SQLx's
handle lock. An automatic rollback blocks further statements until explicit
rollback/begin or the next work unit; an ordinary constraint error need not.
BEGIN accepts DEFERRED, IMMEDIATE or EXCLUSIVE. Savepoints are caller-supplied
SQL, and the bridge never retries COMMIT.

Cells are decoded by storage class, including mixed-type columns and BLOBs.
Insert IDs come from statement results. Errors preserve extended SQLite codes
and SQLSTATE (`23000` for constraints, `HY000` otherwise). The pool's
`busy_timeout` limits lock waiting, not query execution.

## Returned PHP values without HTTP

`evaluate::PhpEvaluator` is an independent synchronous one-shot API: source
bytes + original path + an execution environment yield owned `PhpValue`.
It requires neither AppState, a request listener, provider boot nor a service
pool. A parser using it offloads the blocking call rather than binding a reused
Tokio worker to TSRM. Each evaluation owns an OS/ZTS thread and one native
request scope; all Zend values are decoded/dropped before RSHUTDOWN. It shares
the process SAPI/extension table and INI selection with hosted PHP, never
`Embed::run` or a second module startup.

`PhpBootstrap` optionally names trusted PHP code returning a callable
`(argument, environment): void`, which establishes helper/autoload context.
The document is then compiled from the supplied bytes under its original
filename. There is no second root-document read or stale OPcache lookup; PHP
itself owns any `require` the document executes. These executable inputs are
trusted code, not a sandbox. Objects/resources, recursive arrays and nesting
beyond the caller's declared maximum fail rather than crossing Zend lifetimes.
Array keys and binary strings are preserved; Serde lists have ordered integer
keys from zero, including empty arrays, and other arrays serialize as maps.

Evaluation is allowed in a master preparing config, but registers no HTTP,
TaskService or application execution placement there. Declare INI locations and
host extensions before the first native evaluation; prepare provider-backed
host bridges separately before using them. Evaluation's deadline comes only
from the existing host/INI declaration. Its cancellation token is exposed to
PHP's cooperative cancellation API, not a promise to interrupt arbitrary
foreign blocking I/O. Native execution checks belong in an external executable
harness; library tests cover owned value/adapter contracts without starting PHP.

## Caller-owned console commands

`console::PhpConsole` runs one terminal PHP command without AppState, provider
boot, HTTP or TaskService. It shares `engine/program.rs`'s argv, virtual cwd,
SAPI, TSRM and request-cleanup path rather than using the returned-value evaluator.
`ProgramModel::Console` distinguishes terminal ownership from managed task
output. STDIN/STDOUT/STDERR inherit process streams even in builds with `tasks`;
no TaskService output writer or application receive loop is fabricated.
The command runs on an owned, joined OS/ZTS thread. Intentional nonzero PHP exit
is returned as the console outcome, while compilation/fatal/engine errors remain
failures. Existing task/application nonzero-exit failure semantics are unchanged.
Console execution honours the process INI execution budget and an explicit
cancellation token, with no invented deadline or automatic runtime registration.
Host capabilities and extensions must still be composed before SAPI startup.

> **Status:** typed `PhpEmbedRuntime` composition provides an optional
> `RequestPool`, a TaskService-installed PHP executor and named `PhpApplication`
> pools in one process SAPI. Request threads use idle-only round-robin dispatch
> and per-thread TSRM; task occurrences run on separate OS/ZTS threads. At request threads=8 the
> embed runner outperforms PHP-FPM on the same Laravel + Postgres
> row-lock workload (see [§5. Performance — measured against FPM](#5-performance--measured-against-fpm)).
>
> Graceful shutdown drain ships in the runtime path:
> `SapiThreadPool::drain` stops accepting new work, flips the
> `claviron_is_cancelled()` PHP flag, then waits on the in-flight
> counter — bounded by the service's `graceful_timeout` (default: wait
> forever, deferring to the supervisor's hard kill timer).
> `PhpEmbedRuntime` is the shared runtime provider, not a `Runnable`.
> `PhpScriptService` and `PhpApplicationService` each own an independent
> `RunContext`, execution generation and drain. A bounded incomplete drain
> fails only its owning service and retains ownership of remaining work —
> **one service's drain may never interrupt another's execution.** Enforced
> since 2026-09-12 by `ExecutionOwner`: every PHP thread carries the identity
> of the generation that spawned it, and a drain interrupts only executions
> carrying its own — and only those actually running, never a thread sitting
> idle between requests. See `engine/interrupt.rs`.
>
> **Operator caveats:** the engine is stable for production under
> the single-operator multi-vhost deployment pattern. The SAPI thread
> pool is intentionally worker-local and vhost-agnostic, like a normal
> execution pool. Boundaries that need stronger isolation belong at the
> worker/process layer. Bounded caveats (no in-place PHP runtime
> reinit, one embedded runtime per serving process, worker/process rotation as the
> RSS safety valve, H3 streaming default-rejected per route) are
> documented below with mitigations.

### Native request identity

Facade `auth` weakly forwards `php-embed/auth`: auth alone never enables PHP.
The dependency is `php-embed -> auth`, never the reverse. A host binds
`auth::PhpRequestAuthenticator` before the PHP runtime boots. The capability
owns credential extraction, lazy user lookup and session activity policy.
`claviron_auth()` lazily invokes it on the host Tokio handle from a PHP OS
thread, with the actual native request headers; no PHP-supplied token is accepted
and no loopback HTTP is used.

Script request Parts and persistent application Jobs own the cached verified
result (including anonymous results), not the ZTS thread. A preceding Rust
adapter can attach `claviron_auth::session::AuthenticatedSession` to the request
extensions; the bridge reuses that user instead of querying again. Otherwise
the first native auth call resolves it. A native `AuthenticationResolved` marker
also preserves an anonymous decision: the bridge returns null rather than
trying another policy through its default header authenticator. No marker is
serialized or reused across requests. This is request state, not serialized
user data in the session store. The live session is
rechecked before projecting it. Event, task, detached and idle contexts return
null. Missing capability on an HTTP request is an exception. The projection
contains `subject`, nullable UUID `session_id`, credential `expires_at` in Unix
seconds and raw model `attributes`, not a mutable session or raw token.
`PhpModelAttributes` supplies the application's borrowed field projection;
Claviron encodes it into a PHP associative array. The projection must not query.
Native application extensions may use `auth::current()` to obtain the same
verified HTTP identity and checked live session. This accessor accepts no
caller-supplied identity and does not transfer authentication to another work
unit. Application-owned authorization projections stay separate from model
attributes and remain the application's responsibility.
Raw attributes are scalar/null values; decimal and JSON columns remain strings.
They may include hidden hashes and are not a public user serializer. Raw PHP,
Symfony and Laravel can consume the same array without framework-owned codecs.

### `ext-php-rs` feature posture

The dependency is built without its default features and explicitly enables
`embed`, `enum`, `observer`, `runtime`, and `smartstring` with ext-php-rs 0.16:

- `embed` provides the in-process SAPI/embed support used by this crate.
- `enum` makes native PHP enum builders and Rust/PHP enum conversion available
  to Claviron and downstream compiled-in extensions. Enabling the feature does
  not register an enum by itself; enum registration must travel through the
  same module-startup and per-thread ZTS contract described in §4.4.1.
- `observer` compiles the Zend function/error/exception observer APIs.
  Downstream `PhpEmbedExtension`s may contribute `PhpObserver` sinks. When at
  least one sink requests a callback family, Claviron builds one internal
  `ext-php-rs` module and passes it to `php_module_startup` during MINIT. The
  process-global dispatcher fans callbacks out to the immutable sink snapshot;
  sinks remain thread-safe and keep hot-path callbacks lightweight.
- `runtime` loads libclang dynamically for upstream bindgen; it must not be
  combined with ext-php-rs `_static`. The former bindgen fork is no longer used.
- `smartstring` adds `SmartString` zval conversions for compiled-in host
  functions. Existing `String` arguments are unchanged until an implementation
  deliberately adopts the type and demonstrates a useful allocation win.

Do not re-enable the upstream default feature set implicitly. Claviron keeps
the required capabilities explicit so adding an upstream default cannot change
the embedded runtime unnoticed.

---

## 1. Position in the request pipeline

```
                ┌──────────────────────────────────────────────────┐
                │  TLS / HTTP entry → vhost middleware (SNI)       │
                │  → claviron-edge: pipeline/router.rs           │
                └──────────────────────────────────────────────────┘
                                    │
                                    │  (per-host, per-route layer stack)
                                    ▼
        ┌────────────────────────────────────────────────────────────────┐
        │ Router builds runtimes from HostConfig and stacks layers       │
        │ shortest-prefix-first. Each runtime carries `php_runner`:      │
        │                                                                │
        │   PhpRunner::Fcgi  →  claviron-php-fcgi::build_axum_router    │
        │   PhpRunner::Sapi  →  claviron-php-embed::build_axum_router   │
        └────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
              (this crate — only when runner: script / runner: embed)
                                    │
            routing/bindings  →  routing/proxy  →  service/request
                                                          │
                                                          ▼
                                                     engine/mod.rs
                                                          │
                                                          ▼
                                              engine/pool/ (pool)
                                                          │
                                                          ▼
                                              dedicated PHP thread
                                              (libphp.so + ext-php-rs)
                                                          │
                                                          ▼
                                                  sapi/mod.rs (FFI)
                                                  sapi/callbacks.rs
```

The vhost router is the policy boundary — it decides per route which
runner serves a request based on `runner:` in `vhost.yaml`. Once a
route resolves to `sapi`, this crate owns the rest.

PHP tasks enter through TaskService instead of the HTTP pipeline. They share the
process SAPI and thread initialization with the request pool, but each occurrence
owns separate capacity and script lifetime (§4.7).

---

## 2. Typed composition and configuration sources

### 2.1 The embedded runtime API

This crate owns both configuration-backed registration and the typed runtime API.
The normal application entry is `claviron::ephp::register(state)`: it reads the
optional `php.yaml` under `state.config_dir()`, preserves application env-file paths,
builds the typed service and registers its providers. Applications supply their
operator files and scripts, not a custom PHP config loader. A missing YAML file
preserves the one-thread request-pool default. Configuration errors reject
registration before runtime/provider state is installed.

A single-application runner can call
`register_application_from_yaml(state, name, yaml, description)` with the `php` section of
its own launch document. `application` is adapted to that name in the existing
embed schema; all engine, pool and environment rules remain here. Omitted
`sapi` disables the separate script pool in this shape. Default registration
retains an explicitly registered runtime instead of reading `php.yaml` again.
Explicitly registering a second runtime is still an error. This is boot intent,
not a separate reload mechanism.

Script-mode runners use `register_script_from_yaml(state, name, yaml, description)`
with this same engine/SAPI schema. Their request route remains the public
`claviron-php::PhpConfig`, composed by the runner. A disabled SAPI pool or a
persistent application definition is rejected in that registration shape.
The typed builder's `script_application(name, description)` names the sole
request pool for common application management; unnamed edge pools are unchanged.
The script owner publishes lifecycle transitions from real runnable generations
and reports existing pool counters. Its PHP controller delegates commands to
the existing `ServiceManager` for `PhpSapiService`, without a parallel manager,
new transport resource or per-request metadata work.

`PhpApplication` prepares an `ApplicationDescription` at definition time:
PHP runtime, application request mode and Claviron as host by default. Its
`frameworks(...)`, `icon(...)` and `runner(...)` builders declare integration facts.
`framework(...)` remains a single-label shorthand. YAML
applications accept optional `frameworks` and `icon`; a single-application runner
supplies its own runner/preset description, without overriding an explicitly
written framework. If no framework was declared, application-generation
preparation inspects the nearest Composer project enclosing the entry script.
Installed package records take precedence over the lockfile, then root runtime
requirements. `laravel/framework` identifies Laravel and
`symfony/framework-bundle` identifies Symfony; both are reported as separate tags. Shared
Symfony components alone do not identify a Symfony application. No known
framework produces an empty list, and an unreadable record logs a warning
without changing execution. `framework::ComposerPackages` exposes the same
inspection to runner composition, including optional Octane detection.
An explicitly empty list suppresses discovery. The scalar `framework` config
remains accepted as a single-label list. Icon codes are descriptive client hints,
never paths or execution selectors. Lumenize's mode selection remains separate.
Management snapshots carry the prepared description through the same common
Application row as other engines. No request performs framework discovery.
SAPI remains the PHP embedding interface for both script and
persistent application execution, not a competing application request mode.

The independent builder remains configuration-free for code-driven embedding:

```rust
use claviron::ephp::{PhpApplication, PhpEmbedRuntime, RequestPool};

let php = PhpEmbedRuntime::builder()
    .request_pool(
        RequestPool::elastic()
            .min_idle(2)
            .max_size(32)
            .max_idle(8),
    )
    .application(PhpApplication::new("web", "public/application.php"))
    .build();
php.register(&state)?;
```

Explicit builder construction and its service's `.register(state)` do not read
configuration or script contents and start no PHP threads. Registration does
check that program paths exist. The free `register(state)` is the configured
entry; neither registration path starts PHP.
Registration validates capacities, application names and paths, environment
values and metric policy. Duplicate application names or a duplicate service in
the same registry are errors. `RequestPool::fixed(n)` fixes capacity, while
`elastic()` starts from one instance and allows explicit floor, ceiling and
warm-idle settings. PHP task capacity belongs to TaskService configuration.

Application providers can also contribute programs during their `boot()` through
`PhpEmbedStateExt`, using the already registered service:

```rust
use claviron::ephp::PhpEmbedStateExt;

// Inside an application's Provider<AppState>::boot:
state.php_embed()?.register_application(self.application())?;
```

Application builder entries and provider contributions use one definition catalog. The
catalog seals when the first service generation starts, after all providers have
booted. Duplicate names are rejected at registration; additions after sealing
are rejected instead of being partially applied to a running generation.
`PhpEmbedRuntime::has_application` reads this definition catalog without
starting a pool. Custom ingress providers validate their named targets after
all provider boots, so a missing target fails before listeners open while
boot-time contributions remain supported.
An empty `.builder().build().register(&state)` supports later application
contributions without a YAML file. If no request pool or applications are
registered, its runnables start no PHP threads. The optional `tasks` feature
installs a PHP executor into TaskService independently of that catalog.

| Typed option | Purpose |
|---|---|
| `RequestPool::min_idle` / `max_size` / `max_idle` | Request-pool floor, ceiling, and warm idle target; must satisfy `min_idle <= max_idle <= max_size` |
| `RequestPool::idle_timeout` | Idle duration before trimming excess request threads |
| `RequestPool::max_life` | Optional lifetime bound for renewing/removing idle request threads |
| `RequestPool::max_requests` | Optional completed-request threshold for idle thread rotation |
| `RequestPool::streaming_write_timeout` | Bound for response writes to stalled clients |
| `RequestPool::dispatch_queue_timeout` | Bounded buffered-request wait; zero means immediate rejection when busy |
| Service `ini_path` / `ini_scan_dir` | Process-wide PHP INI location and scan directory, applied before module startup |
| Service `graceful_timeout` | Cooperative drain bound for request and application services; PHP tasks use their TaskService-owned `graceful_timeout`. `None` waits indefinitely. Governed by [GRACEFUL_OWNERSHIP: NO_IMPLICIT_TERMINATION](../../docs/design/GRACEFUL_OWNERSHIP.md). It is **not** an execution timeout and must not be wired to one. |
| Service `management(PhpManagementConfig)` | Named application fleet snapshot publication cadence and accepted freshness |
| Service `metrics(PhpEmbedMetricsConfig)` | E-PHP metric enablement, publication cadence, and fleet sample freshness |

The embed crate's `config/file.rs` owns synchronous configured-registration I/O;
`config/file/schema.rs` owns the strict schema and resolved desired plan.
They read YAML and resolve its placeholders from the captured environment.
Application `env_file` paths are passed to `.env_files(...)`, while inline
values go to `.environments(...)`; resolution reads no environment files.
`claviron-app::environment` prepares these recipes through Dotenw when the
application service starts. Effective env changes use its existing maintenance
and joined-pool replacement path for the affected application only. A stopped
application stays stopped; invalid sources retain the previous execution.
No extra Tokio task or process-wide PHP reinitialization is involved.

For script SAPI dispatch, `PhpEmbedRuntime::execute` prepares the route's shared
environment slot on first use, then carries an immutable snapshot in request
extensions into `ClavironContext`. Preparation failures reject that dispatch;
later invalid replacements retain the previous valid snapshot. An active
request never switches environment mid-execution. These registrations live
with the route, not with a reusable ZTS thread, and require no SAPI renewal loop.
Parsing, defaults, validation and compatibility are library
contracts shared by all embedding applications.

The schema has one canonical shape; obsolete flat `threads*`, `ini_path`,
`graceful_timeout`, `request_pool` and `dedicated` keys are rejected. Unknown
fields, duplicate sections/fields and duplicate named programs are errors.

```yaml
engine:
  ini_path: etc/php.ini
  ini_scan_dir: etc/php.d
lifecycle:
  graceful_timeout: null
management:
  publish_interval: 1s
  max_age: 5s
sapi:
  pool:
    min_idle: 2
    max_idle: 8
    max_size: 32
    idle_timeout: 60s
    max_life: 5m
    max_requests: 10000
  requests:
    streaming_write_timeout: 200ms
    dispatch_queue_timeout: 5s
applications: {}
metrics:
  enabled: true
  publish_interval: 1s
  max_age: 5s
```

`engine` is process-wide INI/extension startup policy, not a pool. `lifecycle`
is the common drain policy for the request and application runnables; null or
omission means unbounded drain. Task drain policy lives in `tasks.yaml`.
`management` controls only the Orbit-backed
status observation window; it does not alter execution or retry. `sapi.pool` contains capacity and retirement,
while `sapi.requests` contains dispatch/write bounds. Omitted `sapi` retains the
configured one-thread default; `sapi: null` disables only that execution model.
A fixed pool sets only `min_idle`; absent ceiling/idle target follow it.
`applications` is a name map and does not inherit SAPI pool settings;
`instances` is fixed, `max_waiting` bounds admission, and `retry` governs failed
instance replacement (§4.8). Application `pool` keys are rejected. There is no `daemons:` section: a named
background program is a `php:` task in `tasks.yaml`, where placement and
supervision are the task's — `scope`, `dispatch`, `concurrency`, `attempts`,
`backoff`, `graceful_timeout` — and only the script, its arguments and its
environment stay with the executor. Metrics remain their own operational section
and currently describe SAPI request execution, not application pool capacity.

Relative script and env-file paths remain process-working-directory relative,
not config-file relative. The loader never implicitly discovers `.env` or mutates
libc environment with program settings. Direct builder values are consumed
literally, without file loading or expansion. Loading PHP entry scripts and PHP's
own INI initialization remain execution concerns.

Parsing and resolution perform no env-file I/O and produce a private
`ResolvedConfig` containing only typed intent, without engine/channel/thread or
registry state. Building that plan composes the existing config-free builder;
validation completes before registration publishes any providers. Resolving
another candidate cannot mutate an existing runtime or its environments.
`std::fs` is used because registration is synchronous, not because files cannot
reload. Future async reload must read off the executor, validate a candidate,
classify changed sections and coordinate apply with their owners before
publication. INI changes require process rotation; execution-policy changes
need an explicit safe pool update or service drain/rebuild contract. Changing
the reader to `tokio::fs` alone would provide none of those guarantees.

No section currently implements live reload. The execution plan and supplied environment remain fixed after the first-run
catalog seal. Service restarts reuse them; adopting new application configuration
requires a new process. Metrics also use immutable typed values, with no file
reload provider: configured registration resolves them once along with the plan.

There is one SAPI module, extension registry, PHP INI configuration, and set of
host bridges per process. Compose all programs into one `PhpEmbedRuntime` for
one serving `AppState`. Independent embedded runtimes require separate
processes; another builder is not an INI or extension isolation boundary.

`engine.warmup` is an optional folder list owned by this same startup plan.
Registration adds a worker/standalone-only `PhpWarmupService` whose one-shot
Runnable begins after provider boot, concurrently with serving runnables. It
does not gate listener readiness, run on the master, or depend on Server's
ProcessStarted event. The process module and INI declaration are still shared;
warmup uses `PhpEvaluator::evaluate_source` on an owned OS/ZTS thread, without
another SAPI startup or PHP-facing function/extension. Cancellation belongs to
the runnable generation; the synchronous execution observes its child token
between files. An owned admission guard remains with the blocking execution
until it ends, including when its async caller is cancelled.

The PHP source is compiled into the Rust binary with `include_str!` from
`php/embeded/warmup.php`. Cargo includes that source so registry builds can
compile it. The Composer publisher's explicit include list excludes `embeded`;
the script is neither autoloaded nor installed by Composer. It scans configured
roots recursively, excludes test/tests directories case-insensitively, skips
nested symlinks, deduplicates canonical file paths and
calls PHP's own OPcache APIs without executing the discovered source files.
Unavailable/disabled OPcache is a logged no-op. Per-file compilation/cache errors
are logged and skipped; other failures fail only the background occurrence,
not provider boot or serving. Native INI freshness,
blacklist, capacity and file-age policies are not overridden; warmup refuses to
report successful caching when PHP has not retained a file.

When TaskService is already registered, the optional `tasks` integration also
registers `php-warmup` as a manual, worker-scoped builtin body that calls the same
runtime operation. Its timing/attempt/placement policy remains TaskService-owned;
startup warmup does not require that integration. Concurrent warmups share one
process-local nonwaiting admission guard. No request checks it. Watch/recompile,
OPcache reset, persistent application reload and INI reload are outside this
operation.

### 2.2 Per-route PHP application config

The separate shared PHP `PhpConfig` (`../php`) is
composed into immutable `Arc<PhpRuntime>` instances at host boot. A vhost
configuration adapter can supply it from `config/vhost.yaml`:

```yaml
some.host:
  root: /sites/foo/public
  php:
    - path: "/"
      runner: script          # or "embed" — same enum variant (PhpRunner::Sapi)
                            # alternatives: fcgi / fpm (PhpRunner::Fcgi)
      entry_script: index.php
      try_files: true
      entry_extensions: [png, webp] # optional non-.php entry routes
      exec_mode: buffered
      environment:
        DB_DSN: "pgsql:host=…"
      env_file: ops.env
```

`runner:` is the switch:

```rust
// crates/claviron-php/src/config.rs
pub enum PhpRunner {
    #[serde(alias = "fpm")] Fcgi,
    #[serde(alias = "embed")] Sapi,
}
```

- `runner: fcgi` / `fpm` — out-of-process: claviron speaks FastCGI to a
  warmed FPM client pool (or one-shot client) in `php-fcgi`.
- `runner: script` / `embed` — in-process: this crate, `libphp.so` in the
  worker's address space.

Selection happens once per route at host boot, when
`pipeline/router.rs` walks the host's runtimes and calls the runner's
`build_script_router`. There is **no** per-request runner decision.

---

## 3. Boot sequence

`PhpEmbedRuntime::register` installs the shared runtime provider, its Orbit counter
dependency, optional E-PHP metrics, and the role-appropriate invalidation bus.
PHP invalidation registers its inbound-only codec on the existing
`FleetEventRegistry`, whose managed event ring owns reuse/preserve SHM policy.
Only workers with peer capacity register the managed invalidation consumer.
The crate features `apcu-intercept` and `opcache-intercept` compile only
their corresponding cache hooks and apply queues. APCu interception is opt-in;
OPcache interception is enabled by default for persistent applications and
multi-worker execution. Neither feature is forwarded by the Claviron facade.
With neither feature, no invalidation bus, codec
registration, consumer, replay guard or cache work-boundary checks are installed.
The public event data/codec remains available for typed observers.
`sapi.cache_invalidation` additionally controls the compiled propagation hooks,
defaulting to true; false registers no invalidation codec/consumer and installs
none of the compiled cache intercepts. This is shared SAPI policy for script, application and
task executions, not just the HTTP request pool. Rust composition uses
`PhpEmbedRuntimeBuilder::cache_invalidation(bool)`. Local PHP cache semantics and
the separate `putenv`/sleep intercept policies remain unchanged.
The policy is frozen in the resolved startup plan: changing it requires worker
process replacement, not an application/pool or service restart. `sapi: null`
disables only the HTTP pool and leaves the shared invalidation default intact.
Its typed subscription is captured before runnables; the synchronous intercept
producer is installed after fleet event boot and before PHP runtime boot.
The facade does not automatically create a PHP service merely because the
`php_embed` feature is enabled. The explicit `lib.rs::register(state)` convenience
function loads configuration and builds the service internally; omission of
`php.yaml` defaults to a one-thread request pool.
On workers and standalone processes it also registers `PhpScriptService` and
`PhpApplicationService` as separate managed runnables, and — with the `tasks`
feature — installs this crate's `PhpExecutor` on `TaskService` so that `php:`
tasks have an engine. `PhpScriptService` owns request-scoped HTTP execution;
`PhpEmbedRuntime` is the common non-runnable provider with its typed builder
API.

With metrics enabled registration adds only the E-PHP family. Prometheus names use
the `claviron_ephp_*` prefix; there is no generic PHP aggregate. Standalone
renders the local atomic store. Workers publish fixed-width kind `214` records
and collect fresh fleet samples. The ring is a managed `Reuse` / `Preserve`
resource; retained samples are excluded by freshness rather than master reset.
Publication cadence and freshness come from the immutable
`PhpEmbedMetricsConfig` supplied to the builder.

Named applications use the runtime-neutral `claviron-app` management plane.
`PhpApplicationController` exposes local PHP application status and lifecycle
commands; the common publisher, fleet manager, and command bridge own fleet
aggregation and routing across PHP, JavaScript, and future runtimes.
Registration retains the controller as a bootable component. Its typed
`ApplicationManager` dependency binds it to the shared manager during boot,
after all definitions are registered; PHP definitions may therefore precede
the server bootstrap without silently losing their hosted reports. The
manager's controller handle shares the existing execution services, not a
copy of their state. No request-path work or additional execution service is
introduced.
Background programs are not a PHP-specific management domain any more: a `php:`
task is listed and commanded through `TaskService`, publishes on the task
snapshot ring, and answers the task verbs. The aggregate Continuo runnables
remain available for whole-model emergency lifecycle, but a named child is not
represented as a fabricated Runnable or provider.

`PhpEmbedRuntime::Deps` declares the fleet, counter and cell producers whose
prepared handles are captured by the PHP host bridges. Compiled cache,
database and log integrations remain optional dependencies: absent providers
leave their native functions unavailable rather than preventing an unrelated
PHP application from starting. Invocation transport is required when the task
integration installs it; cache-invalidation consumption is optional and exists
only on eligible workers. The SAPI stats companion remains an explicit order
edge because it publishes a process-global table, not a consumed DI object.
Boot receives these handles rather than resolving their providers again.
`PhpEmbedRuntime::boot` builds state only.
The common provider is never runnable. The master retains it and fleet
preparation, but `register(state)` does not register either PHP execution
service there. Fleet service management never sees a master PHP execution
placement; worker/standalone policies remain local.

The script, application and invocation runnables declare the shared runtime as
their dependency, preserving the exact constructor-held runtime identity while
requiring its lifecycle preparation before execution. The invalidation consumer
declares its fleet event producer; the metrics publisher declares both its fleet
and its managed metric ring. The metric policy object is plain DI and already
retained by the publisher, so it is not a lifecycle ordering target.

1. **Declare `ini_path` → `PHPRC` and `ini_scan_dir` → `PHP_INI_SCAN_DIR`
   during cold registration, not provider boot.**
   These paths belong to the one process SAPI. `PHPRC` is in
   `claviron_runtime::env::RUNTIME_ESSENTIALS`, so the post-boot env
   scrub preserves it. `libphp.so` reads `PHPRC` the first time a
   PHP thread enters `ensure_sapi()`; later service starts do not reinitialize
   the module or select a new INI configuration.
   The declaration is process-owned and frozen by the first native execution;
   a later runtime/evaluator cannot silently replace it. Existence is checked
   before native initialization. This permits configuration evaluation before
   the provider boot phase without losing the registered INI selection.
2. **At boot, configure host bridges and cache server-scoped `CString`s.** Hostname, runner identifier,
   worker id, worker count,
   and server software are computed once into `SERVER_SCOPED_VARS`
   (`OnceLock<ServerScopedCstrs>`) — five allocations saved per
   future PHP request.

The PHP runtime's own boot does not execute scripts. Application providers may
register named applications later in the same boot phase; the application
catalog seals when its first service generation starts. PHP task definitions do
not enter this catalog: TaskService owns them and invokes the installed executor.

Request and application execution are started by **`Runnable::run`**. Both
services use `run_priority = EARLY`; HTTP listener registration must still
respect normal runtime startup ordering. PHP tasks begin when TaskService invokes
their executor. Application readiness is owned by the application script, not
inferred from the PHP thread's startup acknowledgement:

- **Check retained work before starting a new generation.** A previous bounded
  drain cannot be bypassed while that service's old accepted work is still
  executing. The request and application services each check their own pool;
  neither waits for the other's work before starting.
- **Seal the complete application catalog on first run.** Include builder entries and all
  provider contributions with their already resolved environment values. Retain
  that immutable plan for later service generations; no configuration source is
  read during service execution.
- **The `php` task executor starts one program per occurrence.** Resolve the
  configured script from the process working directory before starting it.
  Placement — local or leased slots, and how many — is the task's `scope`,
  `dispatch` and `concurrency`, decided before the executor is called. A thread handle is
  retained before waiting for PHP request startup, so startup cancellation can
  enter the same cooperative drain path without losing ownership. Instance
  startup and script failures are returned to TaskService without cancelling
  HTTP or application execution. The task thread is joined before the occurrence
  returns.
- **`PhpScriptService` starts the optional request pool.** `start_pool(...)` receives the typed
  `RequestPool` policy. On first use it spawns `min_idle`
  `claviron-php-{id}` OS threads; elastic growth can add threads later
  up to `max_size`. The idle culler first trims burst growth back
  toward `max_idle`; if `max_life` is configured, it
  later trims old idle slots down to `min_idle` and renews floor
  slots in-place. Each thread owns its own
  `mpsc<PhpJob>` receiver and parks on `blocking_recv` waiting for the
  first job after binding its PHP thread context. `ensure_sapi()` initializes PHP
  when the first request-pool, task or application thread binds, deduped process-wide via
  `OnceLock`. The bootstrap-level env scrub runs after `boot()` (before
  runnables spawn), so no PHP code ever observes the pre-scrub libc
  `environ`. Later clean service starts create fresh OS/ZTS floor threads in
  the same process-owned pool container plus generation-owned rotation, culler, and stats
  publisher tasks; they never create a second PHP runtime.
- **Own independent drains.** Request generation cancellation stops maintenance
  and closes request admission; application cancellation stops only its pools;
  task cancellation reaches only the current occurrence. Each owner preserves
  its explicit graceful deadline, and `None` waits indefinitely. Completion
  includes joining all OS threads owned by that execution owner. A successful
  stop leaves zero execution threads belonging to it while the others continue.
  If accepted work remains after an explicit deadline, the generation fails and
  retains it. No OS thread is killed or detached to manufacture success.

Managed request and application errors are reported as Continuo recoverable
service errors without terminating the process runtime or the other PHP owner.
Task occurrence errors return through the installed executor and TaskService
applies its configured attempts, restart and backoff policy. Shared provider boot
failures still fail boot. This is lifecycle isolation, not native crash or PHP
module-global isolation: all three models still inhabit the same process SAPI.

---

## 4. Per-request flow

### 4.1 Native request entry

PhpScriptAdapter and PhpApplicationAdapter consume Request<RouteBody> directly.
The engine no longer has an Axum feature, binding, Next or extractor. Edge's
pipeline/embed/php.rs prepares prefix order and runner-specific call objects;
its Script and Application futures are separate. A host without a PHP owner
installs no PHP step.

Script's consuming try_handle calls the unchanged script resolver. Unhandled
returns the original request without polling/replacing its body. Native host
dispatch passes that request onward directly; it does not use Rust application
interception capabilities. The terminal handle API keeps
its 404-on-miss behavior. Gateway statuses and text retain the former configured
entry behavior without an Axum response conversion.

Application calls its prepared PhpApplicationHandle without script resolution
or filesystem probing. A matched application owns all response statuses;
missing service/name/capacity returns 503 and never selects a Script fallback.
Existing environment preparation, declared policy narrowing, stream lifetime,
head cancellation and service-owned generations remain unchanged.

### 4.2 Script resolution — `routing/proxy.rs`

`try_fcgi_proxy` decides whether a request is PHP at all and what
script to run. Which script is a string question, never a disk one:

1. Path ends in `.php` → execute the literal script.
2. Path contains an embedded `/foo.php/extra` segment → split into
   script + `PATH_INFO`.
3. `entry_script:` is configured → route extensionless paths to it;
   dotted non-`.php` paths only join the front-controller path when
   their extension is listed in `entry_extensions`. When `try_files`
   is enabled, a real disk file still wins first and is left to the
   file handler.
4. Path ends with `/` → first `.php` from `runtime.indexes`.

Whether the *resolved* script is then stat-ed before PHP is invoked is
`validate_script`, and it is governed by `EXECUTION_IO:
OPCACHE_OWNS_FRESHNESS` — canonical text in
[`claviron-php/ARCHITECTURE.md`](../php/ARCHITECTURE.md). Short
version: a pinned entry (`entry_script`, `runner: application`) is never
stat-ed; a folder mixing `.php` with assets has no pinned entry and the stat
there is structural. Do not remove it in the name of the rule.

Optional `validate_script` runs `tokio::fs::metadata` on URI-derived
script paths only (never on `entry_script` — that's an operator pin)
to keep PHP's own "Failed opening required" 200-page from leaking
DOCUMENT_ROOT to clients.

The result is a `PhpSapiRequest` (script_name, script_file,
document_root, document_uri, request_uri, query_string, path_info)
which is handed to `PhpEmbedRuntime::execute`.

### 4.3 Hand-off to the pool — `engine/mod.rs`

`PhpSapiEngine::execute` is the single per-request HTTP execution entry point;
there is no separate `run` forwarding method. Service lifecycle runnables remain
independent from this request path.

Script HTTP execution requires the request's existing `HttpPolicy` extension.
`PhpRuntime::policy` applies the PHP route's overrides without reading listener
or host configuration. The existing transport guard is not wrapped again.
The effective `requests.max_body_size` is validated in the existing PHP body
collection/streaming code, before buffer growth or forwarding input. A
declared oversized body, or an oversized body collected before PHP starts,
returns 413. Stream errors abort PHP's input bridge if the reply already began.
There is no duplicate PHP body-size default; a supplied `off` remains valid.
Missing policy is an internal execution error, not an unlimited fallback.

Buffered requests still behave like classic SAPI/FPM page renders:
the whole request body is bounded by the effective HTTP policy, captured, and
available to PHP immediately.

Streaming requests do **not** pre-buffer the upload. `RequestBodySource`
spawns a tokio body pump that reads `RouteBody::frame()` and feeds a
small bounded channel. PHP's synchronous SAPI `read_post` callback
drains that channel from the dedicated PHP thread. This gives multipart
and large-body endpoints real request-side streaming while preserving
backpressure and the effective HTTP policy limit.

The bridge is also shutdown-aware: the async pump and the blocking
reader poll the process-wide `SAPI_SHUTTING_DOWN` flag so a graceful
drain does not leave a SAPI thread stuck forever in `read_post`.
Positive `Content-Length` is still honoured by the generic SAPI
callback; unknown-length streams delegate to the context until it
returns EOF.

Response streaming is shutdown-aware too. `BodySink::Streaming`
checks the same flag before and during bounded body sends; when
shutdown starts it returns a failed `ub_write`, which the callback maps
to PHP's connection-aborted state. PHP code that polls
`connection_aborted()` or `claviron_is_cancelled()` can then leave
its stream loop without waiting for the transport to notice.

`SapiThreadPool::dispatch`:

```rust
async fn dispatch(&self, mut job: PhpJob) -> Result<(), SapiGatewayError> {
    let n = self.threads.len();
    for _ in 0..n {
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % n;
        match self.threads[idx].tx.send(job).await {
            Ok(()) => return Ok(()),
            Err(mpsc::error::SendError(returned)) => job = returned, // try next
        }
    }
    Err(SapiGatewayError::Internal("php pool shut down".into()))
}
```

- The **per-thread** mpsc channel (capacity 256) is the serialisation
  primitive — each thread runs jobs serially, no global mutex.
- The **pool** does cross-thread dispatch: round-robin via an
  `AtomicUsize` cursor, with retry-on-dead-sender so a single thread
  panic doesn't collapse the pool.
- Backpressure is natural and per-thread: a saturated thread parks
  the caller on `tx.send().await` until a slot frees up. With N
  threads and 256 cap each, total in-flight ceiling is `N × 256`.

### 4.4 The per-thread loop — `engine/pool/worker.rs::php_thread_main`

```rust
fn php_thread_main(thread_id: u8, mut rx: mpsc::Receiver<PhpJob>) {
    ensure_sapi()?;                                  // process-wide,
                                                     // first thread wins
    unsafe {
        ext_php_rs_sapi_per_thread_init();           // PER-thread TSRM bind
        zend_update_current_locale();
    }
    THREAD_ID_CSTR.with(|cell| {                     // stash thread id for
        let _ = cell.set(CString::new(thread_id.to_string()).unwrap_or_default());
    });                                              // $_SERVER injection

    while let Some(job) = rx.blocking_recv() {
        let result = run_php_on_thread(job.runtime, job.sapi_req,
                                       job.parts, job.conn_ctx, job.request_body);
        let _ = job.reply.send(result);
    }
}
```

`ext_php_rs_sapi_per_thread_init` runs **exactly once per thread**.
The ZTS TSRM slot stays bound to that thread for the lifetime of the
worker — every job dispatched to this thread lands on the same Zend
executor / compiler globals slot, so any bailout aftermath is
contained to that slot and `php_request_shutdown` clears it before
the next job runs. A bailout in one pool thread cannot poison the
others; they keep dispatching.

Per-thread identity is stashed in a `thread_local!<OnceCell<CString>>`
and read pointer-only inside `register_server_variables` to populate
`$_SERVER['CLAVIRON_THREAD_ID']`. Same zero-allocation pattern as the
process-wide `SERVER_SCOPED_VARS`, but per-thread because the value
differs per pool slot.

Dispatch is **idle-only**. The pool does not enqueue behind a SAPI
thread whose current PHP request is still running. `SapiThread.busy`
is reserved with an atomic compare-exchange before `try_send`; the
worker clears it when the job finishes. This is intentionally stricter
than a normal per-thread queue: a long-lived SAPI request (`events.php`
SSE, slow upload, blocked DB transaction) must not create
head-of-line blocking for unrelated PHP requests. If all SAPI threads
are busy past `dispatch_queue_timeout`, the request fails fast with
`503 Service Unavailable` and `Retry-After: 1` instead of waiting
behind an arbitrary long request. Other PHP worker/gateway failures
remain `502 Bad Gateway`.

### 4.5 SAPI stats function

`claviron_sapi_stats(): array` exposes request-pool counters to PHP
code; task executions are not included in its capacity, request, or busy-slot
counts. The response includes a local snapshot for the worker process that
executed the PHP request and a fleet aggregate read from a dedicated
SAPI stats SHM table. Workers write only their own table slot; readers
fold fresh slots into totals such as `threads_current_total`,
`busy_threads_total`, and `in_flight_total`. Peak counters are reported
with intentionally narrow meanings:

The table geometry always reserves the binary's build-time fleet worker
capacity, with one slot as the minimum. It does not change with standalone,
master, or worker run mode, so the same instance can move between standalone
and supervised operation without replacing kind 243.

- `max_threads_concurrent_total` is the high-water mark for SAPI fleet
  expansion. It answers "how many SAPI threads existed at the same time?"
  `max_threads_current_total` is kept as a compatibility alias.
- `threads_created_total` is the lifetime creation counter. It answers
  "how many SAPI threads did this worker/fleet create, including initial
  floor threads, elastic growth, and replacement threads?"
- `max_busy_threads_total` is the high-water mark for PHP threads that
  were simultaneously busy. It answers "how much PHP work touched the
  pool at once?"
- `max_in_flight_total` is the high-water mark for requests already
  accepted by SAPI workers. It answers "how much work was actually in
  execution?"

These are capacity and pressure notes, not request distribution metrics;
use benchmark-side worker histograms for distribution. The fleet
aggregate includes the local worker exactly once; local counters are
descriptive, not added on top of the fleet totals. This must not reuse
application-level Orbit cache/event rings.

SAPI fleet statistics resolve current `FleetMembership` for each summary, not
the reader's bootstrap worker count. Added members become expected immediately;
removed members' fresh lanes are excluded. Unavailable membership leaves the
local sample readable but reports fleet aggregation unavailable. SHM geometry
continues to use immutable capacity. `CLAVIRON_WORKER_COUNT` in the cached PHP
execution environment remains birth-context metadata, not this live inventory.
The bespoke table is registered through the shared-memory lifecycle inventory
as `Reuse` / `Preserve`. Supervised first creation and header initialization
belong to master provider boot, which completes before workers are spawned;
workers validate and reuse the initialized table and write only their own
slots. A worker rejects a missing table instead of becoming its implicit
creator. Standalone performs both roles. Master and standalone boot do not
unlink a compatible table; per-worker timestamps and stale filtering prevent
an older run's slot from becoming a current observation while surviving
workers keep the same mapping.

Elastic grow is serialised by a small async grow lock. Growth is a
rare-path operation, but serialising it keeps the exposed
`CLAVIRON_THREAD_ID` / slot identity aligned when multiple dispatchers
notice a full pool at the same time.

### 4.4.1 Multi-thread registration invariant

`ensure_sapi()` is **process-wide** (`OnceLock`): exactly one PHP pool
thread wins `php_module_startup`. Everything that must exist in every
ZTS thread's PHP runtime must therefore be registered through PHP's
module/SAPI startup machinery, not by mutating the winning thread's
`CG(function_table)` after startup.

Load-bearing rule:

- SAPI-global PHP functions owned by claviron
  (`claviron_is_cancelled()` and feature-gated `orbit_*()`
  helpers today) belong in
  `sapi_module.additional_functions` before `run_sapi_startup`.
  `php_module_startup` then installs them as part of PHP's normal
  startup path, making them visible to requests no matter which
  `SapiThread` serves the job.
- Every `SapiThread` also runs a post-`ext_php_rs_sapi_per_thread_init`
  guard that checks the current ZTS thread's `CG(function_table)` and
  registers the same functions there if they are missing. This is the
  belt-and-suspenders part of the invariant: in ZTS, assume compiler
  globals may be thread-local until proven otherwise.
- App/user code compiled into the Claviron binary can extend the embed
  SAPI through `extension::PhpEmbedExtension`. This is a lifecycle
  adapter over `ext-php-rs`, not PHP's dynamic `.so` extension loader:
  extensions must be registered during application bootstrap before
  the PHP SAPI provider boots. The extension registry passes the
  current `AppState` to each extension during registration so PHP
  handlers can read immutable runtime identity or resolve provider
  handles at call time without depending on provider boot order.
  Function entries join `sapi_module.additional_functions`;
  per-thread hooks run after each ZTS thread binds its PHP context
  and provide thread-local setup after normal module startup. Optional
  `PhpObserver` sinks are snapshotted at this same boundary. Once SAPI
  construction begins the extension registry is frozen; late registration is
  rejected rather than creating a partially visible multi-thread runtime.
- Downstream `register_classes(module)` hooks append `#[php_class]` and
  `#[php_enum]` declarations to the existing host `ModuleBuilder`. They run once
  during SAPI construction, after built-in types and before module conversion
  and MINIT. The host retains all `ModuleAllocations`; request, task and
  application threads see the same registered types. Hooks must not replace
  lifecycle callbacks. New declarations require process replacement, not a
  route reload. No request-path registration or extra PHP module is introduced.
- Claviron owns one additional `claviron-host` module and passes it as the
  `additional_module` argument to `php_module_startup`. MINIT registers native
  handle classes such as `Claviron\Runtime\CacheHandle` before any request runs. The
  same module owns Zend's single observer registration point and dispatches
  function/error/exception events to every interested downstream sink. A sink
  panic is caught and disables that sink so Rust unwinding never crosses the
  Zend FFI boundary.
- Do **not** add new PHP-callable functions by calling
  `zend_register_functions(CG(function_table), …)` from `ensure_sapi`.
  That only observes/mutates the compiler globals for the thread that
  won process-wide startup; with multiple PHP execution threads, another
  thread may later execute the request and report "undefined function".
- If a future hook truly must patch an existing Zend function table
  entry (the `putenv` override is the current special case), re-check
  the ZTS/thread-local behaviour explicitly. The default assumption is
  "per-thread until proven process-wide".

### Native registration ownership in ext-php-rs 0.16

`PhpEmbedExtension::register_classes` receives the existing host module builder:

```rust,ignore
fn register_classes<'a>(
    &self,
    module: ModuleBuilder<'a>,
) -> Result<ModuleBuilder<'a>, String> {
    Ok(module.class::<NativeValue>())
}
```

`extension` exports `ModuleBuilder`, `php_class` and `php_impl` alongside enum
support. Upstream derives emit absolute `ext_php_rs` paths, so a downstream
crate using derives also declares a direct ext-php-rs dependency matching the
host's version/features. That is a derive/ABI dependency, not ownership of
another SAPI startup. The host still retains module metadata and allocations;
class-contribution failures identify the contributing extension. Pure contract
tests build declarations through a trait object without initializing PHP.

`FunctionBuilder::build()` returns both the entry and its owned argument table.
`extension::FunctionEntries` accepts that pair through `push` or `extend` and
retains both allocations for the process lifetime before exposing the terminated
SAPI function table. `PhpEmbedExtension::append_functions` takes this collection;
downstream extensions must not discard the returned argument table. Per-thread
ZTS fallback registration uses the same owner. Zend retains argument names and
defaults through request execution and module teardown.

The internal host module stores `ModuleAllocations` with its entry in
`StaticModuleEntry`. `ClavironSapi` owns the boxed SAPI module in the process-wide
`OnceLock`; the allocation stays stable through startup and subsequent requests.
Hand-written host function bodies use `zend::run_handler`, which converts Rust
unwinding panics into PHP errors and propagates Zend bailouts to the enclosing
execution frame. This requires `panic = "unwind"` (the release profile); an
abort-profile panic still terminates the process. The outer execution cleanup and cancellation ownership are unchanged.
The `Invocation` property remains `submitted_at` explicitly, despite the new
upstream getter naming convention.

The explicit `native_registration` integration target checks reflection metadata,
headers and host calls across two ZTS threads and a reused request context.
It boots no listener, database or shared-memory fleet. Run it with
`cargo test -p claviron-php-embed --all-features --test native_registration`.

### 4.5 Per-request lifecycle — `sapi/mod.rs::run_request`

```
init_sapi_globals(ctx)        ← populate request_method, query_string,
                                request_uri, content_length, content_type,
                                proto_num, Authorization (basic auth split)
SapiGlobals.server_context = ctx_raw
php_request_startup()
intercept::pre_execute_request_hooks()
                              ← drains queued peer broadcasts (e.g.
                                apcu_delete events) under a TSRM-bound
                                executor before the script runs, so the
                                user code sees a freshly-invalidated
                                cache. Each intercept that needs
                                request-scoped setup plugs in here.
try_catch_first {
    Embed::run_script(script)
    if bailout → bailout_handler(e)
}
php_request_shutdown(NULL)    ← also fires the SAPI deactivate callback,
                                which cleans request_info CStrings,
                                runs ctx.finish_request(), and nulls
                                server_context
```

Output is captured incrementally via `ub_write` / `flush` /
`send_headers` / `send_header` callbacks into a `CapturedResponse`
(status, `HeaderMap`, `BytesMut` body) held under a `Mutex` shared
with `ClavironContext`.

### 4.6 `$_SERVER` and env — `sapi/callbacks.rs::register_server_variables` + `claviron_getenv`

Two channels expose env to PHP, deliberately not three (matches
`feedback_no_runtime_env_reads.md` and `php_env_access_matrix.md`):

HTTP, task and application execution share one identity visitor for `$_SERVER` and
named `getenv($key)`: `CLAVIRON_HOSTNAME`, `CLAVIRON_RUNNER`,
`CLAVIRON_RUN_MODE`, `CLAVIRON_PROCESS_ID`, `CLAVIRON_WORKER_ID`,
`CLAVIRON_WORKER_COUNT`, and `CLAVIRON_THREAD_ID`.
`CLAVIRON_PHP_RUNNER` is HTTP route metadata from the request's typed
`PhpRuntime::php_runner`, emitted independently of process identity boot. It is
not synthesized for persistent task/application program contexts, which have
no route runtime; there is no generic `"sapi"` fallback. Explicit environment
values retain their existing precedence.
`CLAVIRON_WORKER_NUMBER` matches Task's worker-only identity and is absent
outside worker mode; the existing PHP `WORKER_ID` retains its zero sentinel.
The existing PHP worker-count semantics are preserved. Process ID is the
embedding Claviron process (also PHP's `getmypid()`), not a child PHP process.
Dedicated-only `CLAVIRON_PHP_PROGRAM` and `CLAVIRON_PHP_INSTANCE` use the same
visitor. Neither execution invents `CLAVIRON_TASK_*` metadata. Named getenv
resolution is overlay, explicit application environment, identity, then runtime
essentials. Identity is not written to libc or bulk `getenv()` / `$_ENV`.

| API                       | Source                                       | Vhost values? |
|---------------------------|----------------------------------------------|---------------|
| `$_SERVER['KEY']`         | `register_server_variables` callback         | ✅ |
| `getenv($key)`            | SAPI `getenv` hook → overlay → vhost env → whitelist | ✅ |
| `getenv()` (no arg)       | libc `environ` (post-scrub)                  | ❌ runtime essentials only |
| `$_ENV[…]`                | libc `environ` at `php_request_startup`      | ❌ same |
| `putenv(…)`               | function-table override → per-request overlay | overlay (libc untouched) |

The built-in `putenv` handler in `CG(function_table)` is swapped at
`ensure_sapi()` time (`override_putenv_handler`) so PHP-level env
mutations stay in the per-request `env_overlay` and never bleed to
libc or to subsequent requests. Mirrors PHP-FPM's default
`clean_env=yes` semantics, except scoped per request rather than per
pool.

TLS is terminated by Rust before PHP runs. `register_server_variables`
therefore translates `ConnCtx::tls` into standard CGI/mod_ssl-style
`$_SERVER` variables (`HTTPS=on`, `SSL_PROTOCOL`, `SSL_CIPHER`,
`SSL_CLIENT_VERIFY`, `SSL_CLIENT_S_DN`, `SSL_CLIENT_I_DN`) instead of
expecting libphp to discover TLS state itself.

Script CGI projection derives `REQUEST_SCHEME`, `HTTPS=on|off` and
`HTTP_X_FORWARDED_PROTO` from accepted transport context. Missing context
does not invent HTTPS; shared PHP filtering excludes client forwarding
scheme claims. SSL-specific fields remain TLS-only. Persistent application
messages expose the same `REQUEST_SCHEME`/`HTTPS` in their `server` map,
while preserving the raw `uri` and `headers` envelope; framework adapters
must use the trusted server map for scheme, not trust arbitrary raw headers.
Task execution still has no synthetic HTTP metadata.

Unlike claviron-owned PHP functions, this is an intentional function
table patch rather than a new registration. Treat it cautiously in
multi-threaded mode: if its behaviour ever differs across
`CLAVIRON_THREAD_ID`s, move the hook into a startup path that PHP
applies to every ZTS thread.

### 4.6b How the three models are typed

Three execution models exist — script, task, application — on two axes that
are not the same axis, and saying which is which is the contract:

| | script | task | application |
| --- | --- | --- | --- |
| **Model** — what Rust started on the thread | `PhpExecution::Request` | `Program`, `ProgramModel::Task` | `Program`, `ProgramModel::Application` |
| **Current unit** — what a budget may wrap | the request itself | none, ever | `PhpWorkUnit::Request` or `::Event`, per `receive` |
| **On HTTP** — `is_on_http()` | always | never | while it holds a `Request` |

The axes coincide only in the script model, where one execution *is* one
request. That coincidence is why `PhpExecution::Request` reads like a model
name; it is not one.

Two things follow, and both are load-bearing:

- **A task is a program whose `current` is never `Some`.** Nothing else
  states it. The distinction used to be spelled four ways at once — a
  `thread_kind: &str` of `"dedicated"`, a `whole_program_is_one_execution:
  bool` on `run`, and two flags on the application inbox — each in its own
  vocabulary, none of them a type.
- **An application's `is_on_http` is a property of the message, not of the
  program.** It flips on every `receive` and back on every `respond`, which is
  exactly why it cannot live on `PhpExecution`: that is built once per program.

PHP sees the model as `$_SERVER['CLAVIRON_PHP_MODEL']`, beside
`CLAVIRON_PHP_PROGRAM` and `CLAVIRON_PHP_INSTANCE`, and asks the dynamic
question with `claviron_is_on_http()`. Neither is available from
`php_sapi_name()`, which is one string per process and reads `"claviron"` for
all three.

### 4.7 PHP task execution — `engine/task.rs`

A `php:` entry in `tasks.yaml` is a normal TaskService task. TaskService owns
its timing (`daemon`, cron, interval or manual), scope, dispatch, concurrency,
attempt budget, backoff and graceful deadline. This crate installs the PHP
executor and owns only how one `TaskExecution` occurrence reaches the embedded
engine.

`TaskProgram` is the private engine input assembled from that occurrence: task
name, script path, arguments, working directory and resolved environment.
`TaskExecutions` owns the dedicated OS/ZTS thread and its cancellation and join
handles for the lifetime of the occurrence. Neither type schedules, repeats or
daemonizes a script.

```text
TaskService timing/placement/retry
  -> one TaskExecution occurrence
     -> TaskProgram
        -> TaskExecutions
           -> one dedicated OS/ZTS thread
```

For daemon timing, TaskService starts one occurrence and the script remains
resident only if its own execution remains alive. For cron, interval and manual
timings, natural script completion ends that occurrence; a later trigger creates
a fresh one. A Laravel scheduler task can therefore deliberately use daemon
timing to preserve one framework boot and run sub-minute events, without making
the PHP engine itself daemon-specific.

Every task thread uses the shared `bind_php_thread` prologue: process-wide SAPI
initialization, per-thread TSRM initialization, host function registration,
intercepts and downstream extension hooks. It uses
`task:<program>:<instance>` as `CLAVIRON_THREAD_ID`, reports
`CLAVIRON_PHP_MODEL=task`, and exposes `CLAVIRON_PHP_PROGRAM` and
`CLAVIRON_PHP_INSTANCE` in `$_SERVER`. It fabricates no HTTP or TLS metadata.

PHP still reports `php_sapi_name()` as `"claviron"`, because one process-wide
SAPI is shared by request, task and application execution. A console-shaped task
must therefore provide any framework-specific console marker, such as Laravel's
`APP_RUNNING_IN_CONSOLE`, through its task environment.

Arguments are literal; no shell parses spaces, variables or metacharacters.
`$argv[0]` is the resolved script path and subsequent values are the configured
arguments. Both PHP globals and `$_SERVER['argv']`/`$_SERVER['argc']` are
populated even when `register_argc_argv` is disabled for HTTP. The task thread
also enables `CompilerGlobals::skip_shebang`, so console entry files may retain
their shebang without emitting it as output.

The task block's shared `log:` policy owns console output. `echo` and PHP's
`STDOUT` use its stdout target; `STDERR` and SAPI error messages use its stderr
target. Relative targets resolve against the task `cwd`. These are task-local
writers rather than process descriptor redirection: multiple PHP task threads
may write different files safely inside the same Claviron worker. Application
programs remain request servers and do not acquire this configurable console
surface. Their SAPI warning/error records stay on tracing with structured
`runtime`, `application`, and `instance` fields; the process logging layer adds
worker identity and delivers them to journald under systemd. The Claviron SAPI
forces `log_errors=1` and clears PHP's process-global `error_log` file target;
otherwise PHP writes directly to that file and bypasses the structured SAPI
logging callback.

One Zend request startup/shutdown pair wraps the entire task occurrence. Normal
completion, `return`, `exit()` and `exit(0)` are success; nonzero exit and script
errors are failure. The OS thread is joined before the occurrence returns.
TaskService then applies the task's restart, attempts and backoff policy.

`claviron_is_cancelled()` observes the occurrence's cancellation token.
Cooperative scripts return on cancellation. When an explicit graceful deadline
expires, the executor interrupts only PHP executions owned by that occurrence;
a thread blocked in a native call may still be unreachable. Handles remain
owned until thread teardown completes. Request-pool and application lifecycle
remain independent.

PHP task fixtures live under `tests/fixtures/task/`. The isolated lifecycle test
materializes them in a temporary directory, exercises task/application/request
isolation, and verifies natural completion, cancellation, interrupt and joined
teardown behavior.

#### 4.7.1 On-demand PHP program invocation — `invoke`

The optional `tasks` feature also exposes
`orbit_invoke(string $operation, mixed $payload): string`. Publication uses
the dedicated Orbit invocation ring and returns the mandatory `InvocationId`;
it does not use `ApplicationEvent`, wait for execution or imply success. The
runtime-neutral transport carries an operation, opaque bytes and `OrbitEpoch`
submission time. This crate owns the PHP `EventValue` codec and the
`claviron.php.program.invoke.v1` executor.

That executor expects a framework-neutral payload containing `kind`, an
absolute `script`, literal `argv`, optional `cwd`, and string `env`. `operation`
selects the executor while `kind` identifies the submitted work (for example
`job`). The `job-bridge` TaskService task owns fleet placement;
`PhpInvocationService` owns only admission, per-invocation ZTS thread lifetime
and drain. Linux and
FreeBSD wait on the invocation ring's process-local `eventfd`; other targets
use bounded fallback polling.

With `tasks` enabled, `php.yaml` exposes `launch.log` using the existing task
output policy (`inherit` by default, a file path, split stdout/stderr targets,
or a declared channel). File targets always append. A relative path such as
`storage/logs/invoke.log` is resolved separately against each invocation's cwd,
falling back to the script's parent when cwd is omitted; it is not resolved at
service boot against the bridge's directory. An absolute target deliberately
shares one file across application roots. This routes only native invocation
stdout/stderr: framework-owned logging and explicit framework output captures
keep their own destinations. Runtime start, completion, failure and drain
diagnostics remain in the server log. The plan is immutable until process
replacement, like the other PHP execution settings.

This remains fire-and-forget. A consumer may publish a normal application event
such as `TaskCompleted` carrying `CLAVIRON_INVOKE_ID`, but completion, response,
claim, retry and durability are not part of the invocation primitive.

---

### 4.8 Persistent HTTP applications

`PhpApplicationService` is the third execution model. It shares process SAPI,
INI/extensions and the reserved-thread script prologue with the other services,
but owns named, **worker-local** fixed application instances. It always runs `each`: no master
PHP execution, Orbit slot lease, cross-process queue, or external `octane:start`.

```rust
use claviron_php_embed::{PhpApplication, PhpEmbedRuntime};

let runtime = PhpEmbedRuntime::builder()
    .application(PhpApplication::new("web", "php/application.php")
        .instances(8)
        .max_waiting(64)
        .environment("APP_ENV", "production"))
    .build();
```

Providers can also call `state.php_embed()?.register_application(...)` during
boot. Applications use a first-run catalog seal; later changes are rejected
even while a service is stopped. A `php:` task is named in `tasks.yaml` and
occupies no catalog here. Configuration-backed registration is
still library-owned:

```yaml
applications:
  web:
    script_path: php/application.php
    args: []
    env_file: etc/application.env
    env:
      APP_ENV: production
    instances: 8
    max_waiting: 64
    retry:
      backoff: 1s
      backoff_max: 60s
      max_attempts: null
```

`PhpApplication::instances` declares a fixed slot set. It has no dependency
on `RequestPool`, idle floor/ceiling, rotation, growth demand, or a maintenance
interval. `ApplicationWorkers` exclusively owns the native handles and fixed
slot vector on the lifecycle side. `ApplicationRequests` owns only admission,
atomically published inboxes, request wakeups and cancellation. It cannot spawn,
join or resize instances and contains no lifecycle mutex.

The route-bound `PhpApplicationHandle` follows atomically published generation
replacements; the standard adapter binds it during route construction. HTTP
never looks up an application name or takes the service catalog lock. The
explicit name-based service API remains available for dynamic callers. The
request body remains typed `Bytes`: end-of-stream GET/HEAD takes no body poll,
a single frame needs no buffer copy, and multiple frames are joined with the
route's body limit and cancellation still enforced. Content decoding remains
application-owned; the Zend boundary performs the required ownership copy.
The Tokio runtime handle and streaming cancellation context belong to the
instance, rather than being cloned into every buffered request.

Only lifecycle startup, an explicit replacement, or failed-instance `retry`
starts execution. Thread exit wakes the lifecycle owner after the receive loop
and PHP teardown have returned. The native join runs off Tokio's worker threads
and publishes its result before notifying completion. Declared backoff sleeps
until the next failed slot's retry; there is no periodic scan or drain polling.
A stopped generation closes admission and its strong inbox senders. Transient
HTTP senders cannot outlive delivery; accepted work and framework cleanup drain
before native handles are released. Admission release during stop also wakes
the owner, including cancelled body readers and dispatch waiters.

Route policy remains authoritative for HTTP ingestion and reply delivery.
`execution_timeout` and `retry` stay application-owned because events share the
same instances without an HTTP route. Saturation or stopped execution returns
503; an interrupted or failed request is never automatically replayed.

The explicit, binary-safe PHP protocol is:

```php
claviron_application_receive(): array|\Claviron\Runtime\Event|null;
// request: method, uri, protocol, headers (name => list<string>), body,
//          server (available Claviron connection metadata only)
claviron_application_respond(array $response): void;
// response: status (200..599), headers (name => list<string>), body (string)
```

Only application threads may use these functions. PHP boots its application
before the first receive; that call makes the instance ready. Responses preserve
binary bodies and repeated headers. Invalid status/header/body values throw,
and missing/duplicate responses are protocol errors. Native bridge functions do
not call PHP callbacks across Rust frames. The PHP adapter owns the loop and
`try/finally` cleanup; see `examples/php/application.php`. After responding, the
slot stays busy until PHP calls receive again, so post-response cleanup cannot
overlap the next request. On shutdown receive returns null; active handlers can
also poll `claviron_is_cancelled()`. Uncaught exceptions/exit release the
pending response with a gateway failure, never automatic request replay.

This is **not** transparent SAPI worker mode: `$_GET`, `$_POST`, `$_COOKIE`,
`$_SERVER`, `$_FILES`, `php://input`, `header()` and `echo` are not remapped per
message. Script output remains stdout. A framework adapter builds its request
from the explicit envelope, performs its own multipart/session/request-scoped
cleanup, and returns the explicit response. Laravel/Octane and Symfony adapters
are above this primitive; no stock Octane compatibility is implied. The same PHP
execution remains alive, so globals/statics, environment overlays and registered
shutdown callbacks retain normal long-lived-script semantics. Streaming and
protocol upgrades are not supported by this first buffered contract.

## PHP events

Framework-neutral PHP event primitives use Claviron's existing typed process
bus and the registered Orbit fleet bridge; they do not introduce a second IPC
or inbox protocol:

```php
use Claviron\Runtime\Event;

claviron_event_subscribe_many(['orders.changed', 'users.logged-in']);
claviron_event_publish('orders.changed', ['id' => 7]);

// Explicit cross-runtime/shared topic; adapters must register its schema.
claviron_event_publish_shared('audit.changed.v1', ['id' => 7]);

$event = claviron_event_receive(1_000);
if ($event instanceof Event) {
    // $event->topic and the native $event->payload are immutable properties.
}
```

Subscriptions are execution-scoped. A one-shot SAPI request drops its topics
at request shutdown, while task and application programs retain them for the
life of that PHP execution. Events have fire-and-forget, bounded, no-replay
semantics; they are not a durable job queue or an acknowledgement protocol.
Lag is reported as an exception instead of being silently treated as complete
delivery. Both forms use the fleet ring; there is no PHP-visible local/fleet
transport switch. `claviron_event_publish()` is private to the current named
PHP application and its runtime-qualified target is envelope metadata.
`claviron_event_publish_shared()` deliberately omits that target for an
explicitly registered cross-runtime topic. Rust rejects a non-matching target
before PHP sees it, while subscriptions select topics within the accepted
audience.

Topic and runtime-qualified target selection run synchronously at process-bus
publication, **before** admission to the execution's bounded receiver. Traffic
for an unsubscribed topic or another application cannot consume that inbox or
cause its lag. Adding a topic admits only subsequent publications; it never
replays retained process or fleet history. The ordinary process channels and
Orbit wire protocol are unchanged; there is no forwarding task or polling loop.
Both explicit task/script receive and persistent application's multiplexed
HTTP/event receive use this same subscription primitive. A queued or deferred
event is checked again at delivery so an unsubscribed topic is not delivered.

`php.yaml` owns `events: {capacity: 64}` (the existing default), and the Rust
builder accepts `PhpEventConfig` through `.events(...)`. This is the backlog
per subscribed PHP execution, not a type count, application-wide total or
per-second quota. Only matching events count. Zero and values above Tokio's
capacity range are rejected before engine boot; storage rounds up to the next
power of two. Configuration is pinned at process boot, as with the rest of
the resolved PHP plan. Matching overflow still raises the receive exception:
capacity is not increased automatically and missing events are not swallowed.
Size it from the peak matching arrivals while the consumer is unavailable,
including framework boot, HTTP/event processing and scheduler work, rather
than from the total fleet publication rate.

Orbit owns the shared native value format. PHP converts `null`, booleans,
integers, floats, binary strings, and ordered integer-or-string keyed arrays
losslessly at the extension boundary. JavaScript projects the same format to
its native JSON-shaped values. Laravel, Symfony, or another downstream adapter
still owns the mapping between framework event classes and stable application
topic names; the native runtime does not know those framework classes.

Once a persistent application subscribes, its existing
`claviron_application_receive()` wait multiplexes HTTP requests and events. It
returns request arrays for HTTP and `Claviron\Runtime\Event` for event delivery,
always on the owning PHP/ZTS thread. No Rust callback invokes PHP userland.
Task and script executions receive explicitly through
`claviron_event_receive(timeout_ms)`; zero performs a non-blocking poll.

`Claviron\Runtime` is reserved for these native contracts. Composer packages
can share the open `Claviron` namespace under framework-specific prefixes such
as `Claviron\Laravel` without colliding with native class names. The guarded
declarations in `php/runtime-types.php` keep Composer and IDE tooling usable
under ordinary PHP; Claviron's already-registered native types win at runtime.

Rust routing selects a name via `PhpApplicationService::execute(name, request)`
or an axum-free `PhpApplicationAdapter`. Config-driven vhosts select that same
name in their existing PHP list:

```yaml
php:
  - path: /live/
    runner: application
    application: live
  - path: /
    runner: script
    entry_script: index.php
```

Edge prepares the native Application owner at its longest segment-safe prefix.
Multiple application routes can use the same named application or different
named applications; a route never creates another instance set.
The prefix is segment-safe (`/live` and `/live/x`, not `/lively`). The entire
matched subtree belongs to the application, including `.php` and dotted paths,
with the original URI/query preserved. No script resolution, `try_files`, or
prefix stripping runs here. Missing service, unknown name, stopped pool or
exhausted capacity returns 503 rather than falling through to the SAPI/FPM
fallback. The engine has no Axum middleware boundary.
The adapter holds a service handle, so stop/start does not leave a stale pool
generation in the router. Route reload can change selection among registered
applications; it does not reload `php.yaml` or create a new application catalog.

Application route configuration rejects `environment` (aliases `env`,
`environments`) and `env_file` overrides through `PhpConfig::validate`, before
the route enters a host snapshot. Site request variables must not be presented
as an environment override that PHP `getenv()` does not observe. Configure
environment sources on the named application's boot definition in `php.yaml`.
Script routes retain their existing request-scoped SAPI environment handling.

Application adapters and direct HTTP execution also require the supplied
`HttpPolicy`; they do not invent an unlimited policy. The adapter applies its
route overrides, and execution validates size while collecting input before PHP
receives the message. Explicit `off` is accepted, and oversized input returns
413 before application code runs. Background events, tasks and console programs
do not require an HTTP policy.

A different boot profile requires a different application definition. One named
application retains one fixed instance set and one start/stop/restart management identity
across all its sites. Routing, transport and request/response policy remain
site-owned; application boot environment and capacity remain application-owned.

`engine/application.rs` owns the PHP receive/respond protocol;
`engine/application/request.rs` owns typed inbox delivery and admission;
`engine/application/instances.rs` owns fixed slots, failed-instance replacement
and retained native handles. `functions/application.rs` owns the Zend message
boundary; `service/application.rs` owns the independent Runnable generation. `TaskService` remains responsible for task fleet placement.
Failed fixed application slots retry after join with exponential backoff capped by
`backoff_max`. `max_attempts` limits consecutive attempts without completed
requests; null retries indefinitely. A completed request resets that failure
streak. Exhausted slots remain unavailable until service restart, exposed in
`PhpApplicationService::snapshot()` alongside ready/busy/backoff/retiring counts.

Stopping closes admission, wakes blocked receivers/body collectors, signals
script cancellation and joins every thread, including retiring threads. Grace
expiry retains ownership and rejects a new generation until old work is fully
drained. Max capacity includes retiring OS threads: rotation never overlaps an
old slot with its replacement. Other PHP services are unaffected. These are
lifecycle boundaries, not isolation from native extension crashes.

Private unit tests in `tests/application.rs` cover typed/YAML validation, catalog
sealing, persistent state, binary request/response, repeated headers, HEAD,
body limits, elastic growth/culling, worker failure/retry, rotation and joined
stop/restart including an incomplete bounded drain. Native tests run in an
isolated subprocess and assert that the child actually executed its test.

### 4.9 Named application management

`PhpApplicationService` owns execution and joined teardown. Its manager is a
domain handle, not a Runnable. `list` and `status` return fleet observations
grouped by configured name, with one placement per reporting serving worker.
Placements include pool capacity/ready/busy/retirement state, desired enabled
state, process-local generation, lifecycle, last error and observation time.

Commands report publication acceptance. A caller observes convergence through
a later snapshot: stop preserves the definition and drains only that name,
start recreates it, and restart drains then increments its process-local
generation. Generic service stop remains intentionally broader and causes that
worker's snapshot to expire until the aggregate service starts again.

Background programs are not managed here any more. A `php:` task is listed,
stopped, started, restarted and run through `claviron-client`'s task verbs like
any other task, and its placements are task placements. The former background-program
manager, its fleet snapshot ring and its five client verbs were removed on
2026-09-13 — they described the same three things twice, in a second
vocabulary, for one of the three executor kinds.

## 5. Performance — measured against FPM

> **Historical, unreproduced.** The numbers below were measured once,
> against an application and a harness that no longer live in this
> repository, and nothing here can reproduce them. Read them as the reason
> the multi-thread engine was built, not as a claim this tree still meets.
> The one benchmark that *is* reproducible evidence is
> `lumenal/HARMONY_5M_REPORT.md`, and it measures something else — a mixed
> Rust/PHP causal stream, not throughput against FPM. Quote that one for
> performance claims; quote this table only with this paragraph attached.

Real-world workload (Laravel + Eloquent + Postgres `SELECT … FOR UPDATE`
row lock per request, 10 user_ids × 50 concurrency × 2000 requests over
HTTP/2). Single claviron worker process for SAPI; FPM `pm.max_children=64`
with `pm.start_servers=8`, `max_size=24` per claviron worker.

| metric        | FPM (15 child, 27 conn) | SAPI t=4 | **SAPI t=8** |
|---------------|------------------------:|---------:|-------------:|
| throughput    |               194 req/s | 166 req/s | **308 req/s** |
| p50 latency   |                   171ms |    292ms |   **154ms** |
| p95 latency   |                   733ms |    314ms |   **249ms** |
| p99 latency   |                  1353ms |    615ms |   **267ms** |
| max latency   |                  2581ms |    743ms |   **292ms** |
| processes     |                      15 |        1 |           1 |

SAPI t=8 vs FPM: **+58% throughput**, **5× tighter p99**, 9× tighter
max — single-process, single libphp.so. Lock contention happens
at the Postgres row level on both stacks; the runtime's job is to
not add its own queueing on top of that.

### 5.1 Why the multi-thread engine wins

- **Shared opcache + JIT.** Laravel boot is amortised across all
  threads in one process. FPM's 15 children each warm their own
  opcache + class table.
- **Shared connection pools.** PDO persistent connections live
  per-thread (TSRM), but threads share the libpq client cache and
  opcache-resolved class hierarchy. FPM has 15 disjoint copies.
- **Lower DB concurrency, naturally.** 8 PHP threads → at most 8
  in-flight `SELECT FOR UPDATE` attempts; SAPI's `max_size=24` lets
  24 fight for the same row. Fewer concurrent waiters → shorter
  Postgres lock queues → more consistent tail.
- **No HOL on the accept socket.** FPM workers wait on a shared
  accept socket; a slow request occupies an entire child. SAPI
  workers wait on per-thread mpsc; the dispatcher round-robins
  around any thread that's currently blocked.
- **Bailout containment, per-thread.** A Zend bailout in thread X
  poisons only thread X's TSRM slot, cleared on the next
  `php_request_shutdown` for that thread. Other threads keep
  serving. Pre-pool history (single global thread driven from
  Tokio's `spawn_blocking`) had the failure modes that motivated
  the dedicated-thread design — bailout poisoning across pool
  threads + TSRM rebind crashes — and those constraints carry
  through: each pool thread is itself a *dedicated* PHP thread,
  one TSRM slot for life, mpsc-serialised. The pool just gives us
  N of them.

### 5.2 Pool sizing rule of thumb

`threads ≈ concurrency / target_queue_depth`. From the same
benchmark:

| threads | queue / thread | observed     |
|--------:|---------------:|--------------|
|       4 | 12.5           | starves; min latency 264ms (queue wait, not work) |
|       8 | 6.25           | sweet spot for this workload |
|      16 | 3.1            | (untested — diminishing returns expected before ZTS contention bites) |

Concrete rule: pick `threads` such that the typical request's queue
wait is well under its own work time. For row-lock workloads, fewer
threads is often *better* because admission control at the runtime
layer keeps DB lock queues short.

Production caveat: every loaded extension must be ZTS-safe. opcache /
mysqli / redis / apcu are generally fine; audit any 3rd-party
extension before raising N. There is no runtime allow-list gate today:
the typed request/application capacities are honoured directly, and task
concurrency is honoured by TaskService. This remains an
embedding-application/operator responsibility.

---

## 6. Components — one-line summary

The crate is organised by domain into focused subtrees. Top-level files carry
crate-wide concerns (config, errors, metrics); folders group related files.

### 6.1 Top-level

| File          | Responsibility                                                |
|---------------|---------------------------------------------------------------|
| `lib.rs`      | Public typed API and `PhpEmbedStateExt` exports, HTTP compatibility exports, config-backed registration entry |
| `config.rs` | Typed-option re-exports and shared construction defaults |
| `config/pool.rs` | `RequestPool` capacity, timeouts, defaults and validation |
| `config/task.rs` | Private `TaskProgram` engine input assembled for one PHP task occurrence |
| `config/file.rs` | Single library-owned synchronous file reader for configured registration |
| `config/file/schema.rs` | Strict sectioned YAML schema, raw environment declarations, private desired plan and typed-builder composition; no live state |
| `error.rs`    | `SapiGatewayError`                                            |
| `extensions`| Public compiled-in extension registry; boot-time function/per-thread hooks, observer contribution, enum/ext-php-rs re-exports, and registry freeze contract |
| `observer.rs` | Public `PhpObserver` sink API, frozen snapshot and function/error/exception fan-out; attaches callbacks to the SAPI-owned module builder |
| `metrics/hooks.rs` | RAII guards and hot-path counter helpers (feature-gated) |
| `metrics/config.rs` | Public `PhpEmbedMetricsConfig` and immutable typed provider; no file reader or reload capability |
| `metrics/store.rs` | `PHP_EMBED_METRICS`, snapshot merge rules, Prometheus formatter |
| `metrics/orbit.rs` | Exact-width kind `214` Orbit codec                       |
| `metrics/provider.rs` | Local/fleet family, worker publisher, managed reusable metrics ring |
| `build.rs`    | `php-config` lookup → `cargo:rustc-link-search` for `libphp.so`; binary linker flags such as `mold` live in the consuming binary package (`crates/app/build.rs`) |

### 6.2 `engine/` — request orchestration + thread pool

| File          | Responsibility                                                |
|---------------|---------------------------------------------------------------|
| `engine/mod.rs` | `PhpSapiEngine` façade, request/job types, execution dispatch, `run_php_on_thread`, shared `ensure_sapi` startup and error conversion |
| `engine/context.rs` | `ClavironContext`, `PhpExecution`, `PhpRequestContext` and the concrete `ServerContext` implementation for both execution modes |
| `engine/body/request.rs` | Buffered/streamed request input, bounded body collection and Content-Length validation |
| `engine/body/response.rs` | `CapturedResponse`, buffered/streamed output sinks, one-shot header commit and HTTP response-body bridge |
| `engine/pool/mod.rs` | `SapiThreadPool`, slot snapshots, idle dispatch, elastic capacity, rotation, idle culling and joined drain |
| `engine/pool/worker.rs` | Request slot ownership, OS-thread spawning, per-job loop and in-flight guard |
| `engine/thread.rs` | Shared thread identity, `bind_php_thread` and `PhpThreadContext` teardown; used by request-pool, task and application threads (§4.4.1) |
| `engine/task.rs` | PHP task occurrence OS-thread ownership, startup acknowledgement, completion reporting, cancellation and joined drain |

### 6.3 `sapi/` — SAPI FFI harness + concrete callbacks

| File              | Responsibility                                                |
|-------------------|---------------------------------------------------------------|
| `sapi/mod.rs`     | `ServerContext` trait, `run_request` / `run_request_with_startup`, `build_sapi_module`, raw FFI shims to libphp.so symbols ext-php-rs doesn't re-export (`php_request_startup/shutdown`, `php_module_startup/shutdown`, `sapi_startup`, `php_register_variable_safe`, etc.); `register_variable` / `register_variable_cstr` helpers |
| `sapi/callbacks.rs` | claviron's concrete SAPI callbacks (`ub_write`, `flush`, `send_header(s)`, `read_cookies`, `log_message`, `claviron_getenv`, `register_server_variables`); `ClavironSapi` module wrapper; `SERVER_SCOPED_VARS` cache; `init_server_scoped_vars` |
| `sapi/module.rs` | Single native Zend module entry, native-class registration and MINIT startup; observer callback wiring is delegated to `observer.rs` |

### 6.4 `intercept/` — function-table replacement handlers

| File                   | Responsibility                                                |
|------------------------|---------------------------------------------------------------|
| `intercept/mod.rs`     | `pre_execute_request_hooks()` — fired from `sapi::run_request` after `php_request_startup`, before script execution. Each intercept that needs request-scoped setup plugs in here. |
| `intercept/apcu_delete.rs` | Replaces `apcu_delete` in `CG(function_table)`. Captures call → broadcasts via `crate::invalidation::bus` → delegates to APCu's stock handler. Apply queue + replay guard for peer-broadcast events. |
| `intercept/putenv.rs`  | Replaces `putenv` in `CG(function_table)`. Routes writes into the per-request `env_overlay` on `ClavironContext` (libc `environ` untouched). Mirrors PHP-FPM's `clean_env=yes` semantics per request. |

`opcache_reset` is intentionally still broadcast-capable, but treat
it as the riskiest replication intercept: whole-opcache reset is a
coarse deployment control-plane action, not a request hot-path cache
operation. Peer replay is coalesced with a single pending bit per
worker: multiple reset broadcasts before the next PHP request entry
apply as one local `opcache_reset()`. If active SAPI traffic still
shows thread instability around peer reset replay, disable
`opcache_reset` propagation first and keep targeted
`opcache_invalidate($file)` as the deploy-safe path.

#### 4.7.2 Serving invocations from PHP

The same feature exposes the consumer half. A persistent program subscribes
to operations and takes what the fleet submits under them:

```php
orbit_invoke_subscribe(string $operation): bool;
orbit_invoke_unsubscribe(string $operation): bool;
orbit_invoke_receive(int $timeout_ms = 0): ?\Claviron\Runtime\Invocation; // id, operation, payload, submitted_at
```

Subscriptions belong to the execution, like event topics, and each holds its
own ring cursor: the ring is a broadcast, so two programs subscribed to one
operation both receive every invocation — work claiming is the caller's.
`payload` is the `EventValue` the submitter passed; bytes in another codec
come through as a binary string. `timeout_ms` of zero is one poll of the
ring; a positive value parks the thread, and the program's cancellation ends
the wait. The subscription is created inside the Tokio runtime's context
because the ring's readiness fd is registered with it.

A daemon that takes one invocation at a time can bound that unit with the
engine's own budget: `claviron_budget_begin(int $seconds)` arms what the
engine arms around a script, so PHP raises the ordinary "Maximum execution
time of N seconds exceeded" fatal, and `claviron_budget_end()` disarms it.
PHP's own `max_execution_time` timer is not available for this: request
startup cancels it (§SAPI), because on a threaded host its signal lands on
whichever thread the kernel picks.

#### Task input multiplexing

With `tasks`, `claviron_task_receive()` waits for either an execution-scoped
event or invocation subscription and returns the existing native `Event` or
`Invocation` object. It is task-only: HTTP requests and persistent application
receivers retain their own boundaries. The typed Rust `TaskInput` selects the
two existing receive futures together with the owning task cancellation token;
no additional inbox, thread, polling cadence or scheduler is introduced. `null`
means cancellation, never an idle timeout. At least one subscription is required.
Transport lag and closure remain errors. TaskService still owns placement,
execution, retry and drain; receiving a message does not create another task.
On platforms lacking native invocation readiness, the underlying transport's
existing fallback remains; this adapter adds no new poll loop.

#### PHP native type namespace

`Claviron\Runtime` identifies PHP types implemented by the Rust runtime, not
ordinary PHP integration classes. Native declarations and Composer fallback
declarations must use the same fully qualified name. A fallback supplies type
information outside the embedded runtime; it does not implement the native
capability. Event and invocation objects are runtime-delivered messages, while
Orbital is a handle to runtime-owned shared state. All three belong to this
namespace despite their different roles.

The native handles `CacheHandle`, `PostgresConnection`, `PostgresCursor`, and
`MySqlConnection` also live under `Claviron\Runtime`. Their native registration,
fallback declarations, function signatures, PHP consumers and tests use the
same names; the former global names are not compatibility aliases.
Laravel adapters, providers and other PHP-implemented integration classes retain
their own namespaces. Global host function names are a separate API convention.

#### 4.7.3 `Claviron\Runtime\Orbital` — a fleet cell as a PHP value

`orbit-cell` gives the fleet typed atomic cells addressed by id; this class is
their PHP handle. `Orbital::int()`, `::float()`, `::bool()`, `::string()` allocate
a cell with the default value `0`, `0.0`, `false`, or `''`; an explicit initial
value is optional. `::open($id)` reattaches. Claviron prepares the fleet's common
cell tables before the PHP engine starts; application code does not register a
cell provider or create a table. Allocation failure throws, never returns a
handle that only pretends to be shared. The value is never on the
object: `$n++`, `$n += 5`, `$s .= "x"` apply atomically to the cell through
the class's own Zend handlers (`do_operation`, installed once at module
startup beside `compare` and `cast_object`) and hand back the *same* handle,
so `$b = $a + 1` is `$a` with one added, not a copy. `$n > 10`, `(int) $n`,
`echo $n` read the cell. `serialize()` carries only the id: a cell crosses a
cache, an invocation or a session by reference and reads whatever it holds
when the handle comes back. `release()` retires the cell for every handle.

Two things ext-php-rs does not offer, done by hand in `install_handlers`: the
class is made serializable again (ext-php-rs denies it for any class with a
Rust body), and `create_object` builds an initialised empty handle so
`unserialize()`, which never calls a constructor, has a `$this` for
`__unserialize`. One thing PHP's ABI demands: `cast_object` receives an
uninitialised stack zval, and ext-php-rs setters release what a zval held
before, so the target is zeroed before it is written. Skipping that freed
stack garbage and corrupted the heap; the crash surfaced requests later.

#### Embedded Interception: Lazy Invalidation

APCu and OPcache are PHP runtime-owned caches, not Claviron-owned
shared-value stores. The embed intercepts therefore use lazy
invalidation / invalidation propagation rather than value replication:
workers keep their own local PHP cache state, while Claviron broadcasts
small stale signals through the SHM bus (`delete this APCu key`,
`clear APCu`, `invalidate this OPcache file`, `reset OPcache`). The
receiving worker applies the signal inside its next PHP request
context, where PHP functions are safe to call.

### 6.5 `functions/` — claviron-owned PHP functions

`claviron_config(string $key, mixed $default = null): mixed` reads the existing
AppState Congif catalog, independently of Laravel or any other PHP framework.
The runtime retains the reader handle during registration, not a second config
snapshot; each lookup pins one current catalog generation. Only explicitly
exported `Configurable::export_config` fields are visible under the owner's
namespace. It performs no filesystem IO, parsing, environment reads or JSON
round-trip. PHP receives a detached native value, not a mutable runtime handle.
Missing namespaces/keys return the supplied default (null when omitted), while
configured null/false/zero remain themselves. Invalid keys, an unbound reader,
and values not losslessly representable in PHP throw rather than using default.
Unsigned integers beyond PHP's signed range are rejected, never cast to floats.
Namespace separation is not authorization: a host must deliberately limit what
it exports, especially secrets. The fallback declaration fails closed outside
the native host. No additional feature, provider or watcher is introduced.

`php/sapi/fallback/functions.php` at the repository root provides guarded PHP declarations for all built-in functions,
including optional cache functions and the opaque cache handle type. It is a
fail-fast fallback, not an implementation: missing native functions throw
`RuntimeException`. Existing native definitions are never replaced, including
when the file is loaded more than once. Downstream extensions own their stubs.
`claviron_runtime_available(): bool` returns `true` from the native SAPI in all
three execution models and `false` from the PHP fallback. This indicates runtime
presence, not service readiness, health, or optional feature availability.
`claviron_runtime_configured(): bool` reports completion of the shared PHP
runtime provider's native host bindings, not engine presence. It stays false
for detached evaluation/console hosts and ordinary PHP. This is not a health
check or a guarantee that an optional driver/store exists: native operations
still validate their own features and configured resources.
`claviron_events_available(): bool` separately reports whether the host has
prepared the event bridge. It is false in detached console/evaluation hosts
and in the PHP fallback. Actual event operations still fail explicitly when
the bridge is absent; the probe does not create a bus or join a fleet.
`claviron_app_name(): string` returns the embedding `AppState::name()`, captured
once during the shared runtime provider's boot, before PHP threads start. It is
not a vhost, task name or named PHP application selector. No app-local extension
registration is required; the PHP guard throws when the native function is absent.
`claviron_environment(): string` returns `AppState::environment()`, captured the
same way: `production` only when the process was started with `--production`,
`local` when nothing was selected, otherwise the named environment. A worker
reports its master's. It is Claviron's, not Laravel's `APP_ENV`.
After loading stubs, `function_exists()` alone no longer proves native support.
`php -n crates/php-embed/tests/stubs.php` checks fallback behavior and
coverage against the native function registrations.

| File                       | Responsibility                                                |
|----------------------------|---------------------------------------------------------------|
| `functions/mod.rs`              | `build_function_entries` → leaked `FunctionEntry` slice for `sapi_module.additional_functions`; `ensure_functions_for_current_thread` per-thread re-registration guard. |
| `functions/cancellation.rs`     | `claviron_is_cancelled()` PHP-callable handler. Resolves the current execution context's request-pool, task or application cancellation token. |
| `functions/config.rs` | `claviron_config()` reads exported namespace values from Congif and projects them directly into PHP; defaults apply only to missing keys. |
| `functions/cache.rs`            | Feature-gated `claviron_cache_*` PHP primitives. Resolves the default or a named `CacheRepository` into an opaque native handle, converts PHP writes to `CacheJson` and current archive reads directly from `ArchivedCacheJson` into Zend values, and bridges a PHP program thread to async store operations without exposing driver or codec details. |
| `functions/postgres/`           | Feature-gated `claviron_postgres_*` primitives backed by `db`. A named connection handle resolves the current reloadable pool; `session.rs` leases one pooled connection per pool for the duration of a request, so session state and transactions belong to the request rather than to whichever connection a statement drew. The directory is the backend boundary for later query/value decomposition without mixing MySQL behavior into PostgreSQL types. |
| `functions/orbit/counter.rs`    | Feature-gated keyed signed counter functions: get, increment, decrement, and reset. Values are integer units; PHP chooses any decimal scale. |
| `functions/orbit/lock.rs`       | Feature-gated atomic Orbit lock driver for PHP: acquire, owner-matched release, current owner, and force release. PHP/Laravel owns owner generation, blocking, retry sleep, callbacks, and `finally` release. |

The cache primitive signatures are:

```php
claviron_cache_store(?string $name = null): Claviron\Runtime\CacheHandle;
claviron_cache_get(Claviron\Runtime\CacheHandle $cache, string $key): mixed;
claviron_cache_put(Claviron\Runtime\CacheHandle $cache, string $key, mixed $value, ?int $seconds = null): bool;
claviron_cache_forget(Claviron\Runtime\CacheHandle $cache, string $key): bool;
claviron_cache_clear(Claviron\Runtime\CacheHandle $cache): bool;
```

`null` passed to `claviron_cache_store()` selects the configured default
repository; a non-empty name selects that exact repository. The returned final,
non-serializable native object owns an `Arc<CacheRepository>` and cannot be
constructed directly from PHP. Store lookup therefore happens once, while the
operation primitives remain driver-neutral. A cache miss returns `null`.
Resolution, encoding, and backend errors throw a PHP exception rather than
masquerading as a miss. A non-positive explicit TTL follows Laravel-compatible
semantics and forgets the key. Laravel repository/facade classes and richer TTL
types belong in downstream PHP packages above this primitive boundary.

### MySQL primitives

The `mysql` feature registers `Claviron\Runtime\MySqlConnection` and
`claviron_mysql_connection`, `execute`, `unprepared`, `begin`, `commit`,
`rollback`, `in_transaction`, and `disconnect` through the same MINIT/ZTS path. It is
forwarded by the facade's `mysql` feature when PHP embed is selected.
Arguments mirror the corresponding PostgreSQL functions; SQL retains MySQL's
`?` placeholders. Results include ordered columns/rows, `affected_rows`, and
the statement's `insert_id` as a decimal string. DECIMAL values remain exact
strings and BLOBs remain binary; unsupported column types fail explicitly.
There is no MySQL cursor primitive or multiple-result-set contract yet.

`functions/mysql/session.rs` leases one runtime-owned pool connection per pool
for each script request or application message. Tasks/boot-time queries hold
a lease across calls only while an explicit transaction is open. Script/task
teardown and application receive/respond/stream boundaries clean up abandoned
transactions. A server-reported deadlock or uncertain I/O poisons the session
until explicit rollback/new begin or the next request; it cannot silently
continue in autocommit. Ordinary MySQL SQL errors retain the transaction.

Unlike PostgreSQL, SQLx exposes no MySQL session-reset primitive. This first
implementation therefore **closes** each PHP lease at its work-unit boundary,
after rolling back an open transaction; it does not return potentially dirty
session state to the pool. This is isolation, not cross-request connection
reuse, and has a connection-establishment cost. Other Rust users of the pool
are unchanged. No invented query or cleanup timeout is installed. DDL and raw
transaction SQL retain MySQL semantics; callers must use the transaction
primitives and keep implicit-commit DDL out of transactional business work.

The PostgreSQL surface is intentionally backend-explicit rather than a generic
database API:

```php
claviron_postgres_connection(?string $name = null): Claviron\Runtime\PostgresConnection;
claviron_postgres_execute(Claviron\Runtime\PostgresConnection $connection, string $sql, array $parameters = []): array;
claviron_postgres_unprepared(Claviron\Runtime\PostgresConnection $connection, string $sql): int;
claviron_postgres_begin(Claviron\Runtime\PostgresConnection $connection, ?string $isolation_level = null, ?bool $read_only = null): bool;
claviron_postgres_commit(Claviron\Runtime\PostgresConnection $connection): bool;
claviron_postgres_rollback(Claviron\Runtime\PostgresConnection $connection): bool;
claviron_postgres_in_transaction(Claviron\Runtime\PostgresConnection $connection): bool;
claviron_postgres_disconnect(Claviron\Runtime\PostgresConnection $connection): bool;
claviron_postgres_cursor(Claviron\Runtime\PostgresConnection $connection, string $sql, array $parameters = [], int $batch = 1000): Claviron\Runtime\PostgresCursor;
claviron_postgres_cursor_fetch(Claviron\Runtime\PostgresCursor $cursor): array;
claviron_postgres_cursor_close(Claviron\Runtime\PostgresCursor $cursor): bool;
```

### The request owns the connection

Explicit `claviron_postgres_disconnect` removes only the calling thread's
lease for the named pool. It performs the same rollback/reset-or-discard cleanup
as request teardown, does not acquire a connection, and is idempotent after
native request cleanup. It leaves the request scope open, so subsequent queries
can obtain a fresh lease. Other threads and the runtime-owned pool are untouched.

PHP running inside Claviron never opens a database connection: it borrows one
the runtime already owns. `functions/postgres/session.rs` checks a pooled
connection out on first use and keeps it for the rest of the request, keyed by
pool name, so a request that talks to two pools holds one connection from each
and one that never queries holds nothing.

The lease is what makes the PDO mental model true here. Every statement in a
request lands on the same session, so `SET`, a temp table, an advisory lock and
a transaction behave the way a PHP developer expects. Acquiring per statement —
which this bridge did before — made that depend on which connection each
statement happened to draw, and left whatever it set behind for the next request
that drew the same one.

Because the boundary belongs to the runtime, two things are guaranteed rather
than hoped for:

* **A transaction cannot outlive its request.** `claviron_application_receive`
  and `claviron_application_respond` open and close the scope; whatever is still
  open is rolled back, and the connection is thrown away rather than pooled if
  even the rollback fails. `claviron_postgres_in_transaction` lets a PHP
  connection reconcile its own nesting count with that, so a level left by an
  earlier request is recognised as stale bookkeeping instead of an open
  transaction.
* **Nothing leaks into the next request.** A connection that ran anything
  outside ordinary `SELECT`/`INSERT`/`UPDATE`/`DELETE` traffic is reset before
  it goes back (`RESET ALL`, `UNLISTEN *`, `pg_advisory_unlock_all()`,
  `DISCARD TEMP`, `DISCARD SEQUENCES`) — deliberately not `DISCARD ALL`, whose
  implied `DEALLOCATE ALL` would throw away the prepared statements the type
  layer uses to describe parameters.

A transaction is therefore connection state, not a separate handle, which is why
`ClavironPostgresTransaction` no longer exists: the shape matches
`PDO::beginTransaction()`. Savepoints stay with the caller, since Laravel
already writes them as ordinary statements.

Parameter description is part of statement execution, not a best-effort probe.
If PostgreSQL rejects preparation, the original error and SQLSTATE reach PHP
without a second execution attempt. An open transaction is then aborted until
the caller rolls it back; retrying with natural PHP parameter types would only
replace the useful preparation error with `25P02`.

Execution contexts with no request boundary — a PHP task, an embedded queue
worker — keep the narrower promise: a connection is held only for as long as an
open transaction *or an open cursor* needs it. A cursor lives on the connection
that declared it, so it pins the lease the same way a transaction does;
otherwise the first `FETCH` outside a request would land on a different
connection and find nothing.

### Cursors

`claviron_postgres_execute` materialises every row before PHP sees any of them.
That is right for a page of results and wrong for a job walking a large table,
so the cursor functions move the cost to the server: `DECLARE`/`FETCH`/`CLOSE`
on the request's own connection, one batch in PHP at a time.

It is a cursor name rather than a Rust stream held between two PHP calls,
because a stream would have to keep borrowing the connection across the return
into PHP and back, and there is no honest way to hold that borrow. A name needs
nothing held.

`WITH HOLD` is decided by whether a transaction is already open. Inside one, a
plain cursor streams and the server holds nothing extra. Outside one,
PostgreSQL would close the cursor at the end of the implicit transaction that
declared it, so `WITH HOLD` is used and the server materialises the remainder
into its own temporary storage at that point. Both bound PHP's memory, which is
the goal; only the first also bounds the server's. Opening a transaction on the
caller's behalf is the third option and is deliberately not taken — a hidden
transaction around an iteration changes what every other statement in the
request sees.

`CLOSE ALL` therefore belongs in the session reset alongside `RESET ALL`: a
`WITH HOLD` cursor outlives its transaction by design, so it is exactly the kind
of state that would otherwise reach the next request.

A statement is bounded by the pool's `statement_timeout`
(`claviron-db/ARCHITECTURE.md` §2). PostgreSQL cancels the statement itself, so
the application sees an ordinary query error and the session stays usable for
the rest of the request. The session adds a client-side bound a fixed margin
behind it, for the case where no answer arrives at all — a partitioned network,
a host that went away — where a PHP thread would otherwise block until TCP gives
up. That path throws the connection away instead of pooling it: a query may
still be running on it, and neither a rollback nor a reset would be anything but
another wait on a connection that has already failed to answer.

Parameter arrays use the same typed boundary as the JavaScript database
extension (`null`, `boolean`, `i32`, `i64`, `f64`, `string`, `bytes`, `json`).
This keeps PostgreSQL wire types explicit; a downstream Laravel connection may
map its binding metadata to those tags without owning a socket or pool. Query
results preserve ordered column metadata and ordered row values, so duplicate
column names are not silently collapsed. PostgreSQL access uses the process
Tokio handle only from dedicated PHP OS/ZTS threads, exactly like the cache
bridge; it rejects accidental synchronous use on a Tokio runtime thread.

The cache bridge does not depend on rkyv or `serde_json`. Composite codec ownership
stays in `cache`: writes map recursive PHP arrays and scalars to `CacheJson`.
Reads retain the repository's immutable `Bytes` owner, obtain a checked
`CacheValueRef`, and traverse `ArchivedCacheJson` directly into Zend values.
There is no intermediate owned Rust tree for current composite archives or
temporary Rust vector for binary reads. PHP strings and arrays still require
their own Zend allocations; this is not a zero-copy PHP object representation.
The former JSON-text envelope retains its owned conversion path.
PHP arrays with sequential keys become arrays;
associative arrays become string-keyed objects, matching the pre-`CLVCV2` JSON
semantics while avoiding an intermediate `serde_json::Value` allocation tree.

### 6.6 `invalidation/` — notified PHP cache invalidation events

| File           | Responsibility                                                |
|----------------|---------------------------------------------------------------|
| `invalidation/mod.rs`  | Module declarations, re-exports                               |
| `invalidation/bus.rs`  | Synchronous `publish_raw` intercept producer and managed `PhpInvalidationBusService` typed subscriber. The consumer waits for events or cancellation, never a periodic timer. Its receiver survives targeted stop/start; shutdown consumes only a retained snapshot. |
| `invalidation/ipc.rs`  | `PhpCacheInvalidationObserved`: one versioned fleet topic carrying source worker and operation (`Delete`, `DeletePattern`, `ApcuClearCache`, `OpcacheReset`, `OpcacheInvalidate`). The v1 codec preserves UTF-8 payload and OPcache force, rejects malformed frames, and replicates no cache values. |

There is no PHP-owned SHM ring or invalidation polling interval. Existing
`FleetEventRegistry` native readiness owns notification; the platform fallback
on unsupported targets remains that registry's responsibility. Source-node
frames are skipped by the fleet transport, with an additional source-worker
check in the subscriber. The codec is inbound-only: decoded peer events install
no outbound callback and cannot create a Rust bridge echo. Intercepts publish
directly only after a successful local operation; PHP replay remains guarded.
The thread-local replay guard restores its previous state, including nested
native replay calls, so an inner call cannot re-enable publication prematurely.

Identical operations already pending in an apply queue are coalesced across
sources; whole OPcache reset retains its existing pending bit. Draining removes
pending identity before native application, so new invalidations arriving during
application remain queued for a subsequent request. OPcache force remains part
of operation identity. Different operations/keys are not discarded.

Delivery is bounded best-effort, not acknowledged or linearizable: fleet ring
and local subscription lag are logged, and distinct pending keys retain the
existing apply-queue behavior. Persistent applications still apply only on PHP
execution/request entry, not from their long-running receive loop. APCUIterator
pattern replay remains unimplemented; its placeholder is diagnostic only.

The former private PHP SHM kind 244 is retired, not automatically unlinked.
A deployment crossing this transport migration must replace all PHP workers;
old and new binaries do not exchange invalidations across the two transports.
Remove the orphan segment only while the old fleet is stopped, with `orbit`.

`PhpCacheInvalidationObserved` and `PhpCacheInvalidationKind` are exported for
typed Rust subscriptions. Emitting that type locally does not broadcast it;
only the intercept producer commits the observed effect via `publish_raw`.

Publication and receive/enqueue diagnostics include the invalidation operation,
its key or file when applicable, and the source worker in the message as well as
structured fields, so plain journal output can identify the signal. A queued
signal has not yet been applied; application still waits for PHP request entry.
APCu delete intercept and replay diagnostics also carry the key, result and
replay status in the message. Interception alone does not mean publication;
only the publish diagnostic confirms a signal was written to the bus.

### 6.7 Native HTTP dispatch

routing/proxy.rs owns script resolution and native service dispatch.
routing/adapter/script.rs exposes terminal handle and consuming try_handle;
routing/adapter/application.rs owns the named application's prepared native
handle and request policy. These are implementation modules behind the existing
root adapter exports, not a router or an Axum binding. The former bindings
module and build_script_router/build_application_router exports are removed.

### Service lifecycle

| File | Responsibility |
|---|---|
| `service.rs`  | `PhpEmbedRuntimeBuilder`, shared `PhpEmbedRuntime` provider, and `PhpEmbedStateExt`. Owns the application catalog, process runtime setup, request/application service registration and PHP task-executor installation. |
| `service/request.rs` | `PhpScriptService`: independent HTTP pool generation, maintenance, stats publication and joined drain. |
| `service/application.rs` | `PhpApplicationService`: named fixed applications, immutable catalog, independent generation/admission and event-driven joined drain. |
| `manager.rs` | `PhpApplicationController`, which projects engine-owned application state into `claviron-app` without copying it. |

These moves do not change public exports, execution policy, the process SAPI
lifetime or graceful drain. `engine/thread.rs` remains the one shared PHP
thread prologue and teardown path. PHP invalidation now uses the shared notified
fleet event transport, with native application still owned by PHP request entry.

### 6.8 PhpScriptAdapter ownership

Both native entries share the same resolver and PhpScriptService execution.
handle(&self, request) is terminal and converts a miss to 404.
try_handle(self, request) returns Result<ServiceResult<RouteResponse,
Request<RouteBody>>, SapiGatewayError>. It consumes an explicitly cloned handle
so service/runtime references move into execution without another internal
clone. Unhandled is decided before the body is polled or an engine job starts.

Prefix selection belongs to the caller. No body buffering, transport adaptation,
thread creation, timing policy or capacity maintenance is added by the adapter.
Existing exchange_tx capture remains an explicit caller capability. A persistent
application uses its separate PhpApplicationAdapter and does not enter this path.

## 7. Status & remaining work

The multi-thread engine is **shipped and benchmark-validated** (see §5).
What landed:

- ✅ `SapiThreadPool { threads: RwLock<Vec<SapiThread>>, next: AtomicUsize }`
  with round-robin dispatch, busy-CAS slot reservation, bounded queueing
  for buffered requests, grow-on-demand, and idle culling.
- ✅ `SapiThread { id, tx, busy, request_count, last_used_ms, spawned_at_ms, joiner }`;
  `JoinHandle` owned for the pool's lifetime, no `Option` wrapper.
- ✅ N=1 collapses to a one-element pool — same code path as N>1.
  No special "single-thread mode" branch.
- ✅ Typed `RequestPool` controls supply request capacity, idle culling,
  lifetime renewal, and request-count rotation. Strict sectioned YAML parsing
  lives in this crate and uses the same typed builder.
- ✅ PHP task occurrences use separate OS/ZTS threads within the same process
  SAPI, with task-local environments, cooperative cancellation, failure
  reporting and joined teardown.
- ✅ Per-thread identity exposed as `$_SERVER['CLAVIRON_THREAD_ID']`
  via `thread_local!<OnceCell<CString>>`; zero-allocation per request,
  same shape as the process-wide `SERVER_SCOPED_VARS` cache.
- ✅ Per-worker identity exposed as `$_SERVER['CLAVIRON_WORKER_ID']`
  and `$_SERVER['CLAVIRON_WORKER_COUNT']`; these are server-scoped
  values cached at SAPI boot.
- ✅ `ensure_sapi()` deduped process-wide (first thread wins
  `OnceLock`); `ext_php_rs_sapi_per_thread_init` runs **per** thread
  during its bind prologue, before its first script/job.
- ✅ `claviron-full/tests/sapi_chi2.py` chi-square uniformity harness +
  `claviron-full/public/sapi-conn.php` probe — keys on
  `(claviron_runner, sapi_thread_id) → (php_pid, zend_thread_id)`
  bijection; `randomness` verdict 🟢/🟡 across threads.
- ✅ **Slice 4 — graceful shutdown drain.** `SapiThreadPool::drain`
  flips a global `SAPI_SHUTTING_DOWN` flag (visible to PHP via
  `claviron_is_cancelled()`), stops accepting new dispatches,
  and waits on an `Arc<AtomicUsize>` in-flight counter (incremented
  via `InFlightGuard` RAII, panic-safe). Wait is bounded by
  the service's `graceful_timeout` — `None` = wait forever (correct
  default for transactional workloads where partial commits are
  catastrophic), `Some(d)` = report incomplete drain as a generation failure.
  `PhpScriptService::run` invokes drain inside its runnable future once
  its generation is cancelled. A cleanly drained pool may be re-opened by
  a later service generation; a bounded incomplete drain remains failed until
  its accepted requests and thread teardown finish. Request-pool channels close
  after accepted work settles; all OS threads release TSRM and are joined.
  Subsequent starts create fresh threads rather than reusing parked executors.
- ✅ **Request/response stream cancellation during shutdown.**
  Streaming request body pumps and blocking `read_post` readers poll
  `SAPI_SHUTTING_DOWN`; streaming response writes also stop accepting
  new chunks once the flag flips. Shutdown therefore cancels PHP stream
  I/O through the same connection-aborted path used for client drops.
- ✅ **H3 streaming gate** (`HostConfig::transport.h3.streaming`,
  default `false`). HTTP/3 over UDP has no equivalent of TCP FIN;
  quinn relies on `max_idle_timeout` to detect peer-gone, so a
  streaming-response slot held by an SSE / EventSource handler
  stays in a zombie state for tens of seconds after the client
  closes. Hyper on H2 drops the body in milliseconds via the
  TCP FIN → connection-reset chain.

  Net behaviour: when an `ExecMode::Stream` request arrives over
  HTTP/3 on a host that hasn't opted in, `PhpSapiEngine::execute`
  returns `421 Misdirected Request` + `Alt-Svc: h2=":443"; ma=86400`
  + `x-claviron-reason` diagnostic header. PHP isn't entered;
  the response is a pure-Rust early return that uses no SAPI
  thread. Browsers honour the Alt-Svc hint and retry over H2,
  where disconnect detection is fast.

  Operators with controlled clients (server-to-server feeds,
  internal tools) can flip `allow_h3_stream: true` per host to
  bypass the gate and accept idle-timeout-bound recovery.

  Background and the original investigation:
  <https://github.com/iadev09/h3-zombie-task-persistence>.

Operational notes:

- **Thread safety is an embedding-application/operator contract.** Typed
  request/application capacities and TaskService task concurrency are honoured
  directly.
  The old `threads_unsafe_extensions` opt-in idea
  was a possible policy gate, not an engine requirement.
- **Per-thread state is available, but not exported as Prometheus
  labels.** `SapiThreadPoolSnapshot` / `SapiThreadSlotSnapshot` back
  the PHP-visible SAPI stats path; `PHP_EMBED_METRICS` remains
  process-scoped because the current Prometheus surface only needs
  request-pool-wide counters. Dedicated program capacity is excluded.
- **No formal in-process `php_module_shutdown` / reinit path.**
  libphp.so has no production-safe per-process teardown/restart
  cycle (see memory entry `php_embed_lifecycle.md`). This does not
  affect request graceful drain; it only means full PHP runtime refresh
  remains a worker-process restart/rotation concern.

None of these notes block production use of the pool today.

---

## 8. Out of scope for this document

- libphp.so build / linking details — see `build.rs` and the
  `php-zts-embed` toolchain notes.
- **FastCGI runner — see `../php-fcgi`.**
  That doc carries the **SAPI vs FCGI risk matrix** (§8) — failure
  isolation, performance profile, operational comparison,
  decision shortcut. Read it together with this one when picking
  a runner per route.
- Vhost SNI routing and 421 enforcement — see
  `../edge` and the memory entries on SNI / cert SAN.
- Worker supervision and rotation — see `../supervisor`.

This document tracks the embed runtime as it stands.

The engine has no Axum feature, dependency or router binding. Rust applications
own their optional router; native PHP execution starts after that boundary.
