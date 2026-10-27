# claviron-php-embed

Managed in-process PHP execution through Claviron's SAPI and OS/ZTS threads.

This crate links the PHP engine into the host through `ext-php-rs`. Rust owns
admission, thread capacity, execution generations, and drain; PHP executes the
application. No FastCGI socket or external `php` command sits between an HTTP
request and this engine.

Use `claviron::ephp` with the facade's `php_embed` feature alongside the desired
server composition. [Edge](../edge/README.md) can mix PHP with native services
and other applications; [Runner](../runner/README.md) surrounds a single
application owner without virtual-host selection.

The `postgres`, `mysql` and `sqlite` facade features optionally expose native
database primitives backed by named Rust pools. PHP framework adapters select
them explicitly; they do not replace every application's PDO connections.

## Execution models

- **Script requests:** a reusable thread runs PHP request startup, the selected
  script, and request shutdown for each HTTP request. Reusing the thread does
  not retain that request's PHP objects. Ordinary Laravel front-controller
  execution does not require an Octane loop.
- **Persistent applications:** a named boot script remains alive, receives
  explicit request/event messages, and returns explicit responses. The PHP
  adapter owns framework request construction and cleanup between messages.
- **Tasks:** the optional TaskService integration runs PHP programs on separate
  execution threads. Task timing, placement, cancellation, and supervision
  belong to the task system, not an HTTP application pool.

All share one process SAPI, extension table, and INI selection. Persistent
instances and request threads have separate Zend contexts, but they are not
security or crash-isolation boundaries. Use separate processes where that is
required; [FastCGI](../php-fcgi/README.md) is the external-FPM alternative.

## Register an embedded runtime

Configured registration uses `ephp::register(state)` and the optional `php.yaml`
under the runtime configuration directory. A missing file retains the configured
one-thread request-pool default. An explicitly registered runtime is reused by
that convenience entry instead of being replaced.

For configuration-free composition, a callback can declare a request pool:

```rust
use claviron::{
    ephp::{PhpEmbedRuntime, RequestPool},
    runtime::{AppState, registry},
};

fn register_php(state: &AppState) -> registry::Result<()> {
    PhpEmbedRuntime::builder()
        .request_pool(RequestPool::fixed(2))
        .build()
        .register(state)
}
```

This declares two execution threads; it does not start PHP or mount an HTTP
route. With the facade's `Server`, pass it to
`Server::with_definitions(register_php)` so the explicit definition precedes
built-in defaults; ordinary `Server::register` callbacks run after those
defaults. Select `server` and `php_embed` for these APIs, then compose the
ingress and [PHP route](../php/README.md) separately. An empty builder declares
no request pool. Register all programs and host extensions before native
startup; a second independent embedded runtime in the same process is not
supported.

`PhpEmbedRuntime` is the shared provider. `PhpScriptService` and
`PhpApplicationService` own separate runnable generations and drain. Application
controllers expose those owners through the common application manager instead
of copying their live state.

## Configuration and capacity

The startup document separates `engine` INI settings, `lifecycle` policy, `sapi`
request-pool policy, named `applications`, `management`, `events`, `launch`, and
optional `metrics`. `sapi: null` disables the script request pool, not the shared
engine capabilities or named applications.

### OPcache warmup

Use `engine.warmup` to recursively compile PHP files in selected folders:

```yaml
engine:
  warmup: [app, vendor/example/package]
```

Paths are relative to the process working directory. An empty list disables
warmup. Each worker or standalone process runs it once in the background after
provider boot; it does not delay listener startup. The existing PHP engine runs
the embedded script on an owned OS/ZTS thread. OPcache must be available and
enabled; otherwise warmup logs that it was skipped.

Only regular `.php` files are compiled, without executing their contents.
Overlapping folders are deduplicated. Directories named `test` or `tests`
(case-insensitive) and nested symbolic links are skipped; an explicitly selected
root may itself be a symbolic link. A file that fails compilation or is refused
by OPcache is logged and skipped while the remaining files continue. Missing
root folders fail only the background occurrence, leaving serving processes running.

When TaskService is present and the `tasks` feature is enabled, the same operation
is also registered as the manual `php-warmup` task for management clients. Its
task policy can be overridden under `tasks.php-warmup` in `tasks.yaml`. TaskService
is optional for startup warmup. Concurrent warmups in one process are rejected.

Warmup does not watch files, reset OPcache, or reload persistent applications.
`opcache.validate_timestamps` and other OPcache settings remain native PHP INI
policy. With timestamp validation disabled, updating source still requires
explicit invalidation and an appropriate application or process restart.

Request and application pools support fixed or elastic capacity, idle retention,
and optional idle-instance rotation. Capacity is local to each serving process.
HTTP route policy controls body limits, queue wait, and response handling;
application execution limits and instance replacement are separate contracts.
An accepted request is not automatically replayed when an instance fails.

The resolved `php.yaml` execution plan is startup configuration, not a live
Congif reload owner. Service restarts reuse it; changing INI, definitions, or
the frozen startup plan requires process replacement. Execution environment
files have a separate observation path through
[Dotenw](https://crates.io/crates/dotenw): script requests pin a route-owned
snapshot, while persistent owners can replace their own execution after valid
environment changes. That does not make arbitrary engine configuration live.

## Persistent application protocol

An application enters the explicit `claviron_application_receive()` /
`claviron_application_respond()` protocol after boot. Receive also carries
application events and returns null when admission ends. The response contract
is buffered and preserves binary bodies and repeated headers.

This is not transparent SAPI reuse: `echo`, `header()`, `$_POST`, `$_FILES`, and
`php://input` are not automatically remapped for each received message. The
framework adapter supplies that request boundary and its cleanup. The next
request is not admitted to an instance until PHP re-enters receive after its
post-response work. Streaming and protocol upgrades are not supplied by this
buffered application contract.

## Native capabilities and environment

The PostgreSQL bridge lets a native host function borrow the PHP request's
existing database session through `postgres::with_request_connection`, so both
languages participate in the same transaction rather than acquiring unrelated
connections. [CONNECTION_HIJACK](../../docs/design/CONNECTION_HIJACK.md) explains
the request ownership and cleanup boundary.

`extension::PhpEmbedExtension` contributes compiled-in functions, classes,
enums, per-thread setup, and optional Zend observers. Contributions are frozen
before SAPI startup, with metadata owned for the process lifetime. This is not
PHP's dynamic extension loader, and registration is not a per-request action.

Optional bridges expose configured cache stores, database leases, authentication,
log channels, events, and invocations. A compiled symbol does not establish that
its backing host service is prepared. Authentication projects a verified native
request identity rather than trusting a PHP-supplied credential. Native PHP
objects and Zend values do not cross execution-thread lifetimes.

Declared environment values and trusted HTTP/TLS metadata reach PHP through
the execution context. Named environment access is not the same as dumping the
process environment; `putenv` changes remain execution-local rather than changing
other applications' libc environment.

## Evaluation, console, and lifecycle

`evaluate::PhpEvaluator` executes trusted source bytes and returns owned
`PhpValue` data without an HTTP listener or service pool. Objects, resources,
cycles, and excessive nesting are rejected; evaluation is not a sandbox.
`console::PhpConsole` runs a caller-owned command with terminal streams and an
exit status. Both share native startup with hosted execution; declare INI and
extensions before the first use and offload their blocking calls appropriately.

Shutdown stops admission and drains owned work. A missing grace deadline means
indefinite drain. Guest execution budgets and cooperative cancellation remain
distinct from graceful policy; interrupting PHP does not guarantee termination
of arbitrary blocking native extension code. One service's control grants no
authority over another service's execution.

OPcache invalidation interception is enabled by default in this implementation;
APCu interception is opt-in. When configured, invalidation signals use the
existing bounded fleet event transport and apply at PHP execution boundaries.
They do not replicate cache values or guarantee acknowledged delivery. They are
not a substitute for the shared Claviron cache store.

## Requirements and optional capabilities

Embedding requires a compatible PHP ZTS build with embed headers/library and
the `ext-php-rs` native toolchain. `PHP` and `PHP_CONFIG` must describe the same
installation; use stable toolchain paths across builds. A CLI-only PHP binary
does not by itself provide the embedding library.

Facade features such as `auth`, `cache`, `postgres`, `mysql`, `tasks`,
`log-pipeline`, and `metrics` extend an already selected embedded runtime.
The engine remains framework-neutral. The companion Composer package is
[`claviron/sapi`](php/composer.json); [Laravel](https://github.com/claviron/lumenize) layers its
own adapters above it.

## Status and license

Part of Claviron's alpha development series; APIs and configuration may change.

The Rust crate is licensed under either
[MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT) or
[Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.
The companion Composer package declares its license in [its manifest](php/composer.json).


### Cell revision tokens

Cell `version()` and `wait()` return opaque integer tokens. Pass a token back
unchanged; do not increment it or convert it through a float. Text cells retain
all 64 revision bits in the language's signed 64-bit integer, so a valid token
can be negative. Scalar cell tokens remain unsigned 32-bit counts represented
as nonnegative integers; out-of-range scalar wait tokens are rejected.
