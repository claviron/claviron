# claviron-laravel

Laravel adapters for applications hosted by Claviron.

Laravel keeps its application container, routing, models, authorization,
notifications, and framework request behavior. This crate connects selected
capabilities to the Rust host instead of creating a second Laravel framework
or replacing its application bootstrap.

There are two cooperating packages: the Rust `claviron-laravel` crate, exposed
as `claviron::laravel` by the facade's `laravel` feature, and the companion
Composer package [`claviron/laravel`](php/composer.json), which depends on
`claviron/sapi`. Composer discovery registers the Laravel service providers;
native host registration and configured runtime resources are separate steps.

## HTTP execution remains a choice

An ordinary Laravel front controller can run under
[embedded PHP](../php-embed/README.md) script mode with PHP request startup and
shutdown for every request. Keeping execution threads warm does not turn it
into an Octane application or retain its container across requests.

Persistent application mode is a separate opt-in. The Composer package supplies
`Octane\ApplicationRunner` and its application entry for the explicit native
receive/respond loop. It requires the corresponding Octane dependency and
framework cleanup discipline; it is not an implicit change made by enabling
the Laravel feature. The application owns its base path, autoloader, bootstrap,
providers, and request class.

Use [Edge](../edge/README.md) for host-aware composition or
[Runner](../runner/README.md) for a single-framework application. The
[Lumenize](https://crates.io/crates/lumenize) runner project is being prepared
for release; its current crates.io entry is a name-reservation placeholder.

## Native configuration and Artisan

`congif::LaravelConfigProvider` registers a parser for trusted Laravel-authored
Claviron configuration documents. It uses the application's Composer helpers
without booting all Laravel providers or replacing Laravel's own configuration
repository. [Congif](https://crates.io/crates/congif) still owns source reads,
typed decoding, validation, and publication. Returned PHP data becomes owned
Rust values without a JSON round-trip; executable PHP is not a sandbox.

Declare the process INI and compiled-in extensions before configuration
evaluation, because evaluation shares the process SAPI with hosted execution.
Registration itself does not execute PHP.

`artisan::run` executes the application's own `artisan` entry through the native
console API. This crate also currently includes a `lumenize` console binary
that accepts ordinary Artisan arguments from the project root. It preserves
terminal streams, command boot, and exit status; it does not implicitly start
HTTP, TaskService, native database/cache services, or join a live fleet. Console
cancellation is cooperative and joins the owned PHP thread.

## Framework drivers

The Composer providers register adapters on Laravel's existing managers. A
driver name does not create its backing Rust service or select every native
integration automatically.

- **Cache:** the `claviron` driver selects a configured runtime store through
  `runtime_store`, or the runtime default when omitted. Laravel prefixes and
  repository semantics remain explicit. An ordinary detached PHP process may
  use a configured Laravel-store fallback; without one, detached reads miss and
  writes are dropped with a warning.
- **Database:** opted-in `pgsql` and `mysql` connections use the selected native
  pool while Laravel retains grammar, query events, hydration, transactions,
  savepoints, and commit callbacks. Pool connection/session options come from
  the host configuration, not PDO initialization options. Match those policies
  before switching a connection.
- **Broadcasting:** the `claviron` driver publishes through the native
  `BroadcastService`, using the selected connection's application key rather
  than making a loopback Pusher HTTP call. Echo/PusherJS protocol and Laravel's
  private/presence channel authorization remain unchanged.
- **Logging:** the native Monolog adapter writes through configured Claviron
  log channels while Laravel retains its application-owned log manager.

The database adapter's configuration selector is `claviron: true` for the
runtime default pool, or a named pool string. Ordinary detached CLI/FPM
execution retains the ordinary PDO path. Detection distinguishes a configured
host from the mere presence of native function names; missing features/resources
in a selected native path remain explicit failures. Not every PDO option or
multiple-result/cursor behavior is identical across native backends.

## Authentication and session data

The opt-in `claviron` auth guard consumes the verified native request identity.
The host supplies credential extraction, user lookup, expiry/revocation, and
guard policy. A Laravel `NativeUserProvider` hydrates its own `Authenticatable`;
Laravel retains Gates and authorization. Native model attributes are internal
hydration data, not a public user serializer or a raw-token transport.

The `claviron` session driver stores Laravel's serialized data through its cache
repository. Explicit `session.store` wins; otherwise the configured
`cache.default` is used. The driver name does not require a cache store also
named `claviron`.

`native.session:<data-namespace>` is a separate opt-in middleware binding that
payload to an existing native authenticated session. It derives a data ID from
the native identity, bounds writes by remaining credential life, and emits no
second Laravel session cookie. It does not choose another auth guard.

Laravel still owns serialization and flash aging. Registering this middleware
does not make `session()` a shared Rust/PHP map or give Rust automatic flash
access. Guard selection, application CSRF policy, and admission remain explicit
consumer responsibilities.

## Tasks, events, queues, and notifications

The queue adapter changes execution ownership, not queue storage. Laravel keeps
its queue backend and jobs; embedded workers observe task cancellation at loop
boundaries. Blocking native calls still require an appropriate execution and
shutdown policy. Embedded Redis queue connections use non-blocking polls to
avoid an uncancellable native wait; Laravel's worker policy supplies the idle
wait. Detached CLI/FPM retains its normal queue behavior.

Opted-in background schedule commands submit through the existing PHP program
invocation path; Laravel retains scheduling intent and completion callbacks.
The invocation ID is correlation, not acknowledgement of completed work.

An explicitly configured invocation/event catalog determines which handlers and
decoders a serving task can use. Loading providers for HTTP or queue execution
does not start a consumer automatically. Stable event topics support
cross-language payloads; PHP-class relay serialization is a different contract.
The event stream is bounded and not a durable queue or exactly-once guarantee.

Native auth lifecycle translation is likewise opt-in: the Rust bridge emits
application events, and the selected PHP consumer dispatches Laravel's ordinary
auth events. Notification handlers resolve allowed notifications through
application policy; Laravel's dispatcher still chooses mail, database,
broadcast, synchronous, or queued delivery. Submitted, queued, and delivered
are different outcomes.

## Compatibility and registration

Select the facade's additional capabilities for the services actually used,
such as `auth`, `postgres`, `mysql`, `tasks`, or `log-pipeline`. The Laravel
feature includes embedded-PHP/cache/broadcast dependencies, but framework
adapters still need their native registration and application configuration.
Deploy matching native functions and Composer adapters together.

The Composer manifest currently declares PHP `^8.2` and Illuminate
`^11.0 || ^12.0 || ^13.0`; these are dependency constraints, not a claim that every
version/platform combination has been tested. Native embedding additionally
requires the [PHP engine toolchain](../php-embed/README.md#requirements-and-optional-capabilities).
Application-specific user models, profiles, routes, and deployment policy do
not belong in this shared package.

## Status and license

Part of Claviron's alpha development series; APIs may change. This README
describes the current source tree, not the placeholder package on crates.io.

The Rust crate is licensed under either
[MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT) or
[Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.
The companion Composer package declares its license in [its manifest](php/composer.json).
