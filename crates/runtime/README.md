# claviron-runtime

Process state and service lifecycle for Claviron applications.

Claviron owns its concrete `AppState`, defined and implemented in this crate.
[Continuo](https://crates.io/crates/continuo) supplies generic runtime and registry
primitives and the traits a consumer's state must implement; it does not supply
Claviron's application state. This crate implements those contracts for
`AppState` and connects them to Claviron's process model.

The opt-in `metrics` feature exposes the [metric-family composition registry](docs/metrics/README.md)
as `claviron_runtime::metrics` (also `claviron::metrics` through the facade).
Owners retain their counters, snapshots, and fleet transport. Enabling this
registry does not select HTTP, Edge, or a scrape endpoint.

It also exposes typed service registration, configuration access, process-local
events, and cooperative drain primitives. It does not open listeners, choose
application engines, or supervise child processes by itself.

## When to use it

Use this surface when implementing a Claviron component that needs shared
dependencies, boot/reload/shutdown hooks, or background execution. An application
normally uses `claviron::runtime` through the root `claviron` crate's `runtime`
feature rather than depending on individual implementation crates.

If you only need a general-purpose service registry without Claviron's process
state, use Continuo directly.

## State and lifecycle

`AppState` owns the Claviron registry and process context. Services are registered
in that registry; hosted application integration registers its application
manager and engine services there. Application controllers remain responsible
for their own hosted applications rather than moving their live state into
Continuo.

Here, `AppState` implements Continuo's `SharedState`, `HasRegistry`, `HasEvents`,
and `ReloadState` traits. `Runtime` is the specialization
`continuo::Runtime<AppState>`: generic lifecycle machinery operating on
Claviron-owned state, not a second application state supplied by Continuo.

The registry keeps shared instances by type; managed components declare
dependencies so lifecycle work can run in dependency order. Registering a
component is distinct from booting it, and booting it is distinct from starting
a `Runnable`.

The principal building blocks are:

- `Component`, `Bootable`, `Reloadable`, and `Finalizable` for managed services.
- `Runnable`, `RunContext`, and `ServiceManager` for ongoing work and its control.
- `Registry`, `Register`, and `Resolve` for composition and shared dependencies.
- `Gate`, `Permit`, `Guard`, and `GuardGroup` for admission and cooperative drain.
- `ConfigReader` and `HasConfigReader` for the prepared configuration view.

`AppState` also records the server name, configuration directory, environment
snapshot, and `RunMode`: master, worker, or standalone. These describe process
placement, not the identity of a hosted application. Constructing a state does
not start a server.

The server name and Orbit fleet name are separate identities. Fleet geometry,
configured worker count, and currently live processes are separate quantities;
they must not be substituted for one another.

## Configuration and execution environments

Claviron uses [Congif](https://crates.io/crates/congif) for typed configuration
and [Dotenw](https://crates.io/crates/dotenw) for environment preparation.
`AppState` owns the shared `ConfigReader` and its Dotenw environment
store. Configuration owners consume prepared snapshots through this catalog;
preparation, validation, and reload do not require each service to implement its
own file loader or environment expansion.

Dotenw combines the captured base environment, declared dotenv files and their
deployment variants, and explicit overrides. `state.environment()` identifies
the selected deployment, such as `production`; it is not inferred from the
hostname or a dotenv variable. Loading these values does not mutate the live
process environment.

The same preparation context serves embedded applications and external programs
launched by Claviron, not only its own supervised workers. Execution adapters
use the runtime's program-environment APIs to prepare their declared files and
values against that context. A program receives its declared environment, not
an unrestricted copy of the server's configuration secrets. Sharing the
preparation infrastructure does not make every application's values global.

Request-scoped execution pins an environment snapshot. Long-running owners can
observe changes and drain or replace their own execution under their lifecycle
policy. An environment update does not rewrite the environment of an already
running child process. The legacy `env_snapshot()` accessor remains a frozen
startup view; live configuration uses the prepared catalog and environment
handles instead.

## Process-local callbacks

This registration callback listens for the local process startup event:

```rust
use claviron::runtime::{AppState, ProcessStarted, registry};

fn register_events(state: &AppState) -> registry::Result<()> {
    state.events().listen::<ProcessStarted, _>(|state, _event| async move {
        println!("{} started", state.name());
        Ok(())
    })
}
```

Call it during composition, before the runtime starts its runnables and seals
listener registration. The callback receives the state when invoked; it need not
capture and retain that state. This example registers a listener, not a complete
server bootstrap.

Callbacks run through the process-owned event dispatcher. A listener handles its
own events serially; separate listeners may progress concurrently. Events are
not replayed to late subscribers. The underlying event channel is bounded, so
consumers must account for lag rather than treating it as a durable queue.

`ProcessStarted` announces completed boot and runnable submission. It is not a
promise that every application, listener, or upstream is ready for requests.
Process-local events do not become fleet events automatically; cross-process
delivery belongs to the Orbit integration.

## Reload and shutdown

Reload returns structured validation and reload outcomes. A request to start or
stop work is not proof that the requested state has already been observed;
service snapshots expose the execution state separately.

Shutdown is cooperative. A runnable owns the cleanup and drain of the work it
accepted; the runtime waits for that work rather than inventing a termination
deadline. Cancellation alone does not roll back external effects or make work
durable.

## Features

`config-watch` enables native configuration notifications for opt-in configuration
observers. Through the root crate, select `config-watch`. This is independent of
a language runner's source-file watching and application replacement policy.

## Related crates

- [claviron-app](https://github.com/claviron/claviron/tree/main/crates/app): hosted application identity and control.
- [claviron-supervisor](https://github.com/claviron/claviron/tree/main/crates/supervisor): master/worker process ownership.
- [claviron-orbit](https://github.com/claviron/claviron/tree/main/crates/orbit): shared state and cross-process coordination.

## Status and license

Part of Claviron's alpha development series; APIs may change. This README
describes the current source tree, not the placeholder package on crates.io.

Licensed under either [MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.
