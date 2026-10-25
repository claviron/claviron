# claviron-runner

Shared launch, ingress, and routing support for a Claviron application runner.

A runner hosts one application owner in its process. The owner supplies the
execution engine; this crate supplies the surrounding request path, listener
options, Orbit ingress, and lifecycle integration. It carries no Python, PHP,
or JavaScript interpreter of its own.

Use `claviron::runner` through the root crate's `runner` feature when building a
runner integration. Enable additional capabilities explicitly.

## Runner or edge?

Claviron's two main HTTP server compositions serve different needs. The runner
is for a single framework or application without virtual-host selection. It can
serve HTTP directly, run as an HTTP upstream behind a proxy, or receive requests
from a same-host edge over Orbit Link. Those are ingress choices around the same
application owner, not separate execution engines.

Use [claviron-edge](../edge/README.md) for a host-aware application server that
combines multiple applications, script routes, native services, and other
configured handlers directly on Claviron. A runner does not need to carry that
virtual-host publication layer merely to execute one framework.

## Request ownership

The shared routing path runs admission guards first, then configured broadcast
paths, explicit static mounts, Rust routes, and finally the application owner.
The owner is the fallback, not a second router racing the Rust application.

`Owner` wraps the application service. `Routing` holds the shared routing and HTTP
policy, while `AppRequest`, `AppResponse`, and `RequestBody` describe the request
boundary. A language integration registers these before opening its ingress.

With the `axum` capability, this callback adds a Rust health route while retaining
the registered owner as fallback:

```rust
use claviron::{
    axum::{Router, routing::get},
    runner,
    runtime::{AppState, registry},
};

fn register_routes(state: &AppState) -> registry::Result<()> {
    runner::register_router(state, |_| {
        Router::new().route("/health", get(|| async { "ok" }))
    })
}
```

Select the root `runner` and `axum` features for this example. It is a
composition callback, not a complete engine or executable. `register_router`
also sends method-not-allowed fallbacks to the owner; `register_router_with`
allows integrations to choose their own ownership arrangement.

Static files are explicit mounts. They must not accidentally expose executable
application source. Enabling file support does not make every file in the
application directory public.

## Ingress

`SocketIngress` supports TCP and Unix-domain sockets, HTTP/1 and HTTP/2, and
optional TLS. `OrbitIngress` lets a same-host edge reach the application over
Orbit Link. Both feed the shared request path rather than duplicating guards and
routing for each transport. This crate does not provide its own HTTP/3 listener.

Request origin and physical peer identity remain distinct. Forwarded origin
information is accepted only under the configured trust policy. An Orbit
request must not be represented as a fabricated TCP connection to obtain that
information.

## Language runner projects

Independent projects adapt this shared shell to their execution engines:

- [Gufi](https://crates.io/crates/gufi): the Python/ASGI runner.
- [Lumenize](https://crates.io/crates/lumenize): the PHP/Laravel runner.

Their current crates.io packages are name-reservation placeholders; full runner
releases are being prepared. These projects own their engine and framework
integration, while `claviron-runner` supplies the common launch, ingress, routing,
and lifecycle support. They are not bundled interpreters in this crate.

## Launch and configuration

`RunnerOptions` contains the shared launch settings. `launch::Launch<O>` combines
them with the integration's own options and carries prepared settings to worker
processes. The integration remains responsible for validating and booting its
engine.

The main option groups are:

- Identity and transport: `name`, `fleet_name`, `tcp`, `uds`, `h1`, `h2`, and
  `trusted`; TLS fields are available with TLS support.
- Policy and capacity: `requests`, `responses`, `max_in_flight`, and
  `max_concurrent_requests`.
- Static routing: `files`, `try_files`, and `assets` when file support is selected.
- Lifecycle: `workers`, supervisor settings, Orbit settings, and
  `edge_shutdown_policy`.

An absent TCP option does not open a listener. Zero or omitted workers use
standalone execution; a supervised master does not serve application requests.
Request bounds and timeouts follow the selected HTTP policy, not a hidden
runner fallback. Operators must choose their desired admission and body limits.

The application name and fleet name are separate. An explicit fleet name avoids
requiring the application name itself to satisfy Orbit's naming constraints.
Independent masters must not assume they can allocate the same fleet lanes.

## Optional capabilities

The implementation crate has no default features. Its capabilities are `axum`,
`tls`, `files`, `broadcast`, `cli`, `supervisor`, `websocket`, and `watch`.
The root facade's `runner-cli` extends an already-selected `runner`; it does not
enable the shell on its own. Select both for command-line launch options.
`runner-watch` selects the runner and its watcher. Facade and implementation
feature names are not interchangeable.

Source watching uses native file notifications for development. The engine
chooses whether a change restarts its application or requests worker replacement;
the shell does not prescribe a language-specific reload strategy.

## Shutdown behavior

Accepted connections and sessions drain under their owners. Disconnecting from
the edge alone does not terminate the application. The default edge shutdown
policy continues running; stopping in response to an edge shutdown is an
explicit opt-in. No implicit drain deadline is added by the runner.

Link message requests carry `orbit::SessionShutdown` in their extensions. Owners
observe this read-only ingress retirement signal to begin a cooperative protocol
close while accepted work drains. The signal does not authorize forced task
termination or cancellation of another owner's work.

## Status and license

Part of Claviron's alpha development series; APIs and configuration may change.

Licensed under either [MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.
