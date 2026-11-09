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

## Native Rust applications

Select the facade's `rust` feature for `runner::Runner`. `with_router` accepts
the shared HTTP `RustRouter`; add `axum` or `warp` for a typed framework adapter.
A native application needs no language owner:

```rust,no_run
use claviron::{AppState, axum::{Router, routing::get}, runner::Runner};

fn router(_: AppState) -> Router {
    Router::new().route("/", get(|| async { "hello" }))
}

#[tokio::main]
async fn main() -> claviron::Result<()> {
    Runner::new().with_router(router).run().await
}
```

`runner.yaml` uses the shared options:

```yaml
name: example-app
fleet_name: example
orbit: true
```

`with_options` overrides file values; `with_config_path` selects another file.
`with_services::<T>()` and `with_bootstrap` add normal registry participants
before composition. Host-free HTTP Routables contribute routes before the
terminal service. `with_service` accepts another compatible Rust HTTP service
without requiring Axum.

Files use the existing optional capability. A session is a route like any
other: a handler takes `websocket::WebSocketOffer` and accepts, on the
application's own sockets and over Orbit from an Edge alike. Route it with `any`,
since an HTTP/2 WebSocket is a CONNECT. Lifecycle management requires the
application's registered controller.

| Attempt | Own socket | Over Orbit from an Edge |
| --- | --- | --- |
| WebSocket, HTTP/1.1 and HTTP/2 | Offered through the site | Offered through the site |
| WebTransport | Refused: the runner has no QUIC door | One reliable bidi stream as whole messages |
| Raw HTTP/2 CONNECT | Refused | Refused |

What stands in front of the owner applies to an attempt as to any request. What
nobody claims is refused, and no status makes a session of it.

For Warp, select `rust` and `warp`, then use `with_warp(filter)` or return
`claviron::http::warp::router(filter)` from `with_router`. The adapter runs the
filter without Warp's listener. `OtherService` is the low-level handler choice.

## Composing a runner

`claviron::runner::Composition` is the launch every language runner makes alike:
it checks the launch, hands it to the workers, registers the owner, routing, the
application's own registrations, trust and doors in that order, and serves. A
runner gives it its launch, name, lane runtime and owner.

## Language owners

The shared routing path runs admission guards first, then explicit static mounts, Rust routes, and finally the application owner.
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

With `runner` and `warp`, a language runner can select Rust paths before its
existing owner, without the native `rust` capability:

```rust,no_run
use claviron::{http::warp::router, runner, runtime::{AppState, registry}, warp::Filter};

fn configure(state: &AppState, prefix: claviron::http::ext::PathPrefix) -> registry::Result<()> {
    let filter = claviron::warp::path("health").map(|| "ok");
    runner::register_routes(state, [(prefix, router(filter).into())])
}
```

Selection uses segment prefixes before reading the body. Unmatched paths reach
the language owner unchanged. A selected router's response, including 404 or
405, ends dispatch; it cannot return a consumed body to another engine.

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

- Identity and transport: `name`, `fleet_name`, `listeners`, `defaults.listener` and `trusted`; TLS fields are available with TLS support.
- Request/reply policy: `requests` and `responses`.
- Orbit receiver capacity: `max_in_flight` (reserved and accepted exchanges through body/session completion; omitted means no bound).
- Static routing: `files`, `try_files`, and `assets` when file support is selected.
- Lifecycle: `workers`, supervisor settings, Orbit settings, and
  `edge_shutdown_policy`.

Omitting `listeners` opens no socket. Zero or omitted workers use
standalone execution; a supervised master does not serve application requests.
Request bounds and timeouts follow the selected HTTP policy, not a hidden
runner fallback. Operators must choose their desired admission and body limits.

Socket listeners use the same named declarations as an Edge:

```yaml
defaults:
  listener:
    execution: dedicated
listeners:
  application:
    transport: unix
    listen: unix:/run/example/application.sock
    protocols: [h1, h2]
    uds:
      mode: "0660"
```

Use `transport: tcp` and an IP socket address for TCP; TLS uses `tls: true`
with the runner's certificate inputs. TCP backlog is `tcp.backlog`; Unix backlog
is `uds.backlog`. Each instance is managed as `Listener:<id>`. In supervised
mode the master binds each Unix socket once and workers inherit descriptors;
worker restart retains the socket. Recovering a shared FD from workers that
survive replacement of their master is not supported yet.

Listener connection/protocol settings are per declaration. Request and response
policy stays application-wide under `requests` and `responses` at the runner
root. Per-listener body policy is rejected rather than silently ignored.
`defaults.listener` supplies shared socket settings and Orbit's execution
selection. Each named socket listener can override those defaults. The former
top-level `listener` key is no longer accepted.

The application name and fleet name are separate. An explicit fleet name avoids
requiring the application name itself to satisfy Orbit's naming constraints.
Independent masters must not assume they can allocate the same fleet lanes.

## Optional capabilities

The implementation crate has no default features. Its capabilities are `rust`, `axum`, `warp`,
`socket`, `orbit`, `tls`, `files`, `cli`, `supervisor`, `websocket`, and `watch`.
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

A message session over Link hears its ingress retiring as a close with code 1001
from `WebSocket::recv`, so its owner can finish its output and return while
accepted work drains. This does not authorize forced task termination or
cancellation of another owner's work.

## Status and license

Part of Claviron's alpha development series; APIs and configuration may change.

Licensed under either [MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.
