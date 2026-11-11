# claviron-pipeline Architecture

## Boundary

This crate is what an application or embedded runtime implements to answer
requests. It knows no virtual host, listener or connection driver.

`DispatchStep` is one body-owning operation; `BackendHandler` adds the
declaration a configured owner makes about itself. `HostContext` lends the
accepted connection, the selected host name when there is one, and the scope's
policy for the duration of a call: a host is optional here, and a backend that
does not read one runs the same under one application or many hosts.

Nothing depends upward: `claviron-edge`, `claviron-transport` and the backends
all depend on this crate, and it depends only on `claviron-http`.

## What is here

- The request contract: `DispatchStep`, `BackendHandler`, `HostContext`.
- `site`: what one application serves, with its normalization from a flat YAML
  declaration.
- `plan`: the flat request plan (`HostPipelineBuilder`, `HostPipeline`,
  `HostExecution`, `Backend`), built once and visited in declaration order.
- `layers`: the request and reply steps a plan is built from, none of which
  knows how many hosts exist.
- `factory`, `extension`: how a backend or a route directive is prepared from a
  scope of a `Site`.
- `accept`: work done once while a connection is accepted.
- `link`: dispatch to an application process over Orbit Link, behind `orbit`.
  Each `orbit:` destination prepares its configured `balance` once: `first`
  retains home-first selection, `round_robin` rotates suitable receivers. The
  target's cursor is shared by its clones but not other routes. A reload
  prepares a new destination; in-flight exchanges keep their original target.
  This does not change the application's worker policy.
  `compile` prepares it from a scope's `orbit:` declaration, so a site served by
  one listener reaches its applications the same way a host does. It needs
  `claviron_orbit::register`, which the full server calls; a `Single` composition
  calls it itself, and compiling without it fails. `Single` carries plain HTTP to
  the application. A WebSocket needs a transport policy that permits the upgrade,
  and `Single` has none to declare yet, so it is refused with 403; WebTransport
  sessions need Edge, because `quic::Single` does not offer them. A runner takes a
  whole site through its `site:` option, in front of its application on every door.
- `response`: the replies decided before dispatch.
- `routing`: host-free Axum route contributions, `Routable` and
  `RoutableRegistry`, behind the `axum` feature.
- `log`: access records, behind the `access-log` feature.

## How a site is served

`compile::compile` turns a `Site` into a `HostExecution`. What depends on how the
site is served comes in through `CompileInput`: a host name when there is one,
the connection's terms, the listener and the `SiteRegistry` its directives and
owners resolve against. An owner is registered with `register_backend`, from code
or from a directive, and a scope has one.

`compile_site` serves a site with no host, under `SiteTerms`. `claviron-transport`'s
`Single` serves its result on a listener, and `claviron-runner` on all of its doors
with its application registered as the owner after the site's files. A router with
nothing around it is called directly.
`claviron-edge` is another: its `HostCompiler` adds registered layers,
host-selected providers, Link dispatch and H1 upgrade endpoints.

## What is still in `claviron-edge`

The host-keyed `HostAppRegistry`, `Routable` bindings, the WebTransport registry, and Link's WebTransport sessions, which bind through that registry.
Edge does not re-export this crate: its consumers depend on it directly.

Orbit dispatch waits for receiver-owned admission and creator body-stream
capacity. One request `queue_timeout` bounds the complete pre-publication wait;
no separate Link timer or retry interval is introduced. A failed creator-slot
attempt releases its receiver reservation before parking. Local endpoint return
and remote slot release wake the caller, while event/control readers remain
independent. A published exchange is never retried; inbox publication errors
remain immediate failures.
