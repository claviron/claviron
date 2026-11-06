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
- `response`: the replies decided before dispatch.
- `routing`: host-free Axum route contributions, `Routable` and
  `RoutableRegistry`, behind the `axum` feature.
- `log`: access records, behind the `access-log` feature.

## How a site is served

`compile::compile` turns a `Site` into a `HostExecution`. What depends on how the
site is served comes in through `CompileInput`: a host name when there is one,
the connection's terms, the listener, the `SiteRegistry` its directives resolve
against, and `CompileHooks` for what the serving side adds to a scope. `NoHooks`
serves a site as itself.

`compile_site` is the shortest serving side: one site, one listener, a registry,
no hooks and no host. `claviron-transport`'s `Single` serves its result.
`claviron-edge` is another: its `HostCompiler` adds registered layers,
host-selected providers, Link dispatch and H1 upgrade endpoints.

## What is still in `claviron-edge`

Link dispatch and its session bindings, the host-keyed `HostAppRegistry`,
`Routable`, H2 CONNECT and WebTransport registries, and the H1 upgrade stage.
Lua, broadcast and upstream reach this crate through Edge's re-exports.
