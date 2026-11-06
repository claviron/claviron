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
- `log`: access records, behind the `access-log` feature.

## What is still in `claviron-edge`

Compiling a `Site` into a plan (`pipeline/host/compile.rs`) and the registry it
reads (`HostAppRegistry`) are keyed by host, so nothing can serve a `Site` without
Edge yet. Link dispatch, quotas and H1 upgrade stages stay beside them for now.
Lua, broadcast and upstream reach this crate through Edge's re-exports.
