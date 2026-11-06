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

## What is here and what is on its way

The request contract is here, and PHP's backends are written against it alone.

The plan that runs these steps (`HostPipelineBuilder`, the layers), the factory
a backend registers with, and the scope declaration it reads
(`HostConfig`, to be split into a site and a virtual host) are still in
`claviron-edge`. Lua, broadcast and upstream wait for those: they decode their
declarations from a scope and bind session registries.
