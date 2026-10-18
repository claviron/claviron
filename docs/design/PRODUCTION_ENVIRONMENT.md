# Choose the environment; do not leak it by accident

Design identifier: `PRODUCTION_ENVIRONMENT`.

A release binary is not necessarily a production deployment, and a machine's
hostname is not an environment selection. Claviron makes that choice explicit
and keeps configuration values separate from the process environment exposed
to hosted execution.

## Production is an explicit choice

The server defaults to `local`. A named environment is selected through
`environment(name)`; production is selected through `production()` or the
corresponding hosting CLI option. Passing `production` as an ordinary named
environment is rejected rather than quietly selecting it.

Application code reads the resolved environment from `AppState`, including
`is_production()`. A `.env` value, a debug/release build or a physical hostname
does not replace that decision. The production marker is not a claim that
every optional security or operational policy has been enabled automatically.

## Configuration is not global process mutation

[Dotenw](https://crates.io/crates/dotenw) prepares configuration values from the
inherited environment and the selected environment-file recipe. With the
conventional recipe, `.env` and `.env.<environment>` are read from the
application's working directory. Missing optional files are skipped; an invalid
existing file fails preparation.

Loading these files does not export their values into libc's environment.
[Congif](https://crates.io/crates/congif) bindings retain the live Dotenw handle
for configuration refresh. The legacy `EnvSnapshot` is a separate, frozen
startup view; reloading files does not mutate that view or the process's
environment behind a guest runtime's back.

Internal supervisor bootstrap variables are excluded from the application
snapshot. Program-specific environment recipes remain explicit rather than
being installed globally between requests.

## Serving processes scrub after boot

After configure, provider boot and validation, but before starting runnable
services, a worker or standalone serving process removes nonessential inherited
environment entries. OS/runtime essentials remain. The master is excluded:
it needs the inherited values to start subsequent workers.

This hygiene applies to serving processes, not just ones marked production.
An embedded runtime should receive the capabilities and configuration intended
for it, rather than accidentally finding every host credential through
`getenv()`.

This is not an in-process sandbox. Provider setup and foreign code executed
during boot run before the scrub; host-provided APIs may deliberately expose
values afterward. Native code in the same process is not isolated merely
because environment entries were removed.

## Follow the implementation

- [Server environment selection](../../src/server/mod.rs).
- [Configuration environment, snapshot and scrub](../../crates/runtime/src/env.rs).
- [Boot and scrub ordering](../../src/server/bootstrap.rs).
- [Runtime configuration integration](../../crates/runtime/README.md).
