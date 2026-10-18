# claviron-macros

Compile-time task registration and configuration override generation for Claviron.

This procedural-macro crate generates ordinary Rust implementations and task
registrations. It does not start services, create a runtime, or run a scheduler.
The current source exposes two macros: `task` and `Overridable`.

## Task registration through the facade

Application authors normally import `task` from `claviron::tasks`, with the root
`runtime` and `tasks` features. Set `crate_path = "claviron"` so generated paths
use the facade rather than requiring an implementation-crate dependency:

```rust
use claviron::runtime::AppState;
use claviron::tasks::{TaskResult, task};
use tokio_util::sync::CancellationToken;

#[task(name = "refresh_index", scope = "worker", crate_path = "claviron")]
async fn refresh_index(
    _state: AppState,
    cancellation: CancellationToken,
) -> TaskResult<()> {
    if cancellation.is_cancelled() {
        return Ok(());
    }
    // Perform application-owned work and observe cancellation while it runs.
    Ok(())
}
```

The example also needs `tokio-util`'s cancellation-token support. Applications
do not need a separate `inventory` or macro-crate dependency. Without an explicit
`crate_path`, expansion targets the task implementation crate.

The macro preserves the function and emits a runner factory plus an inventory
registration. The task service discovers that registration during composition;
compiling a function does not by itself configure an automatic trigger.

## Task metadata and function shapes

| Argument | Contract |
| --- | --- |
| `name` | Required task registration name. |
| `scope` | Eligible role: `worker` (default), `master`, or `any`. |
| `dispatch` | Distribution: `each` (default), `one`, or `pool`. |
| `config` | Optional YAML text for the task's built-in configuration entry. |
| `feature` | Consumer feature gating the generated factory and registration. |
| `crate_path` | Root used for generated references; use `claviron` with the facade. |

Scope and dispatch are independent declarations. Legacy scope spellings remain
accepted, but new definitions can use the canonical names above. Runtime task
configuration may override placement metadata.

Task functions take `(AppState, CancellationToken)` or add `TaskRuntime` as a
third argument. The latter exposes per-attempt identity, including a pool slot.
The macro selects the corresponding runner constructor; Rust type checking
checks the function against that runner's contract.

A `config` entry supplies a default when the task has no explicit entry in
`tasks.yaml`. A file entry replaces that built-in entry as a whole, not as a
field-by-field merge. Timing, retries, concurrency, and drain remain task-runtime
policy; the macro does not invent them.

`feature` gates the generated registration, not the original function. If the
function itself uses feature-specific types or dependencies, gate it explicitly
too. No runtime feature lookup or per-request discovery is generated.

## Configuration overrides from one definition

Claviron's configuration implementations use `#[derive(Overridable)]` to keep a
base settings struct and its sparse override aligned. A named-field struct such
as `TcpConfig` produces `TcpOverride` with an `apply(&self, base)` method returning
a resolved value. Other struct names receive an `Override` suffix.

The generated override derives Serde deserialization and rejects unknown fields.
Ordinary fields use the shared `Override<T>` representation; nested `*Config`
blocks use their corresponding override types. Duration fields receive the
duration deserializers instead of requiring callers to repeat that boilerplate.

Container attributes select the generated name and helper path. Field attributes
can replace a deserializer or provide a custom sparse type and resolver. The
default helper path is `crate::config::overrides`; the owning configuration
implementation must supply the helpers and nested override types it references.

This derive is currently an implementation-facing API, not a re-export from the
root facade. It is not a general dependency-injection or scope macro. Applying
an override does not validate the resulting operational policy, publish it, or
reload a service; those responsibilities stay with the configuration owner.

## Related crates

- [Tasks](../tasks/README.md): public task API, scheduling, execution, and lifecycle.
- [Helpers](../helpers/README.md): shared override and deserialization primitives.
- [HTTP](../http/README.md): typed policies using generated sparse overrides.
- [Runtime](../runtime/README.md): application state and service composition.

## Status and license

Part of Claviron's alpha development series; APIs may change. This README
describes the current source tree, not the placeholder package on crates.io.

Licensed under either [MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.
