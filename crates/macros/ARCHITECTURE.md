# claviron-macros Architecture

`claviron-macros` provides the procedural macro used to register
native Rust tasks.

## Role

The `#[task(...)]` macro turns a Rust async function into an inventory
registration consumed by the task runtime. It supports metadata such as:

- task name
- scope (`master`, `worker`, or `any`; legacy `workers`, `fleet`, and `both`
  remain accepted)
- dispatch (`each`, `one`, or `pool`)
- optional feature gate
- crate path override

Example shape:

```rust
#[task(name = "daily-report", scope = "workers", dispatch = "one")]
async fn daily_report(ctx: TaskContext) -> Result<()> {
    Ok(())
}

#[task(name = "queue", scope = "workers", dispatch = "pool")]
async fn queue(ctx: TaskContext) -> Result<()> {
    // Long-running daemon body; tasks.yaml concurrency is shared by workers.
    Ok(())
}
```

Task bodies normally take `(AppState, CancellationToken)`. A body that also
needs its per-attempt identity may take a third `TaskRuntime` argument; the
macro selects `TaskRunner::from_with_runtime` for that signature. In a pool,
`TaskRuntime::slot()` is the stable `1..=concurrency` slot number, not a
process-local launch counter.

## Usage Boundary

Application/task authors should normally use this through the public
exports in `tasks`, not by depending on this macro crate
directly. The macro crate is the implementation detail; `tasks`
is the user-facing task API.

Keep generated code tied to task-runtime contracts. If those contracts
change, update both the task crate docs and this macro docs.

Generated scope values use `TaskScope::Master`, `Worker`, and `Any` through
the task crate's public exports. These are the shared Orbit placement enum;
the macro does not require a direct Orbit dependency in applications.

`scope` and `dispatch` are independent. The macro writes both defaults
into `TaskRegistration`; omitted metadata selects `worker` and `each`.
Master or all-role execution must be explicit. `tasks.yaml` may override
them at runtime. The
`scope` aliases `workers`, `fleet` and `both` remain accepted. The old
`mode` key is not part of the attribute argument contract.

## Attribute parsing

`TaskArgs` accepts only known, unique string-literal arguments. Unknown names,
duplicate keys and wrong literal types fail before expansion with an input-span
diagnostic. This prevents a typo or integer/bool value from silently falling
back to worker/each placement. Scope aliases and intentionally omitted defaults
retain the expansion contract above.
