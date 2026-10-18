# Declare a value; let the runtime prepare its cell

Design identifier: `ORBIT_CELL`.

A shared counter or flag should not need its own provider, ring and background
task. `Orbital<T>` lets an application declare a supported typed value while
Claviron owns binding it to the fleet's common cell tables.

The value has a local Rust owner and a shared cell identity. Those are related,
but they are not the same lifetime or the same kind of memory.

## Declaration and preparation are separate

Declare the value with `claviron::orbit::Orbital::new(initial)` or `Default`.
Fleet registration installs the cells provider, which binds pending declarations
early, before application boot. Creating a declaration in an already prepared
runtime binds it immediately.

Before binding, `load()` returns the declared initial value. Mutation reports
`NotPrepared`; it never pretends a process-local write was a successful shared
write. `try_new()` lets a caller creating a value in a running runtime receive
allocation failure immediately. The infallible constructor retains an
allocation failure for subsequent fallible operations rather than turning it
into an empty value.

## Keep operations close to the value

After binding, the wrapper reaches its prepared cell handle directly. It does
not rejoin the fleet or rediscover a provider for every load or store. Supported
operations include typed loads/stores, exchange and value-specific arithmetic;
their errors remain visible.

A shared cell is not a general Rust object graph or a multi-cell transaction.
Declaring two fields with the same name does not make them one cell. Sharing
the allocation requires the same managed owner or an explicit cell identity.

Change waits use the cell's version and wake mechanism. The wrapper's
`wait_changed` is blocking; it is not an async method merely because a Tokio
runtime exists. Explicit interruption uses the provided interrupt/wake surface,
not an invented periodic polling interval.

## Release follows ownership

Managed clones share one allocation. The last managed clone releases its slot;
explicit `release()` can invalidate it earlier. A low-level handle opened by
cell ID borrows that lifetime and can become stale—it does not keep the managed
allocation alive. Stale access is an error, not a new zero-valued cell.

Pending declarations and the process-default binding hold weak ownership so
they do not keep an application runtime alive. Another live runtime cannot
silently replace that owner. Low-level handles and foreign-language adapters
have their own explicit ownership rules; the Rust wrapper's managed lifetime
is not automatically imposed on them.

The provider reuses and preserves its physical tables across boot/finalization.
That does not make released cells valid or provide database durability. Table
compatibility and allocation identity remain necessary after a restart.

## Follow the implementation

- [Managed declaration, binding and release](../../crates/orbit/src/cell/orbital.rs).
- [Cell resource provider and table policy](../../crates/orbit/src/cell.rs).
- [Fleet and cell registration](../../crates/orbit/src/registry.rs).
- [Orbit guide](../../crates/orbit/README.md).
- [Lifetime policy](LIFETIME_POLICY.md).
