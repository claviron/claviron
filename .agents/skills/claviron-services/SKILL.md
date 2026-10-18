---
name: claviron-services
description: Add or change Claviron services, dependency injection, typed configuration, and lifecycle hooks. Use for component registration and managed background work, not application business logic.
---

# Claviron services

Read the applicable project instructions. Locate the Claviron source or dependency
version used by the application before choosing APIs. The public entry point is
the `claviron` facade; downstream code does not import implementation-crate paths.

## Choose the owner

- `AppState` belongs to Claviron. It contains the process-local registry and
  configuration context; Continuo supplies the generic traits and runtime.
- Register the shared instance during composition and resolve dependencies at
  boot. Resolution returns the registered `Arc`, not a newly constructed service.
  Use a trait binding for an existing instance instead of a second provider.
- Use lifecycle hooks for preparation, validation, reload, and finalization.
  Use `Runnable` for an owned service generation with ongoing work.
- Use TaskService for occurrences with timing, dispatch, attempts, and completion.
  A hosted Application instead has identity, admission, instances, and an engine.
  Neither concept is a synonym for a process or thread.

Declare dependencies rather than relying on incidental registration order.
Registration does not open listeners, resolve services, or start guest runtimes.
Check the actual bootstrap phase before installing an override: definitions that
must replace built-in defaults belong before those defaults are registered.

## Configuration and features

Implement Congif's `Configurable` contract for typed sources and validation.
Read prepared snapshots, not files or environment variables on each request.
Use `ConfigObserver` when the owner supports applying prepared changes; otherwise
keep restart-required settings explicit. Dotenw prepares declared execution
environments without mutating the process environment.

Use existing facade features and combinations. Weak `dependency?/feature`
forwarding extends an already-selected dependency; it does not select it.
Keep activation, configuration, registration, and actual service startup distinct.

## Drain and replacement

Pass the owning generation's cancellation signal into its accepted work. Stop
admission, then drain and join that work before finalization. Cleanup may not
stop another service's generation or an independently owned process.

Durations come from the owning configuration or API. Do not replace an explicit
unbounded drain with a fallback timeout, or add a polling loop to conceal a
missing wakeup. Execution budgets and process-level termination are separate
from graceful drain.

## Source map

From the Claviron source root, start with `crates/runtime/README.md`,
`crates/app/README.md`, and `crates/tasks/README.md` as relevant. Inspect
`crates/runtime/src/state.rs` and the affected component's public implementation
for signatures. Read the owning `ARCHITECTURE.md` when available; its absence in
a public package does not justify inventing another lifecycle.
