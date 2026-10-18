# claviron-app

Language-neutral identity, lifecycle, and control for hosted applications.

A PHP application, a Python application, and Claviron's own infrastructure
services are different things. This crate gives execution engines a common way
to expose their applications without making the management layer own a second
copy of their live state.

Applications normally access it as `claviron::app`, with the root crate's
`application` feature. It does not include an interpreter, an HTTP listener, or
an application execution engine.

## Identity and ownership

An `ApplicationId` combines an `ApplicationRuntime` with an `ApplicationName`.
For example, `python:shop` and `php:shop` identify different applications even
when their names match.

```rust
use claviron::app::{
    ApplicationId, ApplicationIdentityError, ApplicationName, ApplicationRuntime,
};

fn shop_id() -> Result<ApplicationId, ApplicationIdentityError> {
    Ok(ApplicationId::new(
        ApplicationRuntime::new("python")?,
        ApplicationName::new("shop")?,
    ))
}
```

The execution engine owns its application's enabled state, generation, pool,
failure state, and start/stop behavior. The application manager delegates to that
owner; it does not create a competing lifecycle state machine. An infrastructure
`Runnable` is not automatically a hosted application.

## Connecting an engine

Implement `ApplicationController` for the engine's application registry. A
controller provides its runtime identity, application listings and status, and
asynchronous start, stop, and restart operations. Register one shared controller
per runtime with `ApplicationManager::add`. Duplicate runtime registration is an
error, not an implicit replacement.

The manager addresses applications by their full identity:

```rust
use claviron::app::{ApplicationId, ApplicationManager, ApplicationManagerError};

async fn restart_application(
    manager: &ApplicationManager,
    id: &ApplicationId,
) -> Result<(), ApplicationManagerError> {
    manager.restart(id).await
}
```

This assumes that the corresponding controller has already been registered.
Unknown runtimes and unknown applications remain explicit errors.

## Status is separate from control

`ApplicationSnapshot` reports identity, description, lifecycle, enabled state,
generation, optional pool status, and the last error. Descriptions can identify
the runtime, frameworks, runner, request mode, and a symbolic icon without
reconstructing this information on each HTTP request.

A successfully accepted management command does not establish that an
application has reached its target state. Observe the engine's snapshots and
lifecycle events. Likewise, a reachable Orbit Link peer is not proof that its
application is running or accepting requests.

There is no universal application reload operation: an engine may reload code,
replace instances, or restart an application according to its own contract.
Ordinary HTTP upstreams are not managed applications merely because Claviron can
forward requests to them.

## Admission and replacement policy

`ApplicationLimits` declares application admission capacity and an optional
execution timeout. Admission counts executing and waiting requests together.
HTTP queue and request-head policy remain HTTP concerns; these limits do not
replace them.

`ApplicationRetryPolicy` governs failed instance replacement. It does not
authorize replaying an accepted application request. Retry timing and limits
belong to the declared policy, not to individual request handlers.

## Events and integration

`ApplicationEvent`, `EventTopic`, and `EventValue` provide validated topics and a
language-neutral payload representation. Events may target a runtime-qualified
application or use an explicitly shared topic. The `environment`, `invoke`,
`link`, `fleet`, and `management` modules connect this contract to prepared
environment settings, invocation routing, transport, and management views.

Event delivery is bounded and fire-and-forget. It supplies neither replay nor
acknowledged, exactly-once execution. Use a durable application protocol when
lost delivery would affect financial correctness or other persistent outcomes.

## Related crates

- [claviron-runtime](https://github.com/claviron/claviron/tree/main/crates/runtime): component lifecycle and process state.
- [claviron-runner](https://github.com/claviron/claviron/tree/main/crates/runner): ingress and routing around one application owner.
- [claviron-orbit](https://github.com/claviron/claviron/tree/main/crates/orbit): same-host transport and shared coordination.

## Status and license

Part of Claviron's alpha development series; APIs may change. This README
describes the current source tree, not the placeholder package on crates.io.

Licensed under either [MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.
