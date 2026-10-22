# Link invoke

Call a separately running same-host service and await its result without
embedding that service or routing the operation through HTTP.

## Ownership

The service deployment chooses the Link name, owner/group access and `LinkSpec`:
participant lanes, inbox frames and bytes, stream slots and per-direction bytes.
Clients attach with that same geometry. Their own fleets and private resources
need not be shared. Capacity is not inherited from the HTTP application transport.

The embedding host opens a `link::Connection`, or uses an existing
`EventConnection`, and calls `invocations(limits)`. This returns a caller handle
and one driver. The host polls `driver.run_until` with its own shutdown future,
call handler and event/frame observer. No task, signal handler or timer starts
implicitly. An invoke-only connection needs no event bus.

## Calling

Registration selects transport. A call names a logical target and operation:

```rust
let (handle, driver) = connection.invocations(limits)?;
let mut calls = claviron::orbit::invoke::Client::new();
calls.register("example-service", handle.route("example-worker")?)?;
// Drive `driver.run_until(...)` concurrently in the owning service.
let result = calls.call("example-service", "read", payload).await?;
```

`Route` also permits an in-process implementation. The client does not select
Link, lane, UID or fleet on each call. An unknown target or multiple live Link
endpoints is an error; there is no implicit broadcast, retry or round-robin.
Operation names select domain behavior, not event subscriptions.

## Admission and results

Each endpoint declares pending-call and accepted-callback counts, operation
length, and request/result byte limits. Commands fit one inbox frame. Before
publication, the caller reserves a native Link stream slot for its result.
Its generation/epoch-protected ticket travels with the command. The service
claims the result side once before executing. Results are bounded records over
that slot, with native backpressure; they do not compete with event inbox frames.
The fixed rejection record must fit a stream buffer. Physical slot exhaustion
and local admission exhaustion refuse a call rather than growing a queue.

Publication is not execution. A result matches both endpoint incarnations and
the originating call sequence. Local future cancellation releases the caller's
side but does not cancel or roll back accepted domain work; a slot becomes
reusable after both sides release it. A lost
result is outcome-unknown: do not automatically resend a mutating call. Use a
domain operation ID and outcome query when recovery matters.

## Events, shutdown and trust

Events keep their existing topics, interest guards and delivery semantics.
One driver dispatches calls and observations; there is never a second reader
on an existing lane. HTTP/application transport and the separate append-only
invocation ring are unchanged.

Shutdown rejects new work and drains accepted callbacks and outgoing calls
before releasing the lane. The host owns cancellation policy and any deadline;
the adapter invents none. Dropping the driver future is an abort, not a drain.

Owner-only is the default Link policy. Explicit group access shares only the
dedicated Link and its stream resources. Group writers are trusted peers, not
authenticated users; lane names and request fields cannot grant authority.
Bind authorization in the service adapter and separate mutually untrusted
publishers rather than treating SHM permissions as per-operation authentication.
