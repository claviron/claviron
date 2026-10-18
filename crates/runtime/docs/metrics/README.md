# claviron_runtime::metrics

Composition of independently owned Prometheus metric families for Claviron.

`MetricsProvider` is a process-local family registry and exposition composer.
It does not know HTTP, Orbit, PHP, task scheduling, connection pools, or an
application's counters. Each subsystem owns its metrics and contributes a
`MetricsFamily` that knows how to render the appropriate view.

Applications normally use `claviron::metrics` through the root crate's
`metrics` feature. That facade feature also enables instrumentation in already
selected subsystems through their own feature forwarding; it does not select
every subsystem or open a metrics listener.

## Register families, not another collector service

`register(state)` inserts the concrete `MetricsProvider` into Claviron's
runtime registry. Owners then call `provider.register(Arc<Family>)`. Families
are ordinary shared objects, not independent lifecycle providers.

The stable `MetricsFamily::name` is the registration key. Registering that name
again replaces it. `render_prometheus(state)` renders registered families in
name order, returning a contextual error if a family fails. It takes a snapshot
of family handles before invoking their formatters, so user formatting does
not run under the family-registry lock.

## A minimal process-local family

This example uses the facade's `runtime` and `metrics` features. It reports
local state only; it deliberately adds no fleet aggregation:

```rust
use std::fmt::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use claviron::metrics::{MetricsFamily, MetricsProvider};
use claviron::runtime::AppState;

struct QueueDepth(AtomicU64);

impl MetricsFamily for QueueDepth {
    fn name(&self) -> &'static str { "example.queue" }

    fn write_prometheus(
        &self,
        _state: &AppState,
        output: &mut String,
    ) -> Result<(), String> {
        output.push_str("# HELP example_queue_depth Current local queue depth.\n");
        output.push_str("# TYPE example_queue_depth gauge\n");
        writeln!(output, "example_queue_depth {}", self.0.load(Ordering::Relaxed))
            .map_err(|error| error.to_string())
    }
}

fn register_queue(provider: &MetricsProvider) -> Arc<QueueDepth> {
    let family = Arc::new(QueueDepth(AtomicU64::new(0)));
    provider.register(Arc::clone(&family));
    family
}
```

The application updates its own store; the provider neither samples it on a
timer nor chooses its synchronization. Metric names, labels, HELP/TYPE metadata,
and values remain the owner's responsibility. A registration key is not an
automatic prefix or a check for duplicate sample names across formatters.

## Local versus fleet observations

Built-in owners generally separate a hot-path store, captured snapshot, merge
rules, and Prometheus formatter. When values must be visible fleet-wide, the
same owner supplies its bounded Orbit codec, publication/collection lifecycle,
and freshness policy. None of those move into this composition crate.

The intended built-in fleet endpoint can be scraped through any serving worker:
its registered families compose the fleet view instead of asking Prometheus
to scrape and sum every worker independently. This does not happen
automatically for a custom local family such as the example above.

Counters and gauges do not all share one merge rule. In-flight work may sum,
current task names may need deduplication, and master-sourced facts must not be
counted once per worker. Each owner defines that behavior explicitly.
Standalone mode renders its local view without worker IPC.

A subsystem may have a more detailed local labeled projection than its fleet
transport can represent. Do not pretend a scalar fleet total retains labels
that were never transported, or mix differently defined projections under one
sample name. Snapshot formats and freshness belong to the owning subsystem.

## Exposition is an ingress concern

This crate supplies no route, HTTP server, authentication, exporter daemon,
scrape interval, or retention database. An HTTP integration chooses where to
call `render_prometheus` and how to protect or expose it. The registry only
composes the families actually registered in that process.

Adding a metric therefore means updating its owner's store, snapshot,
formatter, optional fleet transport, and feature-gated registration together.
Merely adding a field to a local counter store does not make it observable in
all deployment modes.

## Related crates

- [Runtime](../../README.md): shared service registration and process context.
- [HTTP](../../../http/README.md): request/transport metrics and HTTP exposition integration.
- [Tasks](../../../tasks/README.md): task-owned metrics and fleet projections.
- [Upstream](../../../upstream/README.md): origin, request, and resolution observations.
- [Orbit](../../../orbit/README.md): transport primitives used by metric owners.

## Status and license

Part of Claviron's alpha development series; APIs may change. This README
describes the current source tree, not the placeholder package on crates.io.

Licensed under either [MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.
