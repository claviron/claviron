# Metrics - Architecture

`metrics` owns the process-wide metrics composition point. Its
concrete `MetricsProvider` is registered only when the optional metrics feature
is enabled; subsystem crates contribute independently owned `MetricsFamily`
implementations, and the provider renders them in stable name order.

All domain stores, snapshots, merge rules, Prometheus formatters, Orbit codecs,
publishers, collectors, and ring resets live in their owning subsystem crates.
The provider itself does not know any family and does not know Orbit, IPC,
HTTP, supervisor, PHP, tasks, files, or application code.

```text
transport / http / php / tasks / files
        | register owned MetricsFamily values
        v
claviron_runtime::metrics::MetricsProvider
        |
        | compose Prometheus formatting
        v
HTTP /metrics endpoint
```

## Role

Every metrics-enabled process has one concrete `MetricsProvider`. Registration
is generic and type-checked (`register<T>`); metric families are ordinary
objects, not dynamic runtime lifecycle providers. Re-registering the same
stable family name replaces it, so subsystem registration is idempotent.

Domain stores remain process-local in their owners. Hot paths update them with
atomics or small in-process stores; transport and lifecycle policy stay in the
owner's separate provider modules.

## Feature Gates

This module is selected by Runtime's `metrics` feature. Owners compile and register their
families behind their own `metrics` and optional `orbit` features. A disabled
subsystem therefore disappears without changing this composition module.

## Store / Snapshot / Formatter Pattern

Each metric family follows the same shape:

1. A process-local static store, usually backed by atomics.
2. Increment/decrement/set helper methods used by subsystem hot paths.
3. A `Snapshot` type deriving `Serialize` and `Deserialize`.
4. `snapshot()` / `capture()` methods to copy live values into the snapshot.
5. `merge()` for fleet aggregation where worker values must be
   folded together.
6. `write_prometheus()` on the snapshot, plus a store wrapper that calls
   `snapshot().write_prometheus(...)`.

This shape is the contract. If a metric family skips the snapshot or formatter,
it may work in one process but it will not survive worker aggregation or the
standalone endpoint consistently.

## Fleet Aggregation

Prometheus must not scrape every worker and sum the results itself. Owners
publish compact snapshots through their Orbit families; a worker-facing
`/metrics` endpoint asks each registered family to fold fresh samples into one
fleet-wide Prometheus response. Scraping any one worker is sufficient.

Because of that, any metric value that lives in workers and must be visible
fleet-wide needs a serializable snapshot and an explicit Orbit transport owned
by its subsystem. Fixed-width scalar families may use compact records;
dynamic/labeled families need their own keyed or row/chunk Orbit metric family.

`http` follows that owner-local rule with two stable families rather
than preserving the old endpoint's accidental grouping. `HttpMetricsFamily`
(`claviron.http`) owns request/cache values and exact 65-byte kind `224`;
`ServerMetricsFamily` (`claviron.server`) owns transport/connection values and
exact 57-byte kind `225`. Standalone reads each store directly. In fleet mode
whichever worker handles `/metrics` merges the newest fresh sample per worker
from each independent ring. `local` is therefore a source choice, not a
separate metric family or public name.

Upstream metrics demonstrate the owner-local shape. Standalone mode exports
the full labeled rows (`profile`, `origin`, probe source/result, status class).
Fleet mode exports scalar aggregate counters through a dedicated upstream Orbit family:
requests, request errors, probes, probe failures, state transitions, and
configured live capacity. The store, snapshot, codec, publisher, collector,
ring reset, and `MetricsFamily` all live in `upstream`; neither this
crate nor `supervisor` knows the upstream metric type. A future
label-preserving cluster-wide implementation should add a bounded upstream
row/chunk Orbit family instead of flattening those labels into a shared
transport record.

`php-fcgi` owns the FCGI/FPM split. Standalone mode prints FCGI
request/stream counters plus the full `claviron_fcgi_*{pool=...}` rows from
local process state. Fleet mode exports those request/stream counters and the
critical scalar capacity view (`live`, `idle`, `max_live`) through dedicated
kind `215`. Pool labels can be added later with a bounded row/chunk family if
operators need cluster-wide per-pool detail.

PHP-FPM `/status` metrics are also owned by `php-fcgi` and are
master-sourced. The FPM scraper runs on master
and standalone only. In fleet mode the master publishes each scraped FPM
pool row into keyed kind `216`, and worker `/metrics`
collects those rows by pool key. This keeps FPM status out of worker-local
metrics and out of the supervisor's process-control state.

`php-embed` owns the `claviron_ephp_*` store, snapshot, kind `214`
codec, publisher, collector, ring reset, family, and its
`php.yaml::metrics` policy. `php` owns no metrics. There is no generic
PHP total: FCGI and E-PHP are reported as separate runtime families.

Task metrics follow the same owner-local rule. `tasks` owns its store,
snapshot, three Orbit codecs, publisher, collector, ring reset, and
`MetricsFamily`. Standalone mode renders the full local snapshot; fleet mode
publishes service scalars, per-task counters and metadata, and per-task-role
gauges. The existing kind `218` record carries compact task kind/schedule
metadata, so `claviron_task_info` needs no fourth ring. The fleet formatter
derives `claviron_tasks_running` from distinct merged task names whose
`in_flight` value is nonzero; it never sums process-local running gauges.

Every Orbit metric snapshot owns a per-kind `RingSpec`. Scalar families use
the `orbit-metrics` snapshot scan window (`1_024` slots), and the ordinary
keyed FPM family uses its keyed scan window (`4_096` slots). The
high-cardinality task and task-pair families owned by `tasks` use a
documented Claviron-specific
`16_384`-slot window. These are current-state scan bounds, not metric-history
retention promises. Fixed-width codecs set payload capacity to their exact wire
length; FPM/task codecs containing strings retain the existing 256-byte total
frame budget. Adding fields therefore requires an explicit codec and ring-spec
review rather than silently increasing a fleet-wide slot size.

Supervisor process metrics follow the same ownership rule. The supervisor
owns its compact master/worker snapshot, Orbit publisher/collector, and
Prometheus formatter. It may expose process role, worker id, and PID because
those identities are intrinsic to process management. Domain families must
still publish fleet totals without copying supervisor worker labels.

Do not solve this by requiring external scrapes of every worker. The HTTP
metrics route should expose one aggregated view from whichever process handles
the scrape.

## Standalone Mode

Standalone mode has no worker IPC. The HTTP metrics endpoint reads the local
metric statics directly and writes Prometheus text.

Each owning subsystem registers its own family with `MetricsProvider`; the HTTP
endpoint only composes registered output. If a metric is added or removed, both
paths must stay honest:

- fleet mode: worker snapshot -> Orbit bridge -> merge -> Prometheus output,
- standalone mode: local static -> Prometheus output,
- standalone-only families: local static -> Prometheus output only.

It is not enough for a metric to exist in an owner store. Its family must be
registered for every deployment mode that can produce it.

## Prometheus Formatting

Prometheus text formatting lives with each owning snapshot type. This
keeps the names, HELP strings, TYPE declarations, and values canonical for
both standalone and fleet modes.

Higher-level endpoints should compose family formatters rather than reformat
the same values themselves. That prevents standalone and aggregated outputs
from drifting.

## Merge Semantics

Most worker snapshots merge by field-wise addition:

- counters sum across workers,
- in-flight gauges sum across workers,
- cache bytes sum across workers,
- per-task or per-pool maps merge by key.

If a future metric does not obey simple addition, document that in the snapshot
type and encode the merge rule there. Do not leave special aggregation rules
hidden in a transport endpoint.

## What Does Not Belong Here

Do not add:

- Axum, Hyper, Tower, Tokio service dependencies,
- subsystem lifecycle policy beyond the concrete composition provider,
- supervisor process-control or liveness state,
- PHP/FPM clients,
- file cache logic,
- task process management,
- database writes,
- labels that require request/vhost context unless the producing subsystem
  passes pre-computed primitive values.

This crate is not a metrics collector daemon. `MetricsProvider` composes
registered families; each owner decides how its family captures local state or
collects a fleet view.

## Adding A Metric

When adding a metric, update all relevant layers in one change:

1. Add the store field and helper method in the appropriate family.
2. Add the value to the serializable snapshot.
3. Add `capture()` / `snapshot()` wiring.
4. Add `merge()` wiring if workers can produce the value.
5. Add Prometheus HELP/TYPE/sample output in `write_prometheus()`.
6. If worker-visible, add an owner-local Orbit codec, publisher, collector,
   freshness policy, and master ring reset. Do not add it to supervisor health.
7. Register the owner's `MetricsFamily`; the HTTP endpoint composes it without
   knowing the family type.
8. Check owner feature gates so disabled subsystems still compile.

Removing a metric follows the same list in reverse. Leaving a stale owner
registration or transport record is worse than not having the metric at all.

## Invariants

- Keep `MetricsProvider` independent of subsystem and transport types.
- Register one concrete provider; extend it with generic metric families rather
  than runtime `register_dyn` lifecycle bindings.
- Stores are process-local; cross-process visibility goes through snapshots.
- Worker metrics are composed into one fleet view; operators do not aggregate
  per-worker scrapes.
- Standalone and fleet `/metrics` outputs must not expose partial metric
  families. If only a scalar fleet projection exists, it must use distinct
  Prometheus names from the labeled standalone family.
- Snapshot types are the Orbit transport and formatting contract; keep them explicit.
- Prometheus names and HELP/TYPE metadata live beside the snapshot formatter.
