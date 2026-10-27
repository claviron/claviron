# claviron-logging

Process diagnostics and declared application log channels for Claviron.

This crate has two distinct paths. Rust diagnostics use a process-wide
`tracing` subscriber, writing to the console or journald and optionally a
system-log file. Application and access records can use named channels,
buffered through Orbit and drained by one fleet writer.

Applications normally use `claviron::logging` through the root crate's
`logging` feature. Select `log-pipeline` for named channels and `log-otlp` for
the optional OpenTelemetry logs adapter. System diagnostics do not require the
pipeline, and pipeline failures are reported outside it.

## Install diagnostics once

`console_logger` and `journald_logger` install role-aware subscribers;
`client_logger` provides the simpler CLI format. The `*_with_filter` entry
points accept directives already resolved by the host rather than reading the
live process environment. The legacy entry points read `RUST_LOG`; absent or
invalid directives fall back to `info`.

Logger installation is process-global startup work, not a request handler or
reload operation. Claviron's server composition chooses the subscriber;
application components continue to emit ordinary `tracing` events.

Console lines include a local RFC3339 timestamp, level, target, and runtime
role. Journald output is structured and ANSI-free. The resolved `RunMode`, not
a second environment-based identity guess, supplies master/worker labels.

## System log versus channel log

`logging.yaml` can declare both independently:

```yaml
system_log:
  path: storage/logs/system.log
  format: line
channels:
  application:
    adapter: file
    path: storage/logs/application.log
    format: json
```

Each process opens its own system-log handle for append before installing the
subscriber. That file receives direct, uncoloured diagnostic writes beside the
console/journal sink. It does not depend on an Orbit writer being healthy.
`SystemLogConfig` can also be passed directly to the enclosing server; no
additional file is required in that composition.

Channel file IO instead belongs to the pipeline writer. Relative paths are
application-working-directory relative, not YAML-directory relative. File
sinks accept `truncate_on_startup` (default false): an opted-in reset belongs
only to a new master/standalone startup. Worker replacement, reload, managed
writer restart, and rotation reopen append instead.

## Emit records without sink IO

`LogChannels` resolves declared names to `ChannelId` values. Resolve a channel
once during composition, then hand its ID to producers. Unknown or withdrawn
channels are refused rather than silently redirected.

```rust
use claviron::logging::pipeline::{
    ChannelId, EmitError, Level, LogChannels, Record,
};

fn write_notice(
    channels: &LogChannels,
    channel: ChannelId,
    message: &str,
) -> Result<(), EmitError> {
    channels.emit(channel, &Record {
        level: Level::Info,
        target: "application",
        message,
        fields: &[],
    })
}
```

`Record` borrows its target, message, and typed fields until encoding. The
producer timestamps and encodes it, publishes to its fleet lane, and signals
readiness. It performs no file or network sink IO and does not wait for export.
Large records use bounded multipart frames; content beyond the configured
record geometry is truncated and marked.

## One writer, bounded loss

The pipeline requires a shared-memory Orbit fleet, including in standalone
composition. A process-local in-memory fleet is rejected because it supplies
neither the shared lane nor writer readiness required here. The current native
readiness integration covers Linux, FreeBSD, and macOS; unsupported platforms
fail explicitly rather than starting a polling fallback.

One managed writer runs in the master or standalone process. Native readiness
wakes its async side; a dedicated thread drains lanes, formats records, and
writes sinks. There is no periodic file flush loop or unbounded formatted-record
backlog behind a slow disk. The ring can overwrite old records; the writer
counts and reports detected loss through diagnostics.

Emission success is not durable delivery or an acknowledgement from a remote
collector. A newly started writer begins at the current ring head, not a
persistent replay checkpoint. Keep financial/audit data requiring transactional
durability in its application's database, not only in this log pipeline.

## Configuration, reload, and rotation

`pipeline::register` composes `LogChannels`, the ring, and writer. Congif prepares
`logging.yaml` before boot and during reload. An absent file declares no
channels; a later prepared source change or explicit reload can add them.
Channel IDs are not recycled into another channel during the process lifetime.
The writer retains unchanged sinks and reconfigures changed destinations.

`pipeline::register_with_config` accepts the same `LoggingConfig` as boot
options without a file observer. It does not create access records by itself.
`access_log` names a declared channel; the HTTP owner produces the actual
request records and may apply host-specific selection.

Rotation moves files outside this crate, then requests `ReopenLogs`. Each
process reopens its system log, and the writer reopens channel files on its own
thread after finishing already-read records. Writer shutdown drains remaining
work and joins its thread without inventing a separate graceful deadline.

## Optional exporters and integrations

The `otlp` implementation feature includes `pipeline` and adds OTLP/HTTP logs.
The channel declares the complete endpoint and a required bounded `queue`.
The SDK batch processor owns its export scheduling and timeout behavior; a
full export queue can drop records instead of blocking file channels. The SDK
reports the first queue-full drop and the aggregate count at shutdown
through `tracing`, independently of the pipeline and subject to its subscriber
filter. It does not emit a warning for every dropped record. The current
exporter is HTTP-only, without TLS support. Journald is available for
process diagnostics, not as a pipeline channel adapter.

- [Runtime](../runtime/README.md): prepared environments, lifecycle, reopen events.
- [Orbit](../orbit/README.md): shared log buffer and native readiness.
- [Laravel](https://github.com/claviron/lumenize): framework/Monolog channel integration.
- [Tasks](../tasks/README.md): stdout/stderr and optional named-channel routing.
- [Edge](../edge/README.md): HTTP access-record production.

## Status and license

Part of Claviron's alpha development series; APIs and configuration may change.

Licensed under either [MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.

JSON output reserves `time`, `level`, `channel`, `node`, `target`, `message`,
`truncated` and `fields` for record metadata. Context fields with these names
are preserved under the `fields` object; they cannot replace metadata, even
when an optional metadata value is absent. Other context fields remain at the
top level with their original scalar types.

The binary record budget includes its fixed envelope and target as well as the
message and fields. Oversized text is shortened on UTF-8 boundaries and marks
the record truncated. A budget smaller than the empty envelope is rejected;
record publication never exceeds the declared maximum number of frames.
