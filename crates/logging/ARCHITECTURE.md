# Logging - Architecture

`logging` owns what a Claviron process writes about itself and what the
applications it hosts write through it. It has two parts:

1. **The tracing subscriber** — installed once at startup; Rust code keeps
   using `tracing::{debug, info, warn, error}` for its own diagnostics.
   *Implemented.*
2. **The log pipeline** — named channels, an Orbit ring as the buffer, one
   writer per fleet, and adapters that put records where the operator said.
   Access logs, application logs (Laravel, Python) and anything else that is
   a record rather than a diagnostic go through it. *Design; not implemented.*
   Sections marked **(planned)** describe it.

`tracing` stays the instrumentation API. This crate is not a second facade
over it: the pipeline is a back end, and records enter it through an API of
its own, because what they carry — a PHP channel, a Laravel `context` array —
has field names known only at run time, which `tracing` cannot express.

## Why the pipeline exists (planned)

At the rates Claviron serves PHP and Python, per-request filesystem work in
the application breaks. An embedded Laravel reading `.env` on every request
fell over under a benchmark; Monolog in per-request (SAPI) mode stats, opens,
optionally locks and closes its log file on every request that logs, from
every thread at once. The rule this crate applies to logging is the one
applied to the environment: what is constant for a process's life is opened
once, in Rust, and the application only hands over the record.

Claviron also has no access log. The request path must never wait on one.

## The subscriber (implemented)

Single-file compositions may pass `SystemLogConfig` directly to
`Server::system_log` instead of loading `logging.yaml`. The server resolves its
variables against the prepared boot environment; explicit `None` disables that
file sink without opening another config file. Startup truncation is still
master/standalone-only, and workers append.

`pipeline::register_with_config` accepts the existing `LoggingConfig` in hand.
It installs the same channels, fleet ring and writer as file-backed registration;
the default registration retains these definitions. In-hand channels expose no
file observer or automatic reload. System logging needs no channel, and neither
form creates access records by itself.

```text
server / cli entry point
        |
        | choose one logger once
        v
claviron-logging
        |
        | tracing subscriber + EnvFilter
        v
tracing events from every crate
```

### Entry points

- `console_logger(run_mode)` - role-aware console logging for interactive
  or non-systemd runs.
- `journald_logger(run_mode)` - structured journald logging for systemd
  environments.
- `client_logger()` - simpler CLI tool logging.
- `is_running_under_systemd()` - environment probe used by server startup.

Logger installation returns `Result<()>`. The only recoverable setup failure
currently comes from connecting the journald layer.

`claviron::server` is the main caller. It detects systemd via
inherited `JOURNAL_STREAM` / `INVOCATION_ID`, chooses journald when present,
otherwise installs the console logger. Both receive `RUST_LOG` resolved from
the host's prepared environment through their `*_with_filter` variants,
without process-env writes. CLI tools call `client_logger`
directly.

### Global subscriber boundary

The logger functions call `tracing_subscriber::...init()`, which installs a
process-global subscriber. That is a startup-only operation. Do not call them
from request handlers, reload paths, providers, or tests that share a process
with other tracing setup.

### Filtering

The legacy logger entry points and client logger read process `RUST_LOG`.
`console_logger_with_filter` and `journald_logger_with_filter` instead accept
already-resolved directives from the host's snapshot. Both paths fall back to
`info` for absent/invalid filters. Operators set `RUST_LOG` for per-target
tuning; individual crates do not encode their own default filters here.

### Server role labels

Server logging receives the already-resolved runtime `RunMode`; it does not
rediscover process identity from CLI arguments or environment variables:

- `RunMode::Master` prints `[Master]`,
- `RunMode::Worker(id)` prints `[Worker #id]`,
- `RunMode::Standalone` prints no role prefix.

Journald logging adds `ROLE=master|worker` and `WORKER_ID=<id>` for a
master/worker family and neither for a standalone process. Only the numeric
worker ID uses one deliberate, bounded process-lifetime allocation, because
journald custom fields need a `'static` lifetime.

### Console format

RFC3339 local timestamp, coloured level, event target, role, then the inner
message and fields. The formatter is display-only: it does not mutate fields
or interpret messages.

### Journald format

`tracing_journald`, with explicit priority mappings (`error` → error, `warn`
→ warning, `info` → informational, `debug`/`trace` → debug), no ANSI, role as
structured fields. `tracing_journald` 0.3 prefixes event fields with `F_`,
cannot take per-record `SYSLOG_IDENTIFIER`, and always writes the Rust target
as `TARGET`; a journald adapter of the pipeline would write the protocol
itself (see *Adapters*).

## The pipeline (planned)

```text
 producers (any process of the fleet)            one writer per fleet
 ─────────────────────────────────────           ─────────────────────
 access log   (claviron-edge dispatch) ─┐
 claviron_log (PHP, php-embed)          ├─► Orbit ring, own lane ──► drain all lanes
 Python       (gufi)                    │   (kind 253, per node)     group by channel
 Rust         (record API)             ─┘   copy + doorbell,         hand each batch
                                            never waits              to its adapter
                                                                        │
                                                   ┌────────────────────┼──────────────┐
                                                   ▼                    ▼              ▼
                                            file (storage/logs)       OTLP        journald
```

### Channels

A channel is a named destination: an adapter and its target. The operator
declares them; nothing creates one implicitly.

```yaml
# logging.yaml
channels:
  access:  { adapter: file, path: storage/logs/access.log,  format: json }
  laravel: { adapter: file, path: storage/logs/laravel.log, format: line }
  events:  { adapter: otlp, endpoint: http://127.0.0.1:4318 }
```

- Relative paths are resolved against the application's working directory
  (repository rule 5).
- Channel names are resolved to a small numeric id once — at boot for Rust
  producers, at registration for PHP and Python — and the record carries the
  id. A producer that names a channel the configuration does not declare is
  refused at that point, never silently routed somewhere else.
- A Laravel channel (`'driver' => 'claviron', 'channel' => 'laravel'` in
  `config/logging.php`) maps onto a channel here by name. The two sides use
  the same word for the same thing.

File channels accept `truncate_on_startup` (alias `truncate`), default `false`.
An opted-in channel is cleared only when the fleet's master/standalone writer
first opens its sinks. Producer workers do not open files. Reloads, rotation
reopens and managed writer restarts always append, even for a newly added
channel. A configuration check does not start the writer or modify files.

### Records

What travels through the ring:

| Field | Meaning |
|---|---|
| channel | numeric id resolved from the name |
| level | error, warn, info, debug, trace |
| time | wall-clock time taken by the producer, not by the writer |
| target | where it came from: a Rust module path, a PHP `Namespace\Class::method`, a fixed name the channel was given |
| message | text |
| fields | key/value pairs with run-time names (a Laravel `context`, access-log columns) |
| scope | application name and instance in application mode; script in per-request mode |

The source process is not written into the record: the writer knows it from
the lane it read the record from.

A record is encoded once by its producer and decoded once by the writer.
Formatting — JSON, a text line — happens in the writer, off the request path.

### The buffer: one Orbit ring (planned kind 253)

Each process writes to its own lane of a per-node ring (`RingSpec::per_node`)
with `publish_notified`: a copy into shared memory and a doorbell, nothing
the producer waits on.

- **Full ring.** The oldest records are overwritten. The writer sees this as
  loss on its cursor (`poll_lanes` → `loss.overwritten` / `loss.unavailable`),
  counts it per lane and reports it; a producer never blocks. A log line is
  never allowed to slow the request that caused it.
- **A dying worker.** Its last records are in shared memory, not in its heap,
  so the writer still reads them.
- **Geometry.** Capacity and frame size are operator settings with
  `ORBIT_*` defaults in the owning build configuration, and a row in the
  [SHM Kind ID Index](../orbit/ARCHITECTURE.md), added with the kind.
- **Record size.** Frames are fixed-size. A record longer than one frame is
  split into parts — each prefixed with its index and the part count — and
  published as one batch, so a per-node lane holds them consecutively and a
  reader sees all or none. The writer puts them back together per node; a
  record whose parts do not line up is dropped and reported. A record may
  span at most `ORBIT_LOG_RECORD_MAX_FRAMES` frames (32 by default); longer
  ones are truncated and marked. Not `orbit-arena`: the arena is shared by
  unrelated work, and a stack trace should not compete with it for room.

### The writer

Exactly one per fleet, in one of two shapes:

- **A service** — a runnable registered in the master of a master/worker
  family, and in the process itself when standalone.
- **A task with `dispatch: pool`, `concurrency: 1`** — one slot leased
  fleet-wide; the writer runs in whichever eligible process holds the lease,
  and when that process dies another takes the lease and continues from the
  ring. The ring outlives the writer either way.

Which is the default is open (see *Open questions*). Each fleet drains its own
ring — an edge's, a gufi application's and a lumenize application's are
separate fleets, and none reads another's.

- It waits on the ring's readiness and nothing else: woken by a producer's
  doorbell, it drains every lane, groups what it read by channel and hands
  each group to that channel's adapter in one call, then waits again. Under
  load a wake carries many records; when traffic is light each record is
  written as it arrives. There is no flush interval.
- **All I/O happens here.** Producers do none.
- Reading the ring and writing the files happen on one thread of the
  writer's own, not on the async runtime's workers; the async side only
  turns the doorbell into a nudge for that thread.
- **The ring is the only buffer.** The thread reads only as fast as it
  writes, so a slow disk makes the ring overwrite — counted and reported —
  instead of queueing formatted lines in memory. (A first version handed
  batches to the file thread over an unbounded channel; on a slow shared
  disk the backlog grew without bound.)
- On shutdown the writer drains after its producers have stopped, and in a
  master after its workers have exited. Its drain has no deadline of its own
  ([GRACEFUL_OWNERSHIP: NO_IMPLICIT_TERMINATION](../../docs/design/GRACEFUL_OWNERSHIP.md)).

### Adapters

- **file** — appends to one file per channel; formats `json` (one object per
  line) and `line` (human-readable). Reopens its files when rotation asks
  (`claviron.sh logrotate`). First adapter.
- **otlp** (feature `otlp`; facade `log-otlp`) — OpenTelemetry logs over
  OTLP/HTTP, through the SDK's logs bridge (attribute names at run time).
  The writer hands each record to the SDK's `BatchLogProcessor` and returns;
  the processor queues it and sends in batches on its own thread, with the
  SDK's documented batch interval and export timeout. `queue` (required, the
  operator's number) bounds what waits; a slow or absent endpoint drops and
  counts there, and the file channels beside it never wait. `endpoint` is the
  logs URL as written (a collector's is usually `/v1/logs`). `http` only:
  reqwest's blocking client without TLS features (repository rule 9); TLS is
  added with rustls when needed. Records carry `channel`, `node`, `target`,
  their fields and `truncated`; the resource's `service.name` is the process's
  instance name. Measured (octane-app, 12.5 KB Laravel records): no
  difference from a file channel; with the receiver down, requests were not
  held up.
- **Later: a reader outside the server.** Reading the ring does not consume
  it, so a separate process — a task, a daemon — could attach to a fleet's
  log ring and do the sending, taking the network out of the server
  entirely. It needs an observer that can register as a parked reader
  (`FleetObserver` maps read-only today) and records that carry their
  channel's name. Further out, an OTLP collector that reads Orbit itself
  (an Orbit receiver, possibly over `orbit-stream`).
- **journald** — only if asked for; it would speak the native protocol
  itself (fields per key, per-record `SYSLOG_IDENTIFIER`, `TARGET` from the
  record). journald's rate limit is per unit, so a busy access-log channel
  sent there can make journald drop the server's own error lines.

## Producers (planned)

### Access log — in `claviron-edge`

HTTP stays out of this crate. `HostService::dispatch` is where every request
a host serves passes exactly once — bypass paths included — and where the
record is made:

- A request start time is taken by the transport when the request head is
  parsed (hyper, h3, Orbit ingress) and carried as a request extension.
  php-embed answers PHP's SAPI `get_request_time` with it, so
  `$_SERVER['REQUEST_TIME_FLOAT']` includes the admission wait.
- A guard is created at dispatch, learns the status from the reply head, and
  moves into the reply body. It counts bytes as the body goes out and writes
  the record when the body ends or is dropped — so a streamed reply, an
  event stream ended at shutdown and a client that left mid-body are all
  recorded with their real duration and size.
- Three durations: `queue` (admission), `head` (to the reply head), `total`
  (to the end of the body).
- Where the record goes: `logging.yaml`'s `access_log` names the channel
  every host writes to, and a host may name its own or turn records off:

  ```yaml
  g02.example:
    access_log: off               # no record at all — a polled management API
  www.example:
    access_log: www-access        # its own channel; several hosts may share one
  ```

  Unwritten, a host writes to `logging.yaml`'s channel, and with none there
  it writes nothing. A channel a host names must be declared: a misspelt one
  fails the vhost load (boot and reload), never a host whose records go
  nowhere. Each record carries its `host`, so one shared channel still tells
  hosts apart. A request refused by the listener before a host is chosen —
  its `deny` list — has no host and makes no record.

### Application logs — later

Each language gets Orbit at its own framework-neutral log door, not a new
logging API:

- **PHP:** `claviron_log_channel()` / `claviron_log()` in php-embed, and the
  Monolog handler `Claviron\Monolog\ClavironHandler` in the `claviron/monolog`
  package (`crates/logging/php`) — Laravel (claviron-laravel registers the
  `claviron` driver), Symfony or any Monolog user.
- **Python:** `claviron.LogHandler`, a `logging.Handler`, in gufi's `log`
  feature — Django, FastAPI, anything on the standard `logging`.
- **JavaScript:** `claviron:log` (`channel(name).warn(...)`) and the LogTape
  sink `getClavironSink()` in `claviron:log/logtape`, in claviron-js's `log`
  feature. LogTape because its sink is a plain function, so the adapter
  carries no dependency, and its core needs no Node API; pino's transports
  need worker threads and winston's Node streams. Task programs keep routing
  their stdout/stderr by their own `log:` setting (tasks crate).

## The system log file (implemented)

`logging.yaml` may declare a file for the server's own diagnostics:

```yaml
system_log:
  path: ${SYSTEM_LOG_PATH}
  format: line
  truncate_on_startup: false
```

Each process — master and every worker — opens it itself with `O_APPEND`,
before the logger is installed, and a `tracing` layer writes each event to it
with one `write`: the console's line (time, level, target, role, message)
without colour. Independent `O_APPEND` opens append whole lines to one file on
a local disk, so no fd is handed between processes and nothing depends on who
started the master or where its stdout went. The console or journald layer
stays; this one is beside it. `${NAME}` expands from the boot environment, so
a shared configuration directory can name one file per host.

`truncate_on_startup` (alias `truncate`) defaults to `false`. When enabled,
only a new master or standalone launch clears the system log before writing.
Workers always append, including replacement workers. Configuration checks
do not open the file; reloads and rotation reopens never clear it. This option
controls only the system log; each file channel has its own option above.

Writes are direct — no queue, and so no queue size to choose. The system log
is low-volume; a background writer is added only if a measurement asks for
it.

## Rotation

Files are moved by the operator's tool (logrotate) and reopened on
`ReopenLogs`. The event is emitted where the reopen signal lands — USR1 by
default, the binary's choice through `ServerSignals` — in the master or a
standalone process, and the fleet event bridge carries it to each worker. The
system log file swaps its handle under a lock, so a line never splits between
the old file and the new; the pipeline writer drains what it has read into
the old files first and then reopens the channels' files on its own thread.

## The server's own logs stay out of the pipeline

The server's diagnostics — `tracing` events, `[Master]` / `[Worker #n]` lines
— are written by each process itself, to the console or journald, and never
through the ring. A pipeline that carried its own failures would go silent
exactly when it fails: a segment that cannot be opened, a ring that breaks, a
writer that dies would each be reported through the path that broke. The
pipeline's own problems (loss, a failed write, a ring that cannot open) are
reported through `tracing`, which does not depend on it.

## Configuration

`LogChannels` owns `logging.yaml` as a Congif `Configurable`: registered in
every process, bound to the application's frozen configuration environment,
prepared before boot and in the central reload batch. An absent file is the
empty document — no channels — and the pipeline is registered all the same,
so a file that appears later declares channels without a restart: a prepared
source change or an explicit reload reaches `LogChannels`, which rebuilds its
table and wakes the writer.

Channel ids are stable for the life of a process. A channel keeps the id it
was first given, one declared later takes the next, and one withdrawn leaves
its id unused — a producer that resolved a name once never writes into
another channel after the file changed. The writer matches its sinks to the
new table: unchanged channels keep their sink, changed or new ones are opened,
withdrawn ones are closed, and what it had read before the change goes to the
channels as they were. The system log's block in the same file is read by
`SystemLog` before the runtime exists and is not this pipeline's.

## Relationship to other crates

- `claviron` root: re-exports this crate as `claviron::logging`; server
  startup installs the subscriber and, when the pipeline is enabled,
  registers the writer.
- `claviron-edge`: produces access records; this crate does not know HTTP.
- `tasks`: child-process stdout/stderr routing stays there.
- `orbitive`: the ring, consumed through its public facade.
- Database-backed audit rows remain the application's own data, written in
  its own transactions. This crate never writes to a database.

## Invariants

- One tracing subscriber per process, installed at startup.
- Honor `RUST_LOG`; no per-crate filters here.
- Journald output structured and ANSI-free; console output role-labelled.
- **(planned)** Producers never wait and never do I/O; the writer does all
  of it.
- **(planned)** A channel exists only if the operator declared it.
- **(planned)** A lost record is counted and reported, never silent.
- **(planned)** No timer drives the writer; it is woken by producers.
- The server's own diagnostics never go through the pipeline.
- No database writes.

## Open questions

- The ring's default capacity and frame size.
- The writer's default shape: master service or pool-dispatched task.

## JSON metadata ownership

The writer owns the time/level/channel/node/target/message/truncated envelope.
Context fields never replace it: collisions, including the reserved `fields`
container name itself, are nested under `fields`; non-colliding context remains
flat for access-log compatibility. This collision rule also applies to absent
optional metadata, so an empty native target/message cannot be forged by a
context field. Types are preserved. No producer-side lookup or hot-path
normalization is added.

## Record budget

Encoding reserves the mandatory 21-byte version-1 envelope before copying target
or message bytes. Each text prefix respects UTF-8 and its wire length width.
Insufficient envelope capacity yields no record; the channel emitter reports
`RecordCapacity`. Publication checks both total bytes and checked u8 part count
against the build-declared geometry before writing any batch. No ring-size or
wire-version change is made.
