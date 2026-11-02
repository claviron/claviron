# HTTP benchmarks

Compare native Hyper/Axum with Claviron through decoded dispatch, serial HTTP/1
round trips and concurrent HTTP/1 server CPU measurements.

## Layout

| Directory | Measurement |
| --- | --- |
| `dispatch/native/` | Native Hyper service and Axum router, with a JSON response. |
| `dispatch/edge/` | Registered Edge owners; paired plain controls and a separate JSON workload. |
| `transport/http1/native/` | Real Hyper and `axum::serve` over persistent loopback HTTP/1.1. |
| `transport/http1/edge/` | Native Edge HTTP/1 driver, including host selection, for Other, Axum and Routable. |
| `transport/http1/concurrent.rs` | Separate server/client processes; concurrent plain requests and server CPU accounting. |
| `support/` | Shared workloads, typed config fixtures and preparation outside timing. |

Dispatch excludes HTTP decoding, host selection, sockets and TLS. Serial HTTP/1
measurements include client/server protocol work and socket IO, with one
connection and one in-flight request on a current-thread Tokio runtime. Edge
HTTP/1 targets include the native connection driver and per-request host selection.

## Decoded dispatch

Each target compares the same `200 OK`, `text/plain` response with a small static
body. Host configuration and application registration are prepared before timing;
the fixture declares no optional policies or backend execution.

| Target | Native control | Registered Claviron owner |
| --- | --- | --- |
| `dispatch-edge-other` | Hyper service | `OtherHandler` |
| `dispatch-edge-axum` | The same Axum router | Dedicated Axum router |
| `dispatch-edge-pipeline` | Hyper service | Config-compiled plan with a `Routable` contribution |

Each produces three measurements:

- `native`: direct application call through response consumption.
- `prepare`: synchronous host admission and request-context preparation;
  application execution and prepared-request destruction are outside timing.
- `host`: the registered application through host admission, execution and
  response consumption.

Request construction is outside timing. `host - native` shows the extra full-call
cost, including body adaptation and response completion. `prepare` isolates only
the synchronous prefix; it does not include later async admission/dispatch work.

## HTTP/1 after connection establishment

These targets use the same static `/plain` response on one persistent connection:

| Target | Server path |
| --- | --- |
| `http1-plain-hyper` | Native Hyper service. |
| `http1-plain-axum` | Native `axum::serve` with the same Axum router. |
| `http1-plain-edge-other` | Native Edge driver and registered Other handler. |
| `http1-plain-edge-axum` | Native Edge driver and registered Axum router. |
| `http1-plain-edge-routable` | Native Edge driver and registered Routable contribution. |

Axum selects `RustRouter::Axum`; an Other handler selects `RustRouter::Other`.
Both enter the same compiled `HostExecution`. Routable contributes host layers
and handlers; it is a registration path, not another router kind.

All targets enable TCP_NODELAY and HTTP keep-alive, disable half-close and
pipeline flush, and run without a header-read timer. The Edge fixture overrides
Claviron's half-close default to match native `axum::serve`; production defaults
are unchanged. Native controls retain Tokio's `TcpStream` IO, while Edge retains
its production `TcpIo` adapter. The comparison includes that IO difference.

The Edge targets call `HttpServer::connection_handler`, the accepted-stream path
also used by the native listener. Request timing includes protocol decoding,
authority validation, the current vhost/application lookup, connection-local
metadata reuse, host policy, native body adaptation, dispatch and response IO.
The benchmark does not duplicate those decisions.

Configuration, application publication, socket bind/accept, connection admission
and initial setup precede timing. Every target's preflight sends the same Host
sequence: known, unknown, alias, then known again. Edge must return 404 for the
unknown host; native controls have no host routing and return their usual reply.
Every measured response is checked. Shutdown joins the native connection and
sessions and checks that their permits were released.

Compare Other/Routable with Hyper, and Edge Axum with native Axum. These are serial
client/server round-trip times, including client work; keep them separate from
the narrower dispatch measurements and concurrent server CPU/request results.

## Concurrent HTTP/1 server CPU

`http1-concurrent` accepts one of five owners: native Hyper, native
`axum::serve`, or registered Edge Other/Axum/Routable. The server is a child
process with its own main multi-thread Tokio runtime. A separate client process
keeps the declared TCP connections open, with one in-flight request per connection.
Connections and both runtime thread counts are required inputs.

`--server-execution shared` serves HTTP on the main runtime. Selecting
`current_thread` places bind, accept and connection tasks on one additional
current-thread executor using Claviron's production `CurrentThreadDriver` and
listener execution specification. All native and Edge controls use the same
cold placement setup; native HTTP still runs directly through Hyper or
`axum::serve`. Setup and executor teardown remain outside measurement.
The benchmark checks the selected runtime flavor before binding.

`--server-threads` always controls the main runtime's thread budget, including
in `current_thread` mode. It does not count HTTP executors or processes.
Results record both the execution mode and effective HTTP runtime thread count.
Each run has one serving process and one listener; a 32-worker fleet is a
different measurement topology.

Each Edge connection enters the production `ConnectionHandler`, creating its
own connection context and host-policy cache. Host/application publications
remain shared; the benchmark does not clone one connection's HostService into
all connections. The H1 settings and response match the serial cases.

Setup, handshakes, host preflight and warmup precede measurement. The declared
measurement duration is divided into sample windows. A client-side timer stops
new requests between replies; outstanding replies finish and are counted. Every
response must pass the same status/content/body checks. Request or connection-task
failure invalidates the run.

The server samples Unix `getrusage(RUSAGE_SELF)` user+system CPU before and after
each window. This includes all server runtime threads and the control thread,
excludes client CPU, and adds no clock reads to request handling. Raw windows
retain completed requests, wall time, server CPU and per-connection counts.
Throughput uses completed requests / observed wall time; CPU/request uses total
server CPU / completed requests. These are not request latency percentiles.

Keep connections and client threads fixed while varying server threads from
one to several to expose concurrency costs. This includes Arc and activity-counter
contention, allocation and scheduling; it does not attribute the entire change
to Arc. Run every native and Edge owner with matching inputs.

## Run

From the repository root:

```sh
python3 scripts/bench-http.py http1-plain-edge-other \
  --warmup-seconds 3 --measurement-seconds 5 --samples 100
```

Substitute a target from the HTTP/1 table to compare another host owner. The script
builds the optimized benchmark first, using the same `edge,axum` features for all
controls; no compilation occurs during measurement. Times and sample count are
explicit arguments, and Criterion requires at least ten samples.

For concurrent server CPU, all five targets use the same `http1-concurrent`
executable with different owners:

```sh
python3 scripts/bench-http.py http1-concurrent-edge-other \
  --connections 1024 --server-threads 4 --client-threads 4 \
  --server-execution shared \
  --warmup-seconds 3 --measurement-seconds 5 --samples 1
```

Substitute `http1-concurrent-hyper`, `http1-concurrent-axum`,
`http1-concurrent-edge-axum` or `http1-concurrent-edge-routable`. Repeat with
`--server-threads 1`, keeping other inputs fixed. These targets require Unix
process CPU accounting and do not use Criterion's serial RTT estimator.
Use `--samples 1` for one uninterrupted load window; multiple samples drain
outstanding replies between windows.

Repeat each case with `--server-execution current_thread`, leaving connections,
main-runtime threads, client threads and timing unchanged. Compare native Hyper
with Edge Other, and native Axum with Edge Axum, within each execution mode.
Compare each owner across modes to measure execution placement separately.
`shared` remains the default when the execution argument is omitted.

For the narrower decoded-dispatch comparison:

```sh
python3 scripts/bench-http.py dispatch-edge-other \
  --warmup-seconds 3 --measurement-seconds 5 --samples 100
```

For the secondary JSON workload, use `dispatch-hyper`, `dispatch-axum` or
`dispatch-edge-json`. Native HTTP/1 JSON controls are `http1-hyper` and
`http1-axum`; pass the same timing arguments. Keep JSON workloads separate from
the full Edge plain-path comparison. Each completed response is checked for
the expected status, content type and body.

## Results

Local records and Criterion data stay under ignored `.benchmarks/http/`. Records
include the Git commit, dirty-source digest, binary hash and measurement controls.
`--compare RUN_ID` requires matching controls; automatic history comparisons use
the latest compatible run of the same target. Source/binary changes during a run
invalidate it. Arithmetic differences do not establish statistical significance.

Keep decoded calls, serial RTT and concurrent server CPU results separate.
The launcher rejects comparisons with different measurement boundaries or
thread/connection controls. None of these targets reports request p99.

An explicit `--compare RUN_ID` may compare concurrent runs that differ only in
HTTP execution placement; automatic history comparisons keep the same mode.
Other measurement controls must still match. CPU totals include every serving
process thread, not just the HTTP executor.
