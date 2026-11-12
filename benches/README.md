# Benchmarks

Compare native Hyper/Axum with Claviron through decoded dispatch, serial HTTP/1
round trips and concurrent HTTP/1 server CPU measurements.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the file index and call paths.

## Layout

| Directory | Measurement |
| --- | --- |
| `cache/geoip.rs` | Moka and the SHM-backed Orbit cache with GeoIP-sized values. |
| `dispatch/native/` | Native Hyper service and Axum router, with a JSON response. |
| `dispatch/edge/` | Registered Edge owners; paired plain controls and a separate JSON workload. |
| `transport/http1/native/` | Real Hyper and `axum::serve` over persistent loopback HTTP/1.1. |
| `transport/http1/edge/` | Native Edge HTTP/1 driver, including host selection, for Other, Axum and Routable. |
| `transport/http1/concurrent.rs` | Separate server/client processes; concurrent plain requests and server CPU accounting. |
| `transport/link/concurrent.rs` | Supervised client and Runner workers over Orbit Link. |
| `transport/h2/concurrent.rs` | The same supervised families over TCP/H2c. |
| `transport/h2/unix.rs` | The same supervised families over UDS/H2. |
| `harness/`, `config/link/` | Shared boot, phase control, process measurements and transport fixtures. |
| `results/` | JSON reports of the two-process targets, by transport and Claviron commit, kept for comparison. |
| `support/` | Shared workloads, typed config fixtures and preparation outside timing. |

Dispatch excludes HTTP decoding, host selection, sockets and TLS. Serial HTTP/1
measurements include client/server protocol work and socket IO, with one
connection and one in-flight request on a current-thread Tokio runtime. Edge
HTTP/1 targets include the native connection driver and per-request host selection.

## GeoIP cache

```sh
cargo bench --features orbit --bench geoip-cache
```

Requires `orbit` from `orbitive-cli` on PATH. The benchmark opens a fresh SHM
fleet for each Orbit case and clears it once its members have dropped.

The workload cycles through 1,024 resident IPv4 keys with two-byte country
codes and a 30-minute TTL. Moka uses GeoIP's `IpAddr → Arc<str>` representation.
Orbit uses string keys and worker-local bytes, without L2. `shm-one` reserves
two lanes and uses one member to write and read. The peer cases use two members
in one process with separate cache connections and reserve either 2 or 40 lanes.
One member writes before timing, and the other consumes those mutations and reads.

`l1` measures local value lookup. `async` uses the asynchronous facade.
Peer mutation delivery happens explicitly during setup. `sync` includes the synchronous bridge's per-call Tokio
`block_on`, and `sync-ip` includes formatting the IP into a string. Misses
look up one absent key. Database lookup, writes and concurrent mutation
traffic are outside the timed workload.

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
- `bind`: retain the request and connection facts without executing host layers;
  prepared-request destruction is outside timing.
- `host`: the registered application through host admission, execution and
  response consumption.

Request construction is outside timing. `host - native` shows the extra full-call
cost, including body adaptation and response completion. `bind` isolates only
request binding; it does not include admission or dispatch work.

## HTTP/1 after connection establishment

These targets use the same static `/plain` response on one persistent connection:

| Target | Server path |
| --- | --- |
| `http1-hyper` | Native Hyper service. |
| `http1-axum` | Native `axum::serve` with the same Axum router. |
| `http1-plain-edge-other` | Native Edge driver and registered Other handler. |
| `http1-plain-edge-axum` | Native Edge driver and registered Axum router. |
| `http1-plain-edge-routable` | Native Edge driver and registered Routable contribution. |

Axum selects `RustRouter::Axum`; an Other handler selects `RustRouter::Other`.
Both enter the same compiled `HostExecution`. Routable contributes host layers
and handlers; it is a registration path, not another router kind.

All targets enable TCP_NODELAY and HTTP keep-alive, disable half-close and
pipeline flush, and run without a header-read timer. The Edge fixture states
half-close off explicitly; that is also Claviron's default and what native
`axum::serve` does. Native controls retain Tokio's `TcpStream` IO, while Edge
retains its production `TcpIo` adapter. The comparison includes that IO
difference.

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

`--mode` selects what carries HTTP/1: `plain` (the default) or `tls`. A new
mode is measured with Edge Other against native Hyper only; the other owners
stay on `plain`. In `tls` mode each serving process makes its own authority
and server certificate in memory and reports the authority to its client, so
no file is read or written. The native control uses the same crypto provider,
certificate type and ALPN protocol through `tokio-rustls`. The Edge target
enters the production TLS `ConnectionHandler`, including the handshake, the
server-name host selection and the request authority check, and opens the
real fleet TLS session table under a fleet name of its own process, removing
it on shutdown. Every handshake completes during connection setup, before
warm-up; the measured windows contain persistent connections only. Runs of
different modes are never compared with each other.

`--http-config` names an `http.yaml` with one listener, and its `execution`
places the server. `inherit` uses the main runtime. `dedicated` uses
Continuo's `Dedicated(CurrentThread)` with Claviron's `CurrentThreadDriver`
for bind, accept and connection tasks. All native and Edge controls use the same
cold placement setup; native HTTP still runs directly through Hyper or
`axum::serve`. Setup and executor teardown remain outside measurement.
The benchmark checks the selected runtime flavor before binding.

`--server-threads` always controls the main runtime's thread budget, including
in `dedicated` mode. It does not count HTTP executors or processes.
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

The server samples Unix `getrusage(RUSAGE_SELF)` before and after each window
and reports user and system CPU separately. This includes all server runtime
threads and the control thread, excludes client CPU, and adds no clock reads to
request handling. Raw windows retain completed requests, wall time, user and
system CPU and per-connection counts. Throughput uses completed requests /
observed wall time; CPU/request uses server CPU / completed requests. These are
not request latency percentiles.

### Reading the result

- **User CPU/request is the serving code; system CPU/request is the kernel and
  how the runtime uses it.** A host-path cost appears in user time and stays
  about the same across execution modes. System time changes with thread count
  and load, and dominates a lightly loaded multi-thread runtime.
- **HTTP-thread utilization** is server CPU over wall time times HTTP threads.
  Near 100% the server set the pace and its throughput is a server result.
  Well below that, the client set the pace: CPU/request then includes time the
  server spent waiting, and throughput says nothing about the server. Use
  `dedicated`, or fewer server threads than client threads, when the
  comparison is about serving cost.
- **Allocations/request** counts the serving process's allocation calls
  (`alloc`, zeroed `alloc` and `realloc`) during its warm-up load, divided by
  the warm-up's completed requests. Counting is off in every CPU window, so a
  target that allocates more is not charged for being counted. The figure is
  the same on any host and across runs, which makes it the first thing to
  compare when a serving path changes.

### Paired control

`--control` names a second owner. Both servers stay up with their own
connections, and their measured windows alternate: target then control in one
sample, control then target in the next. Each owner still receives the full
measurement duration. The report adds the control's own result and a paired
comparison: the CPU/request ratio from the totals, the same ratio for each pair
of adjacent windows, and the user, system and allocation differences.

Absolute CPU/request moves with the machine between runs; the ratio inside one
run does not, because both owners saw the same conditions. Use at least four
samples so the per-pair range is visible. Naming an owner as its own control
measures that range with no real difference, which is the floor below which a
ratio means nothing.

Keep connections and client threads fixed while varying server threads from
one to several to expose concurrency costs. This includes Arc and activity-counter
contention, allocation and scheduling; it does not attribute the entire change
to Arc. Compare Edge Other and Edge Routable with Hyper, and Edge Axum with Axum.

## Run

From the repository root:

```sh
python3 scripts/bench-http.py http1-plain-edge-other \
  --warmup-seconds 3 --measurement-seconds 5 --samples 100
```

Substitute a target from the HTTP/1 table to compare another host owner. The script
builds the optimized benchmark first, using the same `edge,axum,tls` features for all
controls; no compilation occurs during measurement. Times and sample count are
explicit arguments, and Criterion requires at least ten samples.

For concurrent server CPU, all five targets use the same `http1-concurrent`
executable with different owners. Name the native owner as the control:

```sh
python3 scripts/bench-http.py http1-concurrent-edge-other \
  --control http1-concurrent-hyper \
  --connections 1024 --server-threads 4 --client-threads 4 \
  --http-config benches/config/http1/dedicated.yaml \
  --warmup-seconds 3 --measurement-seconds 5 --samples 4
```

Substitute `http1-concurrent-edge-axum` with `--control http1-concurrent-axum`,
or `http1-concurrent-edge-routable` with the Hyper control. These targets
require Unix process CPU accounting and do not use Criterion's serial RTT
estimator. Each sample drains outstanding replies before the next window
begins. Without `--control` one owner is measured alone and no ratio is
reported.

Add `--mode tls` to the Edge Other/Hyper pair to measure the same load over
TLS. Read its ratio beside the `plain` ratio of the same pair: both sides pay
for the same record encryption, so the difference between the two ratios is
what the TLS serving path adds.

Repeat each case with `benches/config/http1/inherit.yaml`, leaving connections,
main-runtime threads, client threads and timing unchanged, to measure execution
placement separately. Check its HTTP-thread utilization before reading its
CPU/request.

For the narrower decoded-dispatch comparison:

```sh
python3 scripts/bench-http.py dispatch-edge-other \
  --warmup-seconds 3 --measurement-seconds 5 --samples 100
```

For the secondary JSON workload, use `dispatch-hyper`, `dispatch-axum` or
`dispatch-edge-json`; pass the same timing arguments. Keep JSON workloads
separate from the full Edge plain-path comparison. Each completed response is
checked for the expected status, content type and body.

## Results

Local records and Criterion data stay under ignored `.benchmarks/http/`. Records
include the Git commit, dirty-source digest, binary hash and measurement controls.
`--compare RUN_ID` requires matching controls; automatic history comparisons use
the latest compatible run of the same target. Source/binary changes during a run
invalidate it. Arithmetic differences do not establish statistical significance.

Keep decoded calls, serial RTT and concurrent server CPU results separate.
The launcher rejects comparisons with different measurement boundaries,
thread/connection controls or paired control. None of these targets reports
request p99.

Serial RTT is a round-trip time on one connection and includes the client. A
serving-path difference well under a microsecond is below what it resolves;
use the paired concurrent measurement for that.

An explicit `--compare RUN_ID` may compare concurrent runs that differ only in
HTTP execution placement; automatic history comparisons keep the same mode.
Other measurement controls must still match. CPU totals include every serving
process thread, not just the HTTP executor.

## Supervised Link transports

These targets measure Runner applications directly, through the production Link
backend or upstream client. Both sides boot Claviron with their own supervisor
and fleet. Client workers generate requests from a managed Runnable; they open
no frontend HTTP listener. Application workers use the same benchmark router. The comparison includes each
backend's target selection, admission and pool model; it does not isolate raw
socket or shared-memory copying cost.

```sh
cargo bench --locked \
  --features supervisor,rust,bench,runner-orbit,runner-socket,upstream \
  --bench link-concurrent -- \
  --workload plain --client-workers 2 --application-workers 2 \
  --edge-threads 1 --application-threads 1 \
  --in-flight 4 --pace 1 \
  --warmup-seconds 2 --measurement-seconds 10 --samples 5 \
  --queue-timeout 100ms --output .benchmarks/link/orbit.json
```

`h2-concurrent` defaults to TCP/H2c; `uds-h2-concurrent` defaults to UDS/H2.
Every executable also accepts `--transport orbit|h2c|uds-h2`, so one binary can
compare all three. This feature set does not select Edge. Selecting additional
features can boot additional services: reports record whether Edge and metrics
were compiled in. Keep the feature set identical between comparisons.

### Load and acceptance

Every run starts at **one outstanding request per client worker**. Each accepted
phase adds `--pace` until `--in-flight`, the **per-worker ceiling**, or the first
rejected phase. With two workers and `--in-flight 4`, total offered concurrency
rises through 2, 4, 6 and 8. This replaces the old fixed-total-concurrency harness.
A slot sends its next request after its previous request finishes; no arrival
rate or receiver occupancy is guaranteed.

[config/link/client.yaml](config/link/client.yaml) declares a whole-response
`request_timeout: 100ms` and `accept: 99.99`. A success requires a 2xx status and
the exact complete body within that deadline. Preflight must pass for every
client worker. Each phase has its own untimed warmup and measured windows;
acceptance uses outcomes completed within those windows. Requests crossing a
window boundary count where they complete. Drain completes before the next
phase. No latency percentiles are measured.

Every report retains success, 503, 502, other statuses, transport errors, invalid
bodies and deadline failures, including a rejected phase. `--keep-errors` lets a
rejected measurement exit successfully; it does not continue increasing load.

`--edge-threads` names the client process's main Tokio thread budget;
`--application-threads` names the application's. Dedicated executors are selected
separately in the client and Runner fixtures. These are per-process settings,
not CPU affinity or a total host CPU allocation. Each family has an additional
supervisor master, excluded from its worker CPU total.

### Capacity controls

[config/link](config/link) is the default fixture directory; `--config` selects
another. Each run writes its resolved YAML and launch data beneath
`.benchmarks/link/run-…/`, and embeds the YAML in its JSON report.

- Orbit: `--max-in-flight 128` bounds application admission through body/session
  completion. Omit it for the unbounded admission control. SHM lane/inbox geometry
  remains independently bounded; unbounded admission does not mean infinite slots.
- H2: the common upstream fixture sets `max_live: 1`, `max_idle: 1` and
  `max_streams: 128`. `--max-live` and `--max-streams` override the same pool
  settings for TCP and Unix origins. There is no H2 `min_idle`.
- `--queue-timeout 100ms` declares the production admission/acquire waiting budget.
  For Link this includes receiver and creator-slot waiting under one budget.
  It is separate from the whole-response client deadline. To distinguish budget
  expiry from a client deadline, select an appropriately larger client deadline
  in the fixture before the comparison.
- `--workload echo --body-bytes N` validates a complete echoed request body.
  `--buffer-size` selects Link request buffering policy; keep buffered/streamed
  and plain-body measurements separate.

For an admission comparison, use one binary, identical geometry and the same
ramp with omitted `--max-in-flight` versus `--max-in-flight 128`. A bound equal
to offered concurrency measures admission overhead; a smaller bound also measures
waiting and pacing. Preserve rejected phases instead of comparing successful
responses to a stream of immediate 503s.

Worker counts must fit `CLAVIRON_FLEET_CAPACITY` independently for each family,
including its master. Orbit's selected fleet geometry must fit both families
combined, including both masters. For 40 + 40 workers, compile with
`CLAVIRON_FLEET_CAPACITY=128` and `ORBIT_LINK_FLEET_CAPACITY=128`, and select
compatible geometry. These are **build-time** ceilings, not runtime flags.

### Reports and diagnostics

Schema 3 records all worker/master identities, requested concurrency, resolved
fixtures, client policy, raw per-process elapsed windows, outcomes and user/system
CPU. Client and serving application CPU are reported per success and per attempt;
masters are reported separately. Client CPU includes load generation and response
validation. Windows are requested concurrently but start/end independently, so
application CPU per client success is an overlapping-window estimate.

`--diagnostics` separately records client request-future polls, pending polls,
waker calls, completion time and returned transport errors. Its clocks, wrapped
wakers and counters perturb execution. Never compare diagnostic CPU/throughput
with clean runs. Waker calls are not IPC doorbells, kernel wakes, application polls
or admission waits. The completion high-water mark includes warmup and prior
phases. macOS context-switch counters are marked unavailable.

Keep repository revisions, dirty source hashes, binary hash, OS/architecture and
run order alongside raw JSON. Old two-process fixed-load reports are historical
controls, not identical measurement boundaries. `results/` retains such records;
new local output defaults to the ignored `.benchmarks/` area.

The orchestrator stops client generation first, asks each production master to
drain its workers, then clears only its own isolated fleets after both families
exit and an exclusive fleet lock succeeds. Retained SHM under a generated name
causes startup to fail rather than being adopted. Ctrl-C follows the same owned
shutdown path; no private kill deadline is introduced.
