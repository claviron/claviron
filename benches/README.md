# HTTP benchmarks

Measure the cost of placing Claviron between a decoded request and an
application. Native controls use Hyper or Axum without the Edge host pipeline.

## Layout

| Directory | Measurement |
| --- | --- |
| `dispatch/native/` | Native Hyper service and Axum router, with a JSON response. |
| `dispatch/edge/` | Registered Edge owners; paired plain controls and a separate JSON workload. |
| `transport/http1/native/` | Real Hyper and `axum::serve` over persistent loopback HTTP/1.1. |
| `transport/http1/edge.rs` | Hyper plus a prepared Edge host plan over the same loopback connection. |
| `transport/http1/edge/` | Native Edge HTTP/1 driver, including host selection, for Other, Axum and Routable. |
| `support/` | Shared workloads, typed config fixtures and preparation outside timing. |

Dispatch excludes HTTP decoding, host selection, sockets and TLS. The HTTP/1
measurements include client/server protocol work and socket IO, with one
connection and one in-flight request on a current-thread Tokio runtime. The
older JSON `http1-edge` case excludes Edge listener lifecycle and cleartext host
selection. The new plain Edge cases include the full native request path.

## Plain host overhead

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

The Edge targets call `HttpServer::connection_handler`, the accepted-stream path
also used by the native listener. Request timing includes protocol decoding,
authority validation, the current vhost/application lookup, connection-local
metadata reuse, host policy, native body adaptation, dispatch and response IO.
The benchmark does not duplicate those decisions.

Configuration, application publication, socket bind/accept, connection admission
and initial setup precede timing. Preflight changes the Host header on the same
connection: a known host succeeds, an unknown host returns 404, an alias succeeds,
then the known host succeeds again. Every measured response is checked. Shutdown
joins the native connection and sessions and checks that their permits were released.

Compare Other/Routable with Hyper, and Edge Axum with native Axum. These are serial
client/server round-trip times, including client work; keep them separate from
the narrower dispatch measurements and concurrent server CPU/request results.

## Run

From the repository root:

```sh
python3 scripts/bench-http.py dispatch-edge-other \
  --warmup-seconds 3 --measurement-seconds 5 --samples 100
```

Substitute a target from the table to compare another host owner. The script
builds the optimized benchmark first, using the same `edge,axum` features for all
controls; no compilation occurs during measurement. Times and sample count are
explicit arguments, and Criterion requires at least ten samples.

For the full HTTP/1 plain path, substitute a target from the preceding table:

```sh
python3 scripts/bench-http.py http1-plain-edge-other \
  --warmup-seconds 3 --measurement-seconds 5 --samples 100
```

For the secondary JSON workload, use `dispatch-hyper`, `dispatch-axum` or
`dispatch-edge-json`. Actual HTTP/1 targets are `http1-hyper`, `http1-axum` and
`http1-edge`; pass the same timing arguments. Each completed response is checked
for the expected status, content type and body.

## Results

Local records and Criterion data stay under ignored `.benchmarks/http/`. Records
include the Git commit, dirty-source digest, binary hash and measurement controls.
`--compare RUN_ID` requires matching controls; automatic history comparisons use
the latest compatible run of the same target. Source/binary changes during a run
invalidate it. Arithmetic differences do not establish statistical significance.

Keep plain, JSON and socket round-trip results separate. These serial operation
times are not server CPU/request, request p99 or concurrent server capacity.
