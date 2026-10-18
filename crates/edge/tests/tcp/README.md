# HTTP transport resilience tests

Run from the Claviron repository root:

```sh
just test-http
just test-http-resilience
```

`test-http` runs ordinary deterministic regressions for the shared HTTP
configuration/authority layer, vhost admission, the OS accept classifier and
the Hyper drivers. It checks both TLS-disabled and TLS/HTTP-01-enabled builds.
It does not build a downstream application or enable unrelated PHP/JavaScript
features. The existing `smoke-transports` remains the wider composition matrix.

`test-http-resilience` is opt-in. It first runs bounded, synthetic accept-fault
and listener lifecycle tests, then the ignored native TLS churn probe with its
small diagnostic report. Direct invocation of only the churn probe is:

```sh
SQLX_OFFLINE=true cargo test --workspace --all-features \
  --lib resilience_tls_churn -- --ignored --nocapture
```

## Fixtures and boundaries

Tests live below `unit/` and are included by their owning production modules
with `#[cfg(test)]`/`#[path]`. This preserves private implementation access;
no production API was widened for the harness. `support/` contains shared
framing inputs, native TLS fixture construction and accept fault injection.

The TLS fixture exercises `HttpsServer::handle_connection`, including the real
SNI acceptor, ALPN, production H1/H2 builders, host admission and connection
teardown. It uses generated certificates, loopback port zero, an explicit
socket address and SNI, in-memory host/app configuration and an eight-entry
local TLS session cache. There is no machine DNS/trust-store dependency,
`.env`, application directory, database, fleet startup or Orbit SHM. Local
session-cache injection is not coverage of fleet-session storage/provider boot.

The cleartext tests exercise the production H1 builder and host/redirect/
HTTP-01 path. Raw H1 bytes preserve malformed headers that a friendly HTTP
client would otherwise repair or refuse to send. H2 scenarios use observable,
owned client drivers; H2 stream rejection must not break healthy peer streams.
The GOAWAY scenario uses a test-only `h2` client to observe a protocol-level
`NO_ERROR` GOAWAY while an admitted upload remains open, then verifies that
new streams are rejected and the upload still completes. Hyper dispatch-channel
readiness is not used as a substitute for observing GOAWAY. The forced-drain
scenario drives `Gate::wait_all_done` like the production listener owner and
asserts its typed forced outcome before checking released work and capacity.
The slow-reader scenario advertises a 1 KiB H2 stream window and withholds
receive-capacity release after the first chunk. A second 1 KiB chunk cannot
arrive, but a healthy peer stream completes. Releasing capacity during
indefinite grace delivers the remaining chunk and allows a joined teardown.
This measures protocol flow-control behavior, not kernel socket-buffer exhaustion.

Accept-fault tests wrap an actual loopback listener with a deterministic error
queue. Resource-pressure timing uses paused time and explicit poll counts;
tests do not lower process descriptor limits, exhaust kernel resources or
require privileged operations. The classifier has native-platform errno tests.

## Failure criteria

- Malformed headers/authorities must not enter an application or native session
  handler. Appropriate native rejection must remain stream/connection scoped.
- Finite header deadlines must terminate incomplete heads; explicit null must
  still permit controlled completion. Test deadlines do not add a production
  body timeout or change an operator's indefinite graceful-drain policy.
- Request and response buffers are bounded by each fixture. Controlled slow
  work must not prevent healthy control traffic or cancellation from progressing.
- Connection tasks, submitted sessions and client drivers must be owned and
  joined. Gate permits must return; stopped listeners must be rebindable.
- Resource retries must honor configured bounds without per-retry tasks,
  retained connections or unbounded warning history. Fatal accept errors must
  stop/drain the local runnable generation without cancelling the parent.

Exact per-scenario expectations are assertions beside the input in `unit/`.
Ordinary tests cover correctness rather than throughput. The opt-in probe adds
repetition and post-warmup diagnostics, not a stronger production-readiness claim.

## Bounded churn probe

`unit/tls/churn.rs` performs eight healthy warmup requests, then four batches
of 64 rejection attempts. Each batch mixes distinct unknown SNI against a
configured fallback, invalid TLS handshakes, malformed H1 headers and valid
but misdirected Host values. Unknown-name handshakes use normal certificate
verification; no test verifier silently accepts the fallback certificate.

There is at most one live connection. Each attempt joins its server connection
and checks the released admission slot before continuing. Both a healthy H1
and H2 control request must succeed after every batch. The H2 control's client
driver is deliberately disconnected and joined; graceful GOAWAY belongs to
the deterministic shutdown scenarios, not this probe. An enclosing 30-second
deadline bounds the entire run. H1 responses are capped at 8 KiB, H2 control
responses at 128 bytes, and the fixture accepts at most 1 KiB request data.

The report has a fixed number of lines and retains only five optional RSS
samples and four healthy-control timings. RSS is measured for the **test
process**, including client, server and allocator state, using the existing
`sys` primitive. Unsupported/unavailable readings remain `None`.
Control timings include both H1/H2 connection establishment, requests and
cleanup; they are not request-only latency. The report records OS, architecture,
debug-assertion mode and fixed workload limits; record the tested commit and
Cargo profile when comparing runs.

RSS has no pass/fail threshold: warmup and allocator retention are not proof of
a leak. This is not a sustained-load benchmark or an exhaustive memory-safety
audit. CPU/FD sampling, allocator instrumentation, real FD exhaustion, H3 wire
behavior and long-duration production soak are not measured here. No benchmark
client crate has been moved or coupled into core; extraction should wait for a
second real consumer of these test helpers.
