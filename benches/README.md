# Claviron benchmarks

This directory is a planning skeleton for Claviron operation microbenchmarks.
The Rust files describe intended cases; no measurements are registered yet.
No results or performance claims are recorded here.

## Measurement boundaries

| File | Measure | Exclude from the timed operation |
| --- | --- | --- |
| `http.rs` | Authority, header, cookie, and path operations on prepared inputs. | Socket I/O, TLS, listener startup, and application execution. |
| `routing.rs` | Prepared vhost/route selection and native intercept versus fallback dispatch. | Configuration parsing, provider boot, route construction, and real upstream or guest-runtime execution. |
| `orbit.rs` | Claviron envelope encoding and decoding for existing event or application-transport formats. | SHM attachment, fleet boot, cross-process scheduling, stream throughput, and peer execution. |

Keep fixture preparation outside the timed region unless setup is the operation
being measured. Label lookup-only, encode-only, decode-only, and dispatch cases
separately. State whether allocations and output consumption are included.

For routing comparisons, use equivalent prepared inputs and response work. A
local fallback stand-in measures dispatch overhead, not PHP, Python, or remote
application performance.

Orbit primitive benchmarks belong to Orbitive. Do not duplicate its stream or
Unix-socket comparisons here, or report a codec result as SHM transport speed.

## End-to-end measurements

HTTP/1, HTTP/2, HTTP/3, TLS, worker scaling, and real TCP/UDS/Orbit application
paths belong in a separate executable harness with an explicit load generator.
Use the shared workloads in [claviron::bench](../docs/bench/README.md):

- `/bench/plain`: a small static response.
- `/bench/json`: per-request JSON serialization.
- `/bench/echo`: streaming request/response bodies.
- `/_bench/rust`: buffered native baseline with caller-selected payload/delay.

Use the same handler when comparing ingress paths. Report connection reuse,
protocol, TLS resumption, body handling, and worker topology; changing these
changes the experiment. The `bench` Cargo feature selects workloads, not a load
generator or a benchmark run.

Harnesses must use their own configuration, endpoints, and fleet namespace.
Do not borrow a live deployment's ports, credentials, or SHM resources.

## Add the first real case

Choose the benchmark harness and wire its Cargo target when implementing an
actual measurement. The current outline files are not evidence of a working
benchmark suite. Keep the measured operation small and consume its output so
the compiler cannot eliminate it. Check correctness separately from timing.

Choose and report sample, warmup, and execution settings explicitly rather than
adding hidden timing defaults. Keep measured cases in release/bench builds and
outside the ordinary unit-test path.

Record enough information to reproduce a result:

- Revision and dirty-tree status, Rust version, profile, and enabled features.
- OS/kernel, CPU, memory, allocator, and relevant build flags.
- Input sizes, scenario, concurrency, process/thread counts, and setup boundary.
- Measurement settings, units, variability, and any observed errors.
- For end-to-end runs: client placement, load generator/version, connection
  policy, latency percentiles, throughput, and resource use.

Compare like-for-like runs. Keep source data and commands with any published
result, and distinguish transport cost from application work.
