# Native transport benchmark

One Rust runner serves the same native Claviron benchmark handlers through
Orbit Link, persistent UDS/h2c and persistent TCP/h2c. No interpreter is involved.
The benchmark service is always included. This is an independent Cargo workspace,
not a `cargo run --example` target. Both application and Edge use local Claviron
path dependencies; Orbitive resolution belongs to Claviron's workspace, not this app.

`--workers N` selects supervised application processes; zero keeps one standalone
process. The optional `supervisor` section selects supervision policies. Workers
inherit the resolved launch, including custom configuration and door selection.
For multiple workers, omit `uds` from the runner file and select Orbit or TCP;
the shared Unix listener requires standalone mode.

From the repository root:

```sh
examples/scripts/bench.sh start --release
examples/scripts/bench.sh status
examples/scripts/bench.sh stop
```

Both configurations are validated before either process is launched. The Edge
listens only on `127.0.0.1:8080`; its benchmark config lives in
`../edge-server/config-bench` and does not replace the other demos.
Do not run it beside another instance of the same Edge/application fleets.

| Host header | Edge → application |
| --- | --- |
| `orbit.bench.test` | Orbit Link |
| `uds.bench.test` | Pooled Unix socket, HTTP/2 cleartext |
| `tcp.bench.test` | Pooled loopback TCP, HTTP/2 cleartext |

Run the same `wrk` workload against each transport:

```sh
wrk -H 'Host: orbit.bench.test' http://127.0.0.1:8080/bench/plain
wrk -H 'Host: uds.bench.test' http://127.0.0.1:8080/bench/plain
wrk -H 'Host: tcp.bench.test' http://127.0.0.1:8080/bench/plain
```

Use the same load-generator settings and path for each host. `/bench/plain`,
`/bench/json`, `/bench/echo` and `/_bench/` use the shared native handlers.
Keep process counts, request/response sizes, concurrency and build profiles
equal; record failures alongside throughput, latency and CPU. Socket connections
are retained, not created anew per request. These are different transports, not
an instruction-level inbox microbenchmark. Record the resolved dependency source
and revision before comparing changes; a registry Orbitive build does not contain
unpublished local inbox changes.

Results and measurement limits are recorded in the
[Orbit Link benchmark journal](https://github.com/iadev09/orbitive/blob/main/link/BENCHMARKS.md).

The application uses portable Unix/Claviron APIs for macOS, Linux and FreeBSD.
Build and validate it natively on each host; an execution on one OS is not a
cross-platform validation claim.
