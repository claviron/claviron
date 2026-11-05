# HTTP/1.1 over TLS with 1,024 connections

These November 2026 Linux loopback measurements compare Claviron Other with
native Hyper over TLS, at Claviron revision `f838792` and benchmark revision
`ba34730`. The machine had two Intel Xeon E5-2680 v2 processors, 40 logical
CPUs and Linux 5.15. Client and server shared it. Native Hyper ran as one
process on its default runtime of 40 threads; Other ran as 40 `current_thread`
worker processes.

[`break`](https://crates.io/crates/break) 0.2 (revision `5dd25ec`) drove 1,024
fixed connections with eight declared worker threads and recorded serving CPU
through an Observer extension. Each target served the same 13-byte
`Hello, World!` body on `GET /plain`. Pipeline depth started at 8 and rose by 2
per connection every 30 seconds, to at most 40. The SLA required at least
99.99% correct and complete responses within 100 ms, and each run ended at its
first rejected phase. Listener backlog was 2,048 and the soft file descriptor
limit 65,536.

Every connection negotiated TLS 1.3 with `TLS13_AES_256_GCM_SHA384` and
X25519. The connections are opened once and kept, so no handshake falls inside
a measured phase; what establishing a connection costs is a
[report of its own](tls-connections-1024.md). Each fresh Hyper process
received one HTTPS request before it was measured, because the TLS library
spends tens of milliseconds preparing its random generator on first use.
Claviron does that at boot and received none.

| Target | Peak accepted responses/s | Outstanding at peak | Last accepted / first rejected target outstanding | Responses/s at 16,384 | Serving CPU µs/success at 16,384 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Native Hyper | 1,474,893 | 16,384 | 16,384 / 18,432 | 1,474,893 | 18.247 |
| Claviron Other | 1,421,846 | 26,624 | 28,672 / 30,720 | 1,372,960 | 19.724 |

At 16,384 target outstanding requests, the highest level both passed, Other
used 1.477 µs more serving CPU per successful response (8.1%) and delivered
6.9% fewer responses per second. Mean response times were 11.106 ms and
11.929 ms. Serving CPU was 67.3% of the 40 CPUs for Hyper and 67.7% for Other;
the load generator took 16.8% and 15.4% of the CPU that client and server used
together.

Set beside the [plain series](http1-plain-1024.md) of the same day, TLS adds
about 3.7 µs a response to Hyper and 3.4 µs to Other. Claviron's own addition
is not larger over TLS than without it: 1.5 µs here, 1.8 µs there.

As in the plain series, the servers differ more in depth than in rate. Hyper
was rejected at 18,432 outstanding requests; Other passed up to 28,672 and was
rejected at 30,720. Hyper runs one process with a work-stealing runtime and
Other forty processes that each own a listener and an executor, so the
comparison does not isolate Claviron's own work from that topology, and every
boundary is a joint one on a shared machine.
