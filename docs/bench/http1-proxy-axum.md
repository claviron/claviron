# Three frontend paths to one Axum application

Caddy `reverse_proxy`, Claviron Edge over an HTTP/1.1 upstream, and
Claviron Edge over Orbit Link each reached the same Axum application.
Each run started a fresh standalone application using the same binary,
built with Claviron `0.2.0-alpha.26` from the Axum Link example at `06e4e04`.
It exposed Orbit Link and loopback HTTP/1.1 in all three runs. The
identity preflight verified the application PID and `GET /bench/plain`
returned the exact three-byte body `ok\n`.

The client, frontend and application shared a Linux x86-64 machine with two
Intel Xeon E5-2650 v2 processors and 32 logical CPUs. Each frontend and
application ran in one process. Caddy reused the
[2.11.9 source build from the direct-server series](http1-plain-caddy.md).
Both Edge paths used the same native release `0.2.0-alpha.26` executable with
mimalloc. Both HTTP upstreams allowed 128 live connections and retained up
to 128 idle. Orbit Link allowed 128 in-flight exchanges and 64 waiting.
TCP listener backlog was 2,048 for both the frontend and application.

[`break` 0.2.3](https://crates.io/crates/break/0.2.3) used eight client
threads, 128 fixed HTTP/1.1 connections and pipeline depth six for one
60-second phase per path. The SLA required at least 99.99% complete, correct
responses within 100 ms. CPU sums user and system time per successful
response, measured by PID for the frontend and application in the same
phase. Coverage measures mean outstanding requests against the target of 768.

| Path | Frontend CPU µs/response | Axum CPU µs/response | Combined CPU µs/response | Responses/s | Coverage |
| --- | ---: | ---: | ---: | ---: | ---: |
| Caddy over HTTP/1.1 | 255.723 | 40.509 | 296.232 | 60,057.62 | 99.963% |
| Claviron Edge over Orbit Link | 76.747 | 31.550 | 108.296 | 154,190.09 | 99.967% |
| Claviron Edge over HTTP/1.1 | 90.123 | 38.048 | 128.170 | 118,735.60 | 99.973% |

All three passed with zero errors. The table records one phase at one load
for each complete path. It leaves the SLA boundaries unmeasured.
