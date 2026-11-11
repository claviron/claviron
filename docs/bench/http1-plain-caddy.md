# Caddy and Claviron serving plain HTTP/1.1

Caddy 2.11.9 and two Claviron Axum configurations served the same 13-byte
`Hello, World!` body on `GET /plain`. The client and servers shared a Linux
x86-64 machine with two Intel Xeon E5-2650 v2 processors and 32 logical CPUs.
Each run used a loopback listener with a verified backlog of 2,048 and a
65,536 file descriptor limit.

Caddy was built from upstream `0d98caea58f794f88e709b1bef2880996a471bca`
with Go 1.27.3, `CGO_ENABLED=0`, `GOAMD64=v2` and `GOTOOLCHAIN=local`. The
build used `-p 2 -trimpath`, tags `nobadger,nomysql,nopgx`, and linker flags
`-s -w -X github.com/caddyserver/caddy/v2.CustomVersion=v2.11.9`. It ran
with `GOMAXPROCS=32` and `GOGC=100`. Its JSON configuration enabled
HTTP/1.1 and a static response handler. Automatic HTTPS, admin and access
logging were disabled. The launcher applied and verified the backlog.
[Caddy v2.11.9 source](https://github.com/caddyserver/caddy/releases/tag/v2.11.9)

The Axum harness at `6a56a13` used Claviron `0.2.0-alpha.26` from crates.io
and a native release build with fat LTO and mimalloc. Standalone used the
inherited Tokio executor with 32 worker threads. The other configuration
used 32 serving processes, each with a dedicated current-thread listener.
Both served plain HTTP/1.1 with access logs off.

[`break` 0.2.3](https://crates.io/crates/break/0.2.3) used eight client
threads and 60-second phases. Pipeline depth started at one and rose by one
per phase. The SLA required at least 99.99% complete, correct responses
within 100 ms. Serving CPU sums user and system time of the serving processes
per successful response, excluding client and supervisor time. Coverage is
mean outstanding requests as a percentage of the phase target.

## Matched phases

At 128 connections, depth six was the highest phase all three passed. At
1,024 connections, depth one was the only common phase.

| Connections | Depth | Server | Serving CPU µs/response | Responses/s | Coverage | Errors | SLA |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | --- |
| 128 | 6 | Caddy | 74.757 | 268,816.77 | 99.999% | 717 | Pass |
| 128 | 6 | Claviron Axum standalone | 24.101 | 738,552.38 | 100.000% | 0 | Pass |
| 128 | 6 | Claviron Axum, 32 dedicated workers | 24.092 | 879,752.52 | 100.000% | 0 | Pass |
| 1,024 | 1 | Caddy | 88.419 | 220,144.39 | 99.916% | 3,953 | Reject |
| 1,024 | 1 | Claviron Axum standalone | 32.171 | 464,837.79 | 99.892% | 0 | Pass |
| 1,024 | 1 | Claviron Axum, 32 dedicated workers | 32.328 | 468,011.31 | 99.883% | 0 | Pass |

At 128 connections Caddy first missed the SLA at depth seven. Both Claviron
configurations passed depth seven. At 1,024 connections Caddy missed the
SLA at depth one with 3,953 timeouts. Claviron stopped at Caddy's last
attempted depth at each connection count, so the series leaves its own SLA
boundary unmeasured. The [earlier 128-connection report](http1-plain-128.md)
used different hardware, builds and load generation.
