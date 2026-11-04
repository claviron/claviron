# HTTP/1.1 plain-text benchmark with 128 connections

This November 2026 Linux loopback series compared Caddy 2.11.8 with Claviron
Axum and Warp at Claviron revision `5b96b5b`. The machine had two Intel Xeon
E5-2680 v2 processors and 40 logical CPUs. Client and server shared the machine.

The [`break` load generator](https://crates.io/crates/break) used a custom Observer
extension to record phase results and separate serving, supervisor, and client
CPU time. Each target served the same 13-byte `Hello, World!` body on `GET /plain`
over 128 fixed connections. Every 60-second phase increased pipeline depth by
one per connection. The acceptance rule required at least 99.99% correct and
complete responses within 100 ms. Each run ended at its first rejected phase.
The effective listen backlog was 2,048 and the file descriptor limit was 65,536.

| Target | Peak accepted responses/s | Last accepted target outstanding | First rejected target outstanding |
| --- | ---: | ---: | ---: |
| Caddy | 287,280 | 1,280 | 1,408 |
| Claviron Axum standalone, shared executor | 928,146 | 9,984 | 10,112 |
| Claviron Warp standalone, shared executor | 992,841 | 10,240 | 10,368 |
| Claviron Warp, 40 `current_thread` workers | 1,045,500 | 13,824 | 13,952 |

Caddy and Axum started at pipeline depth 1. Both Warp runs started at depth 77
(9,856 target outstanding requests), so their peaks cover the measured portion
of the ramp. The four runs have no single measured phase in common.

At 9,984 target outstanding requests, the serving CPU cost per successful
response was 24.41 µs for standalone Axum, 22.09 µs for standalone Warp, and
21.52 µs for 40-worker Warp. These figures sum user and system CPU across the
serving processes and exclude the supervisor and load generator.
