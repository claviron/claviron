# Where a response's serving CPU goes

Plain HTTP/1.1, 1,024 connections × pipeline depth 16 (16,384 outstanding
requests), 13-byte body, one 30-second phase a server, profiled with kernel
time included. November 2026, Linux 5.15, 40 logical CPUs shared with the
client. Claviron `9294517` as 40 `current_thread` workers; native Hyper and a
bare `mio` host from benchmark revision `ba34730`.

| Serving CPU per successful response | Native Hyper | Claviron Other | Bare `mio` |
| --- | ---: | ---: | ---: |
| Total | 14.58 µs | 16.71 µs | 8.07 µs |
| user / system | 4.84 / 9.74 | 6.37 / 10.34 | 0.99 / 7.08 |
| Kernel, socket write path | 8.44 | 8.96 | |
| ↳ packet filter and connection tracking | 2.51 | 2.68 | |
| HTTP/1.1: parse, build, encode, frame | 1.76 | 1.97 | |
| Allocation and destruction | 0.71 | 1.06 | |
| Runtime: scheduler, wakers, timers, I/O driver | 0.12 | 0.12 | |
| Claviron pipeline, host selection included | | 0.47 | |

A µs figure is a share of the CPU samples multiplied by that phase's CPU per
response. Rows overlap and do not add up to the total. The bare host is a
hand-written HTTP/1.1 loop with no Hyper and no Tokio; it was measured without
a profiler, so only its first two rows exist.

## Where to look

| Row | What it is | What moves it |
| --- | --- | --- |
| Socket write path | The `writev` of a response. On loopback it also carries the peer's receive path. | Responses per write. HTTP/2 shares writes between streams: 2.9 µs of system time a response in the [HTTP/2 report](http2-tls-1024.md), against 10.3 µs here. For pipelining clients, `h1.pipeline_flush`. Code: `crates/edge/src/tcp/io.rs`, `crates/edge/src/tcp/protocol.rs`. |
| Packet filter | The machine's filter rules and connection tracking, applied to loopback too. | The host, not the server. It is in every absolute figure of these reports and equal for both servers. |
| HTTP/1.1 | Hyper's `proto::h1`: request head parsing, request and header-map construction, response head encoding, body framing. | The dependency. |
| Allocation and destruction | Allocator entry points and drop glue. For Other it includes the response body's box. | That box is by design: a pipeline stage answers `Handled` or `Unhandled`, so every stage speaks one erased body type, `RouteBody` in `crates/http/src/ctx.rs`. One box a response is the floor; a second one is a regression. |
| Runtime | Tokio's own operations, not the work it runs. | Nothing over HTTP/1.1. Over HTTP/2 the listener's `execution` decides a response's cost; see the [HTTP/2 report](http2-tls-1024.md). |
| Claviron pipeline | Host selection and the compiled host pipeline around the handler. | `crates/http/src/authority.rs`, `crates/edge/src/pipeline/host/`. |

## Claviron's addition, by where the samples land

Other minus Hyper in these two profiled phases, each sample counted once at
its leaf:

| Leaf family | µs a response |
| --- | ---: |
| Kernel, socket write | +0.50 |
| Hyper HTTP/1.1 and HTTP types, the same code under another body type | +0.51 |
| Allocation and destruction | +0.38 |
| Copies, libc and other user leaves | +0.36 |
| Functions named Claviron | +0.24 |
| Everything else | +0.13 |
| **Total** | **+2.13** |

Without a profiler attached the difference was 1.81 µs in the
[series](http1-plain-1024.md), 1.24 of it user time and 0.57 system. A clean
phase of this build measured 16.37 µs, against 16.31 µs at Claviron `f838792`.

## Limits

- Native Hyper's stacks stop short of their outer frames: two of its frames
  are larger than the 16 KiB of stack the profiler captures. Its runtime row
  is what could be resolved. Other's stacks reach thread start in 92% of
  samples, so its runtime row is complete.
- Other's recording lost 1.2% of its samples.
- One phase a column. Differences of a few tenths of a µs between the two
  profiled columns are not findings.
- The bare host parses no real request. Its distance to Hyper bounds what a
  leaner server could save; it does not estimate it.
