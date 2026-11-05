# HTTP/1.1 plain-text benchmark with 1,024 connections

These November 2026 Linux loopback measurements compare Claviron with native
Hyper and Axum. The client and server shared a machine with
40 logical CPUs. The native servers used their default multithreaded Tokio
runtimes. Both Claviron targets used 40 `current_thread` worker processes.

The [`break` load generator](https://crates.io/crates/break) used a custom Observer
extension to measure phase results and serving CPU. Each run used 1,024 fixed
connections and the same 13-byte `Hello, World!` body on `GET /plain`. Pipeline
depth started at one and increased by one per connection every 60 seconds. The
SLA required at least 99.99% correct and complete responses within 100 ms. Each
run ended at its first rejected phase. Listener backlog was 2,048 per socket
and the soft file descriptor limit was 65,536.

The sections below are three separate series, newest first. The newest one
used a different load generator and is not a continuation of the two before
it; its own section says why.

## Series with Claviron `f838792` and `break` 0.2

Native Hyper and Claviron Other were built from benchmark revision `ba34730`,
with Continuo `0b075ba`, and measured on Linux 5.15. Native Hyper ran as one
process on its default runtime of 40 threads; Other ran as 40 `current_thread`
worker processes.

The load generator changed, which is why these figures stand apart. `break`
0.2 (revision `5dd25ec`) drives HTTP/1.1 from a readiness loop of its own and
writes the requests admitted together in one write. It ran with eight declared
worker threads, where the earlier series ran one client thread per CPU beside
the server. Depth started at 8 and rose by 2 per connection every 30 seconds,
to at most 40. At a depth above one the bytes on the wire are shaped
differently from the earlier series, so no figure here is to be set against a
figure there.

| Target | Peak accepted responses/s | Outstanding at peak | Last accepted / first rejected target outstanding | Responses/s at 16,384 | Serving CPU µs/success at 16,384 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Native Hyper | 1,811,355 | 16,384 | 16,384 / 18,432 | 1,811,355 | 14.500 |
| Claviron Other | 1,694,961 | 32,768 | 38,912 / 40,960 | 1,678,101 | 16.310 |

At 16,384 target outstanding requests, the highest level both passed, Other
used 1.810 µs more serving CPU per successful response (12.5%) and delivered
7.4% fewer responses per second. Mean response times were 9.044 ms and
9.761 ms. Serving CPU was 65.7% of the 40 CPUs for Hyper and 68.4% for Other,
and the load generator took 14.0% and 12.1% of the CPU that client and server
used together.

The two servers differ more in how far they hold the SLA than in their rate.
Hyper was rejected at 18,432 outstanding requests. Other passed every level up
to 38,912, with its rate between 1.64 and 1.70 million responses per second
from 14,336 upwards, and was rejected at 40,960, the last level of the plan.
A rate that stays level while the depth rises is the machine being full, not a
server limit being approached: client and server share the 40 CPUs, and every
boundary here belongs to both.

Hyper runs one process with a work-stealing runtime and Other runs forty
processes that each own a listener and an executor. The comparison is between
complete serving paths including that difference; it does not isolate
Claviron's own work from the execution topology.

### Where a response's serving CPU goes

The same level, 16,384 outstanding requests, was profiled with kernel time
included, one 30-second phase a server, at Claviron `9294517`. A bare `mio`
host, a hand-written HTTP/1.1 loop with no Hyper and no Tokio, ran beside them
as a floor. The µs figures are a share of the samples multiplied by that
phase's CPU per response; the rows overlap and do not add up.

| | Native Hyper | Claviron Other | Bare `mio` |
| --- | ---: | ---: | ---: |
| Serving CPU per response | 14.58 µs | 16.71 µs | 8.07 µs |
| user / system | 4.84 / 9.74 | 6.37 / 10.34 | 0.99 / 7.08 |
| Kernel, socket write path | 8.44 µs | 8.96 µs | not profiled |
| of which packet filter and connection tracking | 2.51 µs | 2.68 µs | not profiled |
| HTTP/1.1: parse, build, encode, frame | 1.76 µs | 1.97 µs | not profiled |
| Allocation and destruction | 0.71 µs | 1.06 µs | not profiled |
| Runtime: scheduler, wakers, timers, I/O driver | 0.12 µs | 0.12 µs | not profiled |
| Claviron pipeline, host selection included | none | 0.47 µs | none |

Two thirds of a response is kernel time, and nearly all of that is writing to
the socket. On loopback the writer's system call also carries the peer's
receive path and the machine's packet filter, which by itself is 16 to 17% of
serving CPU here; that share is in every absolute figure of these reports and
is the same for both servers. The bare host says the same from the other side:
with almost no code of its own it still spends 7.1 µs a response in the
kernel.

What is left for user space is small. Hyper's HTTP/1.1 work is under 2 µs, the
runtime's own operations about a tenth of one, and Claviron's pipeline about
half of one. Claviron's addition over native Hyper, about 2 µs in these
profiled phases, is spread over the write path, the shared HTTP code,
allocation and copies rather than sitting in one place. A clean phase of the
rebuilt Other measured 16.37 µs, against 16.31 µs in the series above.

The bare host parses no real request, so its distance to Hyper is a ceiling on
what a leaner server could save, not an estimate of it. Native Hyper's stacks
were cut short of their outer frames and Other's recording lost 1.2% of its
samples, so small differences between the two columns are not findings.

## Follow-up with Claviron `20afbeb`

Native Hyper and Claviron Other were rebuilt from benchmark revision `ea1dc5b`
and rerun with the same Observer binary and load plan. The Claviron build used
Continuo `0b075ba`. Other used 40 `current_thread` workers with one dedicated HTTP
executor per worker. Both targets enabled keep-alive and disabled half-close
and pipeline flush.

| Target | Peak accepted responses/s | Outstanding at peak | Last accepted / first rejected target outstanding | Responses/s at 12,288 | Serving CPU µs/success at 12,288 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Native Hyper | 1,217,565 | 15,360 | 15,360 / 16,384 | 1,165,968 | 14.601 |
| Claviron Other | 1,079,128 | 11,264 | 12,288 / 13,312 | 1,075,866 | 18.578 |

At 12,288 target outstanding requests, the highest phase both passed, the
observed mean outstanding was 12,042.60 for Hyper and 11,989.79 for Other. Mean
response times were 10.344 ms and 11.159 ms, respectively. Other used 3.978 µs
more serving CPU per successful response and delivered 7.7% fewer successful
responses per second. Its CPU premium over Hyper fell from **33.1% to 27.2%**
at that same target across the two series. The separate runs compare full
serving paths across revisions, so the change cannot be assigned to an
individual optimization.

## Earlier four-target series with Claviron `14ce868`

| Target | Peak accepted responses/s | Last accepted target outstanding | First rejected target outstanding | Responses/s at 12,288 | Serving CPU µs/success at 12,288 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Native Hyper | 1,189,859 | 14,336 | 15,360 | 1,168,528 | 14.48 |
| Native Axum | 1,022,578 | 15,360 | 16,384 | 994,851 | 19.78 |
| Claviron Axum | 945,980 | 13,312 | 14,336 | 935,983 | 23.23 |
| Claviron Other | 1,065,794 | 12,288 | 13,312 | 1,059,066 | 19.27 |

All four passed at 12,288 target outstanding requests, with roughly 12,000 to
12,075 observed outstanding requests on average. CPU cost sums serving user
and system time and excludes the supervisor and load generator. At this common
target, Claviron Other used 33.1% more serving CPU per successful response than
native Hyper, and Claviron Axum used 17.4% more than native Axum. The runs compare
complete serving paths, including their different executor topologies.
