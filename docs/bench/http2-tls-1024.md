# HTTP/2 over TLS with 1,024 connections

These November 2026 Linux loopback measurements compare Claviron Other with
native Hyper over HTTP/2, at Claviron revision `f838792` and benchmark revision
`ba34730`. The machine had two Intel Xeon E5-2680 v2 processors, 40 logical
CPUs and Linux 5.15, shared by client and server. Both servers use the same
HTTP/2 engine, Hyper's, and both allow 200 concurrent streams a connection.

[`break`](https://crates.io/crates/break) 0.2 (revision `5dd25ec`) held 1,024
connections open and a fixed number of concurrent streams on each. The level
is concurrent streams, where the HTTP/1.1 reports pipeline requests, and the
two are not compared. Each phase lasted 30 seconds with a 100 ms deadline and
99.99% acceptance on the 13-byte `Hello, World!` body of `GET /plain`. TLS 1.3
with `TLS13_AES_256_GCM_SHA384` and X25519 throughout.

There is no ramp in this report. The load generator's HTTP/2 path costs it 11
to 18 µs of CPU a response at these levels, several times its HTTP/1.1 path, and its threads
were full long before either server was. Fixed levels compare what a response
costs each server; they do not find where a server gives way.

## What a response costs

Serving CPU per successful response, client on eight threads:

| Server and how it is run | 8 streams | user / system | 16 streams | user / system |
| --- | ---: | ---: | ---: | ---: |
| Native Hyper, one process, shared runtime | 47.9 µs | 32.1 / 15.8 | 46.4 µs | 30.7 / 15.7 |
| Claviron Other, one process, shared runtime | 46.0 µs | 31.9 / 14.1 | 42.7 µs | 29.5 / 13.2 |
| Claviron Other, 40 `current_thread` workers | 25.2 µs | 19.7 / 5.5 | 17.6 µs | 14.7 / 2.9 |

All six phases passed. The three rows are one finding: what an HTTP/2
response costs depends on how the server is run far more than on which server
it is.

- Run the same way, as one process on a shared work-stealing runtime, Claviron
  Other costs within 4 to 8% of native Hyper, on the lower side. Claviron's
  own layers are not visible over HTTP/2.
- The same Other binary with the same configuration costs 2.4 times less at 16
  streams as forty single-threaded worker processes, and its system time falls
  from 13 µs to 3 µs.

Over HTTP/1.1 the two ways of running differ by a µs or two per response
([plain](http1-plain-1024.md), [TLS](http1-tls-1024.md)). The likely reason
they differ so much here is that HTTP/2 runs every stream as a task of its
own: on a shared runtime those tasks and the connection that owns them meet
across threads, and with one executor per worker they stay on one. No profile
was taken, so that is an explanation that fits, not one that was shown.

For Other at 16 streams, 17.6 µs is about what the same server spends per
response over HTTP/1.1 with TLS.

## With the client on sixteen threads

The eight-thread client was the limit in every phase above, so one level was
repeated with sixteen: 1,024 connections with 16 streams each.

| Server and how it is run | Successful responses/s | Failed requests | Latest noticed deadline | Serving CPU µs/success | Outcome |
| --- | ---: | ---: | ---: | ---: | --- |
| Native Hyper, one process, shared runtime | 667,854 | 27,981 | 39.2 ms | 34.2 | Rejected, 99.86% |
| Claviron Other, 40 `current_thread` workers | 1,088,779 | 0 | 0.0 ms | 16.3 | Passed |

Both runs put 56 threads on 40 CPUs. Hyper failed the SLA there, with the
client itself noticing deadlines up to 39 ms late; Other carried 63% more
responses with no failure and no late deadline. The client was still 91% busy
in Other's run, so 1.09 million is a level Other accepted, not its limit.

## What this does not show

- The two ways of running Other differ in two things at once: the number of
  processes and the kind of executor. Which of the two carries the difference,
  and whether one process with an executor per thread would do as well as
  forty processes, was not measured.
- Each row is one 30-second phase. No spread is known.
- Without a profile, task stealing, wakes across threads and shared state are
  not told apart.
