# TLS connections: what establishing one costs

The request benchmarks keep their connections open. This one measures the
connection itself: each of a fixed number of connection owners connects,
completes a TLS handshake, sends one request, receives the reply and closes,
then starts again. The figure is serving CPU per established connection.

All measurements are from November 2026, on Linux 5.15 with two Intel Xeon
E5-2680 v2 processors and 40 logical CPUs shared by client and server. Native
Hyper ran as one process on its default runtime of 40 threads; Claviron Other
ran as 40 `current_thread` worker processes that share one fleet-wide TLS
session table. [`break`](https://crates.io/crates/break) 0.2 (revision
`5dd25ec`) ran its connection test with eight declared worker threads, a
100 ms deadline for a setup and for a request, and 99.99% acceptance. Every
connection negotiated TLS 1.3 with `TLS13_AES_256_GCM_SHA384` and X25519, and
the client verified the certificate. With resumption enabled each connection
owner behaves as a returning visitor; `--no-resume` makes every handshake a
full one.

## Where it started: Claviron `f838792`

Concurrent connections rose by 256 every 30 seconds. All four runs accepted
1,024; Hyper accepted 1,280 in both modes, Other 1,280 with resumption and
1,024 without.

At 1,024 concurrent connections:

| Target and mode | Established connections/s | Resumed share | Serving CPU µs/connection |
| --- | ---: | ---: | ---: |
| Native Hyper, resumption enabled | 12,968 | 0.1% | 469.8 |
| Native Hyper, full handshakes | 13,536 | 0.0% | 469.3 |
| Claviron Other, resumption enabled | 18,742 | 86.5% | 577.1 |
| Claviron Other, full handshakes | 12,957 | 0.0% | 567.4 |

Other cost about 100 µs more per connection than Hyper, 21 to 23%. The
resumed shares explain the rates and nothing else: the native host keeps
rustls's default in-process cache of 256 sessions, which 1,024 returning
visitors overrun, so its "resumption enabled" run is a run of full handshakes.
Claviron's table holds 2,048 and resumed 86%.

That was the odd part. A resumed handshake skips the certificate signature,
which is the dearest single step of a full one, and Other's connections were
no cheaper with 86% of them resumed than with none.

## What it was

Every operation on the session table took one lock around the whole table
through a lock file: nine system calls an operation, two operations for a full
handshake and three for a resumed one, with forty workers queueing on the one
lock. A profile of the workers that included kernel time, taken after the
first of the changes below had brought that to six calls, still put the table
at 11.0% of serving CPU, about 63 µs a connection. Every store and every consumed
session also erased the full 16 KiB of its slot. Resumption saved the
signature and spent the saving on the extra operation.

The table belongs to Orbitive (`orbit-rustls`, on `orbit-core`). Three changes
went into it, each measured
as its own stage at 1,024 concurrent connections, one 30-second phase a row.
The first two rows were measured at Claviron `f838792`, the last three at
`9294517`; the second and third rows are the same table code and show that
Claviron's own move between those revisions changed little.

| Session table | Claviron | Resumption enabled, µs/connection | Full handshakes, µs/connection |
| --- | --- | ---: | ---: |
| `orbit-rustls` 0.5.5 | `f838792` | 583.3 | 563.4 |
| Lock directory opened once | `f838792` | 565.3 | 558.4 |
| The same | `9294517` | 560.5 | 554.4 |
| Erases only what an entry wrote | `9294517` | 541.5 | 544.6 |
| No lock: a claim on the entry's set | `9294517` | 495.5 | 524.2 |

In the last form an operation claims the one set its key hashes to with a
compare-and-swap, never waits, and makes no system call for a lock; an
operation that finds its set in use is answered with a miss, which TLS turns
into a full handshake. The resumed share stayed at 86% through all stages, so
that answer cost nothing visible. With kernel time included the table fell
from 11.0% to 1.4% of serving CPU, about 7 µs a connection. A resumed
connection is now cheaper than a full one, by 29 µs.

These stages were measured before release. `orbit-rustls` 0.5.6 as published
also drops a process check that the last row still made on every operation;
the profile put it at about 2.9 µs a connection, and the published form has
not been measured again.

## Where it stands

Against the native Hyper figure of about 470 µs, which comes from the earlier
session and the earlier Claviron revision, Other is now about 25 µs dearer per
connection with resumption, where it was 107 µs, and about 55 µs dearer with
full handshakes, where it was 98 µs. Cryptography is 46% of a connection's
serving CPU and is the same work in both servers. The largest named item outside it is memory
copying, about 4% of serving CPU, which has not been traced to its callers.

Every row is a single phase on a machine the client shares; no spread is
known, and differences of a few µs between rows are not findings.
