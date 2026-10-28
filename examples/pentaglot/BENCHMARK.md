# Pentaglot benchmark

Measured on 2026-10-28. Linux x86-64, AlmaLinux 8.10, kernel 5.15.223;
two Intel Xeon E5-2650 v2 CPUs, 16 physical cores / 32 logical CPUs,
125 GiB RAM. Client and server share the host; no CPU affinity or system tuning.

## Configuration

| Setting | Value |
| --- | --- |
| Build | Release, opt-level 3, thin LTO, target-cpu=native |
| Allocator | Rust global MiMalloc; `--mimalloc release` policy |
| Purge policy | `MIMALLOC_PURGE_DELAY=0`, `MIMALLOC_PURGE_DECOMMITS=1` |
| Tokio | Default `multi_thread`; no thread-count override |
| Process family | One master and 16 serving workers; fresh family per language |
| Listener | HTTP/1.1 over private loopback; 16 separate SO_REUSEPORT sockets |
| Generator | wrk 4.2.0, 8 threads, 128 persistent connections |
| Timed load | GET JSON, 5 minutes per language |
| Idle observation | Same live family, +10/+30/+60/+120 seconds after load |
| Logging | warn; 16 startup OPcache completion records at info; access log off |

| Runtime | Per-worker capacity |
| --- | --- |
| Rust | Native handler; typed Serde JSON |
| Lua | One persistent VM, concurrency 1 |
| Deno | 8 persistent V8 isolates |
| PHP / Symfony | 8 warm persistent runner threads, max idle 8, elastic ceiling 64 |
| Python / Gufi-ASGI | One ASGI event loop; compact JSON |

All five applications remain loaded in every worker in every case.
Lua, Deno and PHP admission queues are 128. PHP retains its declared 60-second
idle timeout for excess instances. Symfony runs in `prod`, debug off, with an
authoritative Composer classmap and warmed container/router caches. Every
worker finishes OPcache warmup before measurement; timestamp validation is
disabled and Xdebug is absent. PHP and Deno start with eight instances per
worker in every case, including cases targeting another language.

## Procedure

1. Build once. Reuse the same binary for all five cases.
2. Start a fresh master and 16 workers in private Linux mount, network, IPC
   and PID namespaces. Use private SHM and loopback; keep sources read-only.
3. Verify 16 distinct listener sockets, all PHP production startup records,
   all OPcache completions and the multithreaded runtime.
4. Complete 65,536 untimed selected-language requests over 128 connections,
   verify JSON responses and observe all 16 serving PIDs. Check all five paths.
5. Sample master/worker CPU time and RSS, run wrk for five minutes, then
   sample the same process family again.
6. Leave the server running without load. Sample RSS/CPU at the declared idle
   offsets. Do not trigger GC, a purge API, pool reset or service restart.
7. Request graceful shutdown of the owned master, verify all its workers exit,
   then start the next language's family. Do not rebuild between cases.

## Throughput and CPU

| Language | Completed requests | Load (s) | Successful req/s | CPU (s) | CPU avg | CPU µs/request | p99 (ms) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Rust | 84,059,611 | 300.092083 | 280,112.72 | 5,832.93 | 1,943.52% | 69.3904 | 8.151 |
| Lua | 32,653,111 | 300.092260 | 108,810.24 | 6,487.84 | 2,161.78% | 198.6898 | 5.072 |
| Deno | 20,514,300 | 300.069816 | 68,365.09 | 7,557.83 | 2,518.43% | 368.4176 | 11.011 |
| PHP / Symfony | 8,932,528 | 300.069803 | 29,768.17 | 9,000.04 | 2,999.08% | 1007.5580 | 13.659 |
| Python / Gufi-ASGI | 37,191,678 | 300.097149 | 123,932.13 | 7,250.66 | 2,415.92% | 194.9538 | 6.694 |

Average CPU microseconds per request = total process CPU-time delta × 1,000,000
/ completed HTTP responses. CPU time includes user and kernel time across the
master and every worker, including background runtime work. CPU average is
that same delta divided by the resource window; 100% equals one used core.
Only successful HTTP responses contribute to successful requests/s. Socket
errors and wrk's default two-second request timeout are reported separately.
There is no per-response Lua callback in the load generator.

The same measured binary was also checked with MiMalloc's own verbose output:
`purge_delay=0` and `purge_decommits=1` were confirmed and purging was observed
in that untimed configuration check. The timed cases do not enable verbose
allocator logging or force collection. See [MiMalloc environment options](https://github.com/microsoft/mimalloc#environment-options).

## Resident memory after load

| Language | Before load | End of load | +10s | +30s | +60s | +120s | RSS decrease (MiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Rust | 2,942.28 | 2,994.80 | 2,994.79 | 2,994.79 | 2,994.81 | 2,994.81 | -0.02 |
| Lua | 2,936.61 | 3,014.95 | 3,014.95 | 3,014.95 | 3,014.95 | 3,014.95 | 0.00 |
| Deno | 3,766.58 | 11,331.52 | 11,314.81 | 11,311.31 | 11,311.31 | 11,310.94 | 20.58 |
| PHP / Symfony | 2,964.58 | 3,111.38 | 3,111.38 | 3,111.38 | 3,106.62 | 3,056.35 | 55.03 |
| Python / Gufi-ASGI | 2,936.68 | 3,015.85 | 3,015.85 | 3,015.85 | 3,018.04 | 3,018.05 | -2.20 |

Values are MiB. RSS is summed across the master and 16 workers, so shared pages
are counted per process. It includes all five loaded runtimes. RSS decrease
is end-of-load RSS minus RSS at +120 seconds; a negative value means RSS grew.
The complete +10/+30/+60/+120 samples, actual sample times and per-process CPU
and RSS values are retained in the [JSON report](public/assets/benchmark.json).

MiMalloc is the Rust global allocator, as in the application's launcher policy.
It purges unused pages; it does not free live objects or force Zend, V8 or
CPython to discard their own heaps and caches. The installed language engines
and V8's prebuilt library keep their own allocation behavior. RSS by itself
does not establish a leak or identify which allocator owns retained pages.

## Source and tools

| Source | Revision |
| --- | --- |
| claviron | `1eea00a05ef1fd6526af9a1c135ce8f3d219dce7` |
| deno | `bfbe8434f7f6e0bd5e8d6308dfb2402603b140d2` |
| gufi | `147833418398d73ba2f9603db1e32f04f8698b2d` |
| orbitive | `bc0ebd2f0c5a2c9d953ac4f727d5dbcc50b8246a` |
| continuo | `d9c507ae7560c279f9eae6aa085cbf1e1b948657` |

Rust compiler: `rustc 1.99.1 (58cf8f9c3 2026-10-13)`. PHP 8.5.12 ZTS, CPython 3.15.2, Lua 5.5.1, Deno Core 0.413.0 / V8 15.0.245.2. MiMalloc Rust wrapper 0.1.52, libmimalloc-sys 0.1.49.

Binary SHA-256: `979daa43a57124f383e9770adeb834dc9a758493b649555576b8b9560134282b`.

The JSON records the source base revision, modified-source hashes, release
binary SHA-256 and dependency revisions. The same binary hash is verified
before and after every case. Linux samples read `/proc/<pid>/stat`; process
birth stamps and executable identity prevent a restarted worker or reused PID
from silently contributing to the totals. These are the supplied handlers;
payloads and framework work differ.

## Reproduce

Build and optimize the application once using the [launcher](../scripts/pentaglot.sh)
with `--release --native --optimize`. Run each isolated case with `--workers 16
--production --mimalloc release`, the capacities above and a distinct test
listener/configuration. Do not set `TOKIO_WORKER_THREADS`.
After application and OPcache warmup, use Python 3.11+ and wrk:

```sh
python3 examples/pentaglot/scripts/benchmark.py \
  --url http://127.0.0.1:8080 --runtime rust \
  --workers 16 --connections 128 --threads 8 --duration 5m \
  --idle-sample-seconds 10 30 60 120 \
  --binary examples/pentaglot/target/release/pentaglot --native \
  --label "CPU, RAM and OS" \
  --output examples/pentaglot/logs/rust-report.json \
  --raw-dir examples/pentaglot/logs/benchmark
```

Use one `--runtime` per invocation. The measurement script does not restart
servers: stop and join the family only after the last idle sample, then start
the next case. It rejects changed process sets, replaced PIDs, a different
executable, invalid idle schedules and connection counts that wrk would round
down. Raw logs and generator output stay under ignored `logs/`.

## Deno one-isolate comparison


Deno capacity comparison: same binary, 16 workers, 128 connections, 5 minutes
of load and 120 seconds idle. The original measurements above are retained.
Only Deno isolates per worker changed.

| Deno isolates/worker | Successful req/s | CPU µs/request | p99 (ms) | RSS start → end → +120s (MiB) | Errors |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 68,365 | 368.42 | 11.011 | 3,766.6 → 11,331.5 → 11,310.9 | 0 |
| 1 | 63,708 | 346.69 | 6.904 | 2,614.8 → 3,375.2 → 3,356.5 | 0 |

The [one-isolate report](public/assets/benchmark-deno-one.json) records the new
run and links its unchanged baseline.

Deno has no age/request-count rotation. Failed instances use the retry policy;
the default execution budget is 30 seconds. PHP max_life and max_requests are
null; its 60-second idle timeout trims excess capacity above max_idle 8.
Supervisor rotation max_age and RSS threshold are null. All 17 process birth
stamps stayed unchanged throughout the load and idle samples.

| Idle offset (s) | RSS (MiB) |
| ---: | ---: |
| 0 | 3,375.22 |
| 10 | 3,365.24 |
| 30 | 3,356.49 |
| 60 | 3,356.49 |
| 120 | 3,356.50 |

CPU time: 6,628.29s; completed responses: 19,118,693; measured load: 300.097179s.
RSS decrease after 120 seconds: 18.73 MiB.

Binary SHA-256: `979daa43a57124f383e9770adeb834dc9a758493b649555576b8b9560134282b`.

These are two single observations on the same shared host without CPU affinity.
The comparison varies Deno capacity and does not identify a runtime defect.

## Deno concurrent-request bridge comparison

Measured at `2026-10-28T17:53:56.750121+00:00`. Only Deno received timed load. Each worker
has one persistent isolate; the 16-worker/128-connection configuration, wrk
settings, warmup, allocator policy and all application source files match the
preceding one-isolate run. All five runtimes remain loaded. No other language
benchmark was repeated.

The new release/native binary uses request-local state, concurrent asynchronous
handlers in one isolate and direct static response bodies. Deno's source base
is `bfbe8434f7f6e0bd5e8d6308dfb2402603b140d2` with working-tree changes;
package versions are unchanged. The local Cargo source overrides changed source
selection without changing any package version. Configuration files were
compared byte-for-byte with the previous one-isolate campaign. The report
records hashes of the measured dependency sources.

| Metric | Previous bridge | Concurrent-request bridge | Observed change |
| --- | ---: | ---: | ---: |
| Completed responses | 19,118,693 | 28,626,235 | |
| Timed load (s) | 300.097179 | 300.073106 | |
| Successful req/s | 63,708.34 | 95,397.54 | +49.74% |
| Server CPU time (s) | 6,628.29 | 6,777.71 | |
| Server CPU average | 2,208.55% | 2,258.51% | |
| CPU µs/request | 346.6916 | 236.7657 | -31.71% |
| p99 (ms) | 6.904 | 8.110 | +17.47% |
| Connect/read/write/status/timeout errors | 0 / 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 / 0 | |

CPU/request uses the sum of user and kernel CPU-time deltas across the master
and every worker, divided by completed responses. CPU time includes the other
loaded runtimes' background work. It excludes the load generator.

| RSS observation | Previous bridge (MiB) | Concurrent-request bridge (MiB) |
| --- | ---: | ---: |
| Before load | 2,614.85 | 2,464.02 |
| End of load | 3,375.22 | 3,326.46 |
| Increase during load | 760.38 | 862.44 |
| Released by +120s | 18.73 | 18.58 |

| Idle offset (s) | Previous bridge RSS (MiB) | Concurrent-request bridge RSS (MiB) |
| ---: | ---: | ---: |
| 0 | 3,375.22 | 3,326.46 |
| 10 | 3,365.24 | 3,317.88 |
| 30 | 3,356.49 | 3,307.88 |
| 60 | 3,356.49 | 3,307.88 |
| 120 | 3,356.50 | 3,307.88 |

Absolute RSS at +120s was 48.62 MiB lower
(1.45%). The lower starting RSS and the larger load-to-start
increase are both retained in the table; the idle decrease alone is not evidence
of an allocator change. RSS sums 17 processes, counts shared pages per process
and includes all five runtimes. It does not isolate the V8 heap or establish a
leak. Neither GC nor an explicit purge was triggered.

The run used private mount, network, IPC and PID namespaces with read-only
sources and a separate writable PHP cache copy. All 16 PHP OPcaches and 128
persistent PHP runners were ready; Symfony production checks passed. All 16
SO_REUSEPORT listeners and the same 17 process birth stamps were verified.
No runtime failure/replacement was logged. The owned master and workers exited
gracefully after the last idle sample, with no survivors.

These are single observations taken at different times on the same shared
host without CPU affinity. Throughput and CPU/request improved in this run;
p99 increased by 1.206 ms. The measurements do not establish statistical
repeatability.

New binary SHA-256: `918f2e8bf49d4b80ce7cd81af5c4de08bd4a7192827a5d6580fe5207ea18c945`.

The [new JSON report](public/assets/benchmark-deno-multiplexed.json) contains all
process and idle samples, source hashes and links to the unchanged
[previous one-isolate report](public/assets/benchmark-deno-one.json) and
[original five-language report](public/assets/benchmark.json).

## Deno lower-concurrency check

Measured at `2026-10-28T18:11:19.782180+00:00` with the same release/native binary
`918f2e8bf49d4b80ce7cd81af5c4de08bd4a7192827a5d6580fe5207ea18c945`. No rebuild or source change was made. Server configuration,
16 workers, one isolate per worker, allocator settings, logging and all five
loaded runtimes are unchanged. The server's application admission limit remains
128 per worker. The load generator uses 16 persistent connections and eight
threads for one minute. Its preceding untimed warmup remains 65,536 requests
over 128 connections so every worker starts warm, as in the prior campaign.

| Metric | 128 connections / 5 minutes | 16 connections / 1 minute | Observed change |
| --- | ---: | ---: | ---: |
| Completed responses | 28,626,235 | 3,410,742 | |
| Load duration (s) | 300.073106 | 60.099279 | |
| Successful req/s | 95,397.54 | 56,751.80 | -40.51% |
| Server CPU time (s) | 6,777.71 | 868.19 | |
| CPU µs/request | 236.7657 | 254.5458 | +7.51% |
| p99 (ms) | 8.110 | 0.925 | -88.59% |
| RSS before load (MiB) | 2,464.02 | 2,466.38 | |
| RSS end of load (MiB) | 3,326.46 | 3,004.64 | |
| RSS +120s (MiB) | 3,307.88 | 2,990.30 | -9.60% |
| Released by +120s (MiB) | 18.58 | 14.34 | |
| Errors (all counters) | 0 | 0 | |

| Idle offset (s) | 128-connection RSS (MiB) | 16-connection RSS (MiB) |
| ---: | ---: | ---: |
| 0 | 3,326.46 | 3,004.64 |
| 10 | 3,317.88 | 3,000.64 |
| 30 | 3,307.88 | 2,989.89 |
| 60 | 3,307.88 | 2,989.89 |
| 120 | 3,307.88 | 2,990.30 |

RSS sums the same master and 16 workers with all five runtimes loaded; shared
pages count per process. CPU/request includes those processes' background work.
No GC or explicit purge was triggered. Process birth stamps remained stable,
no runtime replacement was logged, and the owned family exited gracefully with
no survivors after the final idle sample. Isolation again used private Linux
namespaces and a separate PHP cache copy.

The observed p99 was 0.925 ms under this load, versus 8.110 ms previously.
Concurrency and measurement duration both changed, and these are single runs on
a shared host. The result shows load-dependent latency without establishing
which queue, scheduling stage or server limit caused the earlier tail.

The [new JSON report](public/assets/benchmark-deno-c16-1m.json) preserves the full
samples and links the unchanged [128-connection report](public/assets/benchmark-deno-multiplexed.json).

## Deno four-isolate comparison

Measured at `2026-10-28T18:20:44.084514+00:00`. The concurrent-request bridge uses the same
release/native binary `918f2e8bf49d4b80ce7cd81af5c4de08bd4a7192827a5d6580fe5207ea18c945` as the preceding one-isolate
128-connection/five-minute run. No source or package version changed. The sole
configuration change is `instances: 1` to `instances: 4`; all other configuration
bytes match. Each of the 16 workers was verified to own four Deno threads,
for 64 total. The master remains part of the 17-process resource samples.

The load generator still uses eight threads, 128 persistent connections and
five minutes. Warmup is unchanged at 65,536 untimed requests. All five runtimes
remain loaded; Lua concurrency, PHP capacity/production settings, Tokio default
multithreading, mimalloc release and logging settings are unchanged.

| Metric | 1 isolate/worker | 4 isolates/worker | Observed change |
| --- | ---: | ---: | ---: |
| Completed responses | 28,626,235 | 25,513,397 | |
| Load duration (s) | 300.073106 | 300.099756 | |
| Successful req/s | 95,397.54 | 85,016.39 | -10.88% |
| Server CPU time (s) | 6,777.71 | 7,140.13 | |
| CPU µs/request | 236.7657 | 279.8581 | +18.20% |
| p99 (ms) | 8.110 | 10.165 | +25.34% |
| RSS before load (MiB) | 2,464.02 | 2,899.84 | |
| RSS end of load (MiB) | 3,326.46 | 6,618.66 | |
| RSS +120s (MiB) | 3,307.88 | 6,551.09 | +98.05% |
| Released by +120s (MiB) | 18.58 | 67.58 | |
| Errors (all counters) | 0 | 0 | |

| Idle offset (s) | 1-isolate RSS (MiB) | 4-isolate RSS (MiB) |
| ---: | ---: | ---: |
| 0 | 3,326.46 | 6,618.66 |
| 10 | 3,317.88 | 6,584.54 |
| 30 | 3,307.88 | 6,551.18 |
| 60 | 3,307.88 | 6,551.02 |
| 120 | 3,307.88 | 6,551.09 |

Four isolates used 18.20% more CPU per request and had
10.88% lower throughput in this run. p99 rose by
2.055 ms. Post-idle RSS was
3,243.21 MiB higher. RSS sums all 17 processes,
includes every runtime and counts shared pages per process. Neither GC nor an
explicit purge was triggered. These are single observations on a shared host
without CPU affinity, rather than a general result for every JavaScript workload.

The fresh family ran in private Linux mount/network/IPC/PID namespaces with
read-only sources and a separate writable PHP cache copy. All 16 OPcaches and
128 PHP runners completed their production startup checks. The same process
birth stamps persisted through load and idle; no runtime replacement was logged.
The owned master and all workers exited gracefully after the final sample,
with no survivors.

The [new JSON report](public/assets/benchmark-deno-multiplexed-four.json) retains
all process samples, source fingerprints and the link to the unchanged
[one-isolate baseline](public/assets/benchmark-deno-multiplexed.json).
