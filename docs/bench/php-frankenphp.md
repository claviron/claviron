# Persistent PHP and Symfony on Claviron and FrankenPHP

One serving process per side answered `GET /bench/plain` with HTTP 200 and
the three-byte body `ok\n`. Client and server shared a Linux x86-64 machine
with two Intel Xeon E5-2650 v2 processors and 32 logical CPUs. CPU idle
before the timed runs ranged from 95% to 100%. Each listener used plain
HTTP/1.1 with a verified backlog of 2,048.

Claviron and its embedded PHP backend used the published `0.2.0-alpha.26`
packages in a native release build with mimalloc. The embedded harness
used an inherited listener executor, 32 or 128 persistent PHP instances
and queues of up to 480 or 1,920 waiting requests, respectively. Its
OPcache warmup covered the worker adapters and application source.

FrankenPHP 1.13.5 at `a3663e9e2cc4c258018a28ee068e50cc12f3ed2d` included
Caddy 2.11.9. The build used Go 1.27.3, `CGO_ENABLED=1`, `GOAMD64=v2`,
`-O2 -g0 -march=native` for C code, `-trimpath`, tags
`nowatcher,nomercure,nobrotli` and a PGO profile. It ran with
`GOMAXPROCS=32`, `GOGC=100` and `GODEBUG=cgocheck=0`. It reserved one
regular PHP thread alongside 32 or 128 persistent workers and set
`max_threads` to 33 or 129. Admin, automatic HTTPS and access logging
were disabled. [FrankenPHP v1.13.5 source](https://github.com/php/frankenphp/releases/tag/v1.13.5)

Both processes mapped the same PHP 8.5.12 ZTS library and used the same INI.
OPcache used a 256 MiB cache, a 16 MiB interned-strings buffer and up to
20,000 accelerated files. Timestamp validation was disabled and the INI
scan directory was empty. The plain PHP test exercises each worker interface.
The FrankenPHP worker calls `header()` twice and `echo`. The Claviron
worker submits a fixed reply array through `claviron_application_respond()`.
The Symfony test uses the same locked FrameworkBundle 7.4.23 application,
kernel and controller on both sides in production mode. Claviron uses
Skene's buffered Symfony adapter.

[`wrk` 4.2.0](https://github.com/wg/wrk/releases/tag/4.2.0) ran for five
minutes per row with at least 30 seconds between runs. The order was
Claviron plain PHP, Claviron Symfony, FrankenPHP plain PHP, then FrankenPHP
Symfony. Each 128-instance process kept the same PID and process birth time
across the 128-, 256- and 1,024-connection rows. Before
timing, the harness verified the PHP library, checked the response and sent
four requests per instance. Serving CPU is the serving PID's user plus
system time per successful response. All sixteen runs recorded zero HTTP,
connect, read, write and timeout errors.

| Workload | PHP instances | Connections / client threads | Claviron CPU µs/response | FrankenPHP CPU µs/response | Claviron responses/s | FrankenPHP responses/s | p99 ms Claviron / FrankenPHP |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Plain PHP | 32 | 64 / 4 | 62.756 | 223.713 | 215,405.29 | 93,307.53 | 0.868 / 2.583 |
| Plain PHP | 128 | 128 / 32 | 60.857 | 222.413 | 207,272.32 | 92,698.41 | 4.084 / 6.030 |
| Plain PHP | 128 | 256 / 32 | 60.875 | 223.637 | 241,372.46 | 92,735.45 | 7.855 / 12.701 |
| Plain PHP | 128 | 1,024 / 32 | 64.718 | 227.227 | 299,895.64 | 90,015.02 | 16.611 / 31.648 |
| Symfony | 32 | 64 / 4 | 288.368 | 547.648 | 68,642.17 | 43,541.01 | 2.204 / 3.846 |
| Symfony | 128 | 128 / 32 | 292.374 | 552.373 | 74,066.83 | 42,964.05 | 7.649 / 10.708 |
| Symfony | 128 | 256 / 32 | 297.769 | 556.438 | 82,303.61 | 42,727.74 | 13.454 / 23.107 |
| Symfony | 128 | 1,024 / 32 | 312.430 | 565.523 | 88,139.26 | 41,397.54 | 48.384 / 56.488 |

FrankenPHP used roughly 20.5 to 20.9 CPU cores across the plain PHP rows and
served 90,015 to 93,308 responses/s. With 128 instances, Claviron rose from
207,272 to 299,896 responses/s as connections increased. These are single,
sequential observations at each setting. An October series used Claviron
revision `a50f1f879415b9e758f62e875115b72f50a9c816`, FrankenPHP 1.13.4 and
a JSON response. Its results are a separate series.
