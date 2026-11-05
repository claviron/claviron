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
