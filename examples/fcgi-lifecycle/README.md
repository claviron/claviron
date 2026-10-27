# FastCGI lifecycle harness

Exercises FastCGI ownership against an isolated Unix-socket mock, without PHP,
a Claviron server boot, production configuration or an existing listener.

```sh
cargo run --release -- 5
```

The required argument declares a per-case diagnostic deadline in seconds; it
is a harness watchdog, not a production response timeout. Both one-shot and
bounded keep-alive modes cover an abandoned silent SSE response, cancellation
before headers, a blocked request upload, response backpressure and natural
completion. Every abandonment is followed by a successful request; the pooled
case has one live slot. The harness creates and removes its own temporary
socket/configuration state and uses no sleeps or polling loops.

The repair cases configure a one-hour retry backoff, then remove a pool during
failed repair or a silent ping probe. Retirement must complete within the
supplied diagnostic bound, leave the application token untouched, and preserve
an already accepted response. These cases exercise actual sockets outside the
library test suite.
