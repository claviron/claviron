# Upstream lifecycle harness

This independent executable checks provider-owned H1 warm maintenance through
Claviron's public facade. It uses temporary configuration and ephemeral loopback
listeners, without a server fleet or an external backend.

```sh
cargo run --release -- 5
```

The required argument is a positive per-case diagnostic bound in seconds, not a
production timeout. Cases cover cancellation before the first warm poll, three
provider restarts with an accepted response still pending, explicit profile
stop/start, cancellation during a silent TLS handshake, and cancellation in a
configured one-hour retry wait. Retry and handshake budgets in the synthetic
configuration are one hour so cancellation cannot be mistaken for those budgets
expiring. The harness does not certify health probes, DNS resolution, or
fleet-wide lifecycle behavior.
