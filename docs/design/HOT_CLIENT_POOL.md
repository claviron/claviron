# Keep useful clients, not just pool configuration

Design identifier: `HOT_CLIENT_POOL`.

A pool should save the work that made a client useful: connecting, negotiating
the protocol and preparing it for reuse. If every request creates that state
again, retaining an object called a pool has not bought much.

Claviron's pools retain live protocol clients and provide mechanisms to
maintain a declared warm floor. They do not all use the same implementation,
and the floor has to be read in the terms of the protocol that owns it.

## Prepare without hiding the cost

Configuration and construction are not readiness. The general client-pool
API performs no network IO in its constructor: acquisition can create a client
lazily, while its running lifecycle maintains the configured floor. Upstream's
H1 and H2 pools support `capacity.warmup` (default `false`). When enabled,
provider boot/reload or explicit profile start schedules preparation without
waiting for network readiness. Constructors perform no dialing.

Warm-up is owned work. Connection creation consumes the real capacity budget,
only the serving role starts the relevant maintenance, and stop/reload must
not let an old generation replenish itself after retirement.

## Distinguish the pools

| Owner | What is retained / maintained |
| --- | --- |
| General client pool (`claviron::clients`) | Adapter-owned physical clients, bounded request admission and a `min_idle` floor maintained by `run()`. |
| Upstream H1 | Exclusive reusable connections; `min_idle` counts live, leased and connecting connections. Maintenance starts on first demand, or at lifecycle activation with `warmup: true`. |
| Upstream H2 | Multiplexed connections per origin; stream saturation grows the pool up to max_live, max_idle bounds retention, and opt-in warmup prepares one initial connection. No min_idle floor. |
| FastCGI | Keepalive connections with `min_live` replenishment and an idle retention cap; an optional ping validates newly opened connections. |

These mechanisms do not mean an idle client is guaranteed at every instant.
A zero floor, startup, an unavailable peer or fully leased capacity can leave
an acquisition without a ready client. With warmup disabled, H2 creates its
connection on demand.
Those are observable states, not evidence that an empty placeholder is a live
connection.

## Reuse only what is safe to reuse

A returned client must still belong to the right pool generation and be in a
reusable protocol state. Upstream H1 tracks whether an exchange leaves its
connection reusable; a broken or incomplete exchange must not contaminate the
next one. Database session cleanup has the same purpose, described in
[Connection hijack](CONNECTION_HIJACK.md).

Warm capacity is not unlimited capacity. Physical clients, simultaneous
operations and queued callers can have different ceilings. A protocol with
multiplexing must not pretend one request equals one connection. Wait bounds,
retirement and replenishment backoff belong to declared policy, not hidden
constants chosen by the pool's caller.

Sharing configuration does not share connections. Fleet-composed client pools
can coordinate admission, but each physical client remains in its owner
process; remote users reach that owner through an exchange.

## Follow the implementation

- [General client-pool facade](../../src/clients.rs) and [typed policy](../../src/clients/config.rs).
- [Upstream H1 accounting](../../crates/upstream/src/proxy/h1/pool.rs) and [warm maintenance](../../crates/upstream/src/proxy/h1/pool/warm.rs).
- [Upstream H2 session and stream admission](../../crates/upstream/src/proxy/h2/pool.rs).
- FastCGI pool and live floor.
- [Upstream guide](../../crates/upstream/README.md) and FastCGI guide.

Use `HOT_CLIENT_POOL` for this reuse and preparation policy; a pool's actual
warm guarantee still comes from its configuration and running lifecycle.
