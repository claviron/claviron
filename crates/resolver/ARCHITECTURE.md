# claviron-resolver — Architecture

Shared DNS client contracts below application/runtime subsystems. Downstream
code imports only `claviron::resolver`; no dependency on upstream, HTTP,
AppState, providers, Orbit, zone storage or balancers belongs here.

## Public boundary

- `Resolver::lookup(Query)` is object-safe and returns a Send future borrowing
  its resolver and query. A boxed future is intentional on this cold DNS path;
  this is not upstream's request/acquire path.
- Query types are A, AAAA, SRV and TXT. Answers retain SRV priority/weight/port/
  target and raw TXT chunks; no socket dialing, SRV target expansion, family
  fallback or address truncation happens here. Consumers own those choices.
- `Answer::valid_until` is a process-local monotonic deadline. Preserve remaining
  cache lifetime: reading a cached answer must not grant a fresh minimum TTL.
  Instants are not an SHM/wire format; consumers translate expiry when publishing.
- Error kinds distinguish NXDOMAIN, no records, timeout, invalid input/config
  and backend failure. Backend errors retain their standard Error source.
- `ResolverConfig` owns nameserver, request timeout/retry, cache and TTL policy.
  Empty nameservers select system configuration; explicit socket addresses use
  UDP plus TCP on the configured port. Config construction is validated before
  backend creation; no hidden public DNS fallback repairs bad system settings.

## Driver and ownership

Default features expose only contracts/config, including `ResolverFactory`:
the application binds one implementation as `dyn ResolverFactory` in its
registry, and a consumer that needs DNS resolves that binding and fails its
boot when none exists. Feature `hickory` adds the private Hickory adapter, the
public `HickoryResolver` and `HickoryFactory`. Only Tokio and system
configuration are selected on Hickory; DNSSEC/DoH/DoT/QUIC are not implied.
Construction reads configuration but does not start a resolver Runnable or
issue DNS queries. Hickory starts its own protocol work when lookup is polled;
use it within a live Tokio runtime. Dropping a caller future is not a promise
that Hickory's shared protocol/cache work has synchronously joined.
Clones share the same driver/cache. No process-global singleton is installed.

Hickory applies configured min/max TTL at cache insertion to positive and
negative responses. In 0.26, cached Lookup reconstructs its deadline from
whole-second remaining record TTL and can extend expiry by a fraction of a
second. The adapter therefore keeps original per-query deadlines in a bounded
map (at most cache_size), shared by clones. A known cache hit cannot advance
its first expiry. Unknown/expired provenance clears that query in Hickory before
lookup; removed tracking cannot turn into fresh cached TTL. Negative results
retain bounded provenance using the configured negative TTL limits.

One async admission guard serializes lookup/cache insertion across driver
clones; synchronous state locks never span await. This intentionally favors
consistent cache deadlines over concurrent queries within one cold driver.
Cancellation releases the guard; clear_cache clears both stores and advances
an epoch so an older caller cannot repopulate deadline tracking. Hickory shared
protocol work remains subject to the cancellation boundary above.
The timeout is Hickory's per-request transport timeout, not an overall lookup,
HTTP request, boot or drain deadline. Callers own any total execution budget.
Nonzero IPv6 scope ids/flowinfo cannot be represented by Hickory's nameserver
IP field and are rejected explicitly rather than silently stripped.

The facade's `resolver` feature exposes contracts; `resolver-hickory` adds the
built-in driver. A custom downstream implementation therefore needs no Hickory
in its dependency graph. Server/bootstrap does not auto-register this
capability and no consumer defaults to Hickory: the facade's `upstream`
feature pulls in the contracts only, and the application chooses the driver by
binding a `dyn ResolverFactory`.

## Consumers and verification

Upstream will own resolution generations, fleet owner election/publication,
origin membership/retirement and pool/balancer updates in Phase 6. Its boot and
failure policy is not generic DNS configuration. ACME/outbound fetch are future
consumers; none is migrated in this foundation step. `claviron-dns` remains the
separate DNS server and can be queried through an explicit nameserver.

Offline tests exercise a scripted custom implementation, adapter conversion,
remaining TTL, record-type filtering, negative/timeout error classification and
explicit nameserver construction. No external DNS, sleeps, SHM or running server
are required. A facade integration test proves a consumer can implement the
trait without importing Hickory or hidden core crate paths.
