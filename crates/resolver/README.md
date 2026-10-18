# claviron-resolver

Shared DNS client contracts with an optional Hickory implementation.

This crate gives consumers a common query, answer, error, and factory boundary
without coupling them to a DNS backend. It knows no HTTP request, upstream pool,
vhost, Orbit table, or application schema. It is a DNS client capability, not an
authoritative DNS server or a background membership controller.

Applications normally use `claviron::resolver` through the root crate's
`resolver` feature. `resolver-hickory` adds the built-in driver. The
implementation crate has no default features, so a custom resolver needs no
Hickory dependency.

## Queries and answers

`Resolver::lookup(Query)` is object-safe and returns a boxed `Send` future
borrowing the resolver and query. Boxing is intentional on this cold control
path; it is not a per-HTTP-request origin-selection abstraction.

The query kinds are A, AAAA, SRV, and TXT. `Record` retains address values,
SRV priority/weight/port/target, and TXT chunks as raw byte strings. The consumer
decides how to expand SRV targets, combine address families, truncate an address
set, or dial a socket.

For example, this caller depends only on the shared capability:

```rust
use claviron::resolver::{Answer, Query, RecordType, ResolveError, Resolver};

async fn lookup_txt(
    resolver: &dyn Resolver,
    name: &str,
) -> Result<Answer, ResolveError> {
    resolver.lookup(Query::new(name, RecordType::Txt)).await
}
```

`Answer::valid_until` is remaining validity expressed as a process-local
monotonic `Instant`. Reusing a cached answer must not grant a fresh TTL. Do not
serialize that Instant into SHM or a wire message; a publishing consumer owns
the translation to its own expiry representation.

`ResolveErrorKind` distinguishes invalid configuration/name, NXDOMAIN,
no records, timeout, and backend failure. Backend errors retain their standard
error source without exposing backend-specific types in the public contract.

## Explicit factory selection

`ResolverFactory::open` constructs one shared resolver for the consumer's
`ResolverConfig`. An application binds its chosen implementation as
`dyn ResolverFactory` in its runtime registry. Consumers resolve that binding
when they need DNS; a missing binding is not repaired by choosing a driver
silently.

Selecting the Cargo feature does not install the binding. In particular,
Claviron's `upstream` feature selects resolver contracts, not a default DNS
driver. `HickoryFactory` is available to applications that explicitly choose
the built-in backend. Independent consumers can retain distinct resolver
instances and policies rather than using a process-global singleton.

## Resolver policy versus consumer policy

`ResolverConfig` declares:

- Nameserver socket addresses; an empty list uses system configuration.
- Per-transport-request timeout and attempt count.
- Driver cache capacity and minimum/maximum TTL policy.

Configuration is validated before backend construction. Invalid system or
explicit nameserver settings do not trigger a hidden public-DNS fallback.
Explicit nameservers use UDP and TCP at the declared port; unrepresentable
IPv6 scope/flow settings are rejected rather than stripped.

The timeout is not a total DNS lookup, application boot, HTTP request, or drain
deadline. Consumers own any overall execution budget, refresh scheduling,
failure behavior, fleet election/publication, and origin membership changes.
This crate does not start those services for them.

## Hickory driver behavior

The `hickory` implementation feature exposes `HickoryResolver` and
`HickoryFactory`. Construct the driver once for its owning consumer and use
lookups inside a live Tokio runtime. Clones share the backend and cache.
Construction does not issue a DNS query or register a Claviron `Runnable`.

Driver caching defaults to disabled (`cache_size: 0`); a consumer may already
retain answers until expiry. When enabled, positive and negative cache policy
uses the declared TTL bounds. The adapter tracks original answer deadlines so
a known cached result cannot gain lifetime from repeated reads. Its bounded
deadline tracking and cache clear epoch prevent forgotten provenance or an old
caller from reinstalling stale tracking after a clear.

Lookups/cache insertion are serialized across clones of that driver to preserve
these deadline semantics. Cancelling a caller releases its admission guard;
it does not promise that all Hickory shared protocol/cache work has
synchronously joined. The selected backend capabilities are Tokio and system
configuration, not implicit DNSSEC, DoH, DoT, or QUIC support.

## Related crates

- [Upstream](../upstream/README.md): DNS/SRV membership, refresh, and pool adoption.
- [Runtime](../runtime/README.md): application-owned capability binding.
- [Orbit](../orbit/README.md): shared-state primitives used by consumers, not by
  this resolver contract.

## Status and license

Part of Claviron's alpha development series; APIs may change. This README
describes the current source tree, not the placeholder package on crates.io.

Licensed under either [MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.
