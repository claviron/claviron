# claviron-geoip Architecture

`geoip` provides a small GeoIP lookup service for Claviron. It
is used at connection/request level for operational and legal decisions,
such as country-aware policy, non-routable defaults, and edge diagnostics.

## Role

The crate loads a MaxMind-style country database, keeps a small in-memory
cache, and exposes lookup helpers to the rest of the process.

The important output today is country code. Deeper city/network
enrichment can be added later without forcing request paths to carry the
whole database lookup model.

## Boot Model

`GeoIpProvider` is a Congif `Configurable`: it declares `geoip.yaml` in the
runtime config directory as its source and is registered through `Register`,
bound to the application's frozen configuration environment, so `${NAME}`
placeholders expand from that snapshot and never from the live process
environment. The reader prepares and validates the file before any provider
boots; `configure` applies it. An absent file is a declaration of its own —
GeoIP disabled, lookups answer the fallback code — and no crate re-implements
that branch. Unknown keys and missing variables fail preparation with the path
in the error.

GeoIP is bootstrapped once through `boot_geoip`. The current process-wide
service uses `OnceCell`, so changing the database path or enabled state is
not a hot-reload operation. Restart the process when changing GeoIP
runtime configuration.

Database read or decode failures remain non-fatal and use the fallback lookup
behavior. Both the resolved database path and the underlying error appear in
the log message itself, not only in structured fields, so plain journal output
includes the diagnostic.

## Non-Routable Addresses

Private, local, and otherwise non-routable addresses return the configured
fallback code. This keeps local/dev traffic deterministic and prevents
"unknown" from accidentally looking like a real country decision.

## Cache policy

`GeoIpConfig.cache` owns the lookup capacity and TTL, with compatibility defaults
of 100,000 entries and 30 minutes. The process-wide cache is initialized from
that configuration at boot, including disabled/failed-database initialization.
Pre-boot lookups do not create a cache. Zero capacity or TTL disables retention;
parsing rejects values beyond Moka's timestamp arithmetic bound. No new reload
mechanism is introduced. The fallback code is installed before enabled/database
early returns, independently of successful reader creation.
