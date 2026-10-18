# claviron-geoip

Boot-configured country lookup from a local MaxMind-style database.

This crate loads a country MMDB into process memory, caches IP lookups, and
returns country-code strings. It does not perform network geolocation calls,
download databases, resolve DNS names, or decide application access policy.

Applications normally use `claviron::geoip` through the root crate's `geoip`
feature. The facade also forwards this capability to already selected ingress
implementations. Callers supply an IP address; trusted peer/forwarded-address
selection belongs to the ingress, not this lookup primitive.

## Configure once at startup

`GeoIpProvider` declares `geoip.yaml` under Claviron's configuration directory.
The runtime's Congif reader prepares it and expands declared environment values
before configuration is applied. For example:

```yaml
enabled: true
db_path: storage/geoip/GeoLite2-Country.mmdb
non_routable: ZZ
```

The operator supplies the database file and manages its updates and applicable
licensing separately. Relative paths follow the application process's working
directory. Unknown configuration keys or unresolved environment placeholders
fail configuration preparation.

An absent file disables initialization. If a document is present, `enabled`
defaults to true; the built-in database path is
`/usr/share/GeoIP/GeoLite2-Country.mmdb` and the fallback code is `ZZ`.

`register(state)` registers the configurable owner. `boot_geoip(config)` is the
initializer used by that owner; it reads the file into an owned byte vector
and installs one process-wide reader. It is not a per-request file read or a
memory-mapped database.

## Lookup and fallback

```rust
use std::net::IpAddr;
use claviron::geoip::geoip_lookup;

fn country_for_peer(peer: IpAddr) -> String {
    geoip_lookup(peer)
}
```

Lookup tries `country.iso_code`, then `registered_country.iso_code`. Missing
records, decode errors, or an unavailable reader return the fallback rather
than failing the HTTP request. Database read/parse failures are logged and
remain non-fatal.

Selected local/private ranges bypass the database: IPv4 loopback, private,
link-local and broadcast addresses; IPv6 loopback, unique-local, unspecified,
and multicast addresses. This is the current explicit classification, not a
complete public-address validation or routing-policy implementation.

Country results are hints derived from the supplied address and database.
They are not proof of a user's identity, physical location, language, or
authorization. Applications must decide how unknown/fallback results affect
their policy.

## Process lifetime and cache

The reader and fallback configuration use process-wide once-initialized cells.
Changing the database path or enabled state is not a hot-swap operation;
restart the process to apply a new dataset/configuration consistently.
Initialization does not clear an already populated lookup cache.

The current Moka cache is process-local, with a built-in capacity of 100,000
entries and a 30-minute TTL. Those are implementation values, not configurable
fields in `geoip.yaml`. It caches fallback results too. This is neither a
named application cache store nor a shared fleet cache.

## Related crates

- [Runtime](../runtime/README.md): prepared configuration and provider registration.
- [HTTP](../http/README.md): trusted request/connection context.
- [Edge](../edge/README.md): policies that may consume country information.

## Status and license

Part of Claviron's alpha development series; APIs and configuration may change.

Licensed under either [MIT](https://github.com/claviron/claviron/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/claviron/claviron/blob/main/LICENSE-APACHE), at your option.
