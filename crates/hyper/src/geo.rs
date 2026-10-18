//! Where a connection came from, as far as this transport can tell.
//!
//! One function, but it had been living in `protocol.rs` among the H1 builder
//! settings and socket options — two different questions filed under one name.
//! A reader looking for "what fills `ConnCtx.country_code`" had no reason to
//! open a file about protocol configuration.

use std::net::IpAddr;

/// Resolves a peer address to a country, or `None` when this build has no
/// database to ask.
pub(crate) fn country_code(ip: IpAddr) -> Option<String> {
    #[cfg(feature = "geoip")]
    {
        Some(claviron_geoip::geoip_lookup(ip))
    }
    #[cfg(not(feature = "geoip"))]
    {
        let _ = ip;
        None
    }
}
