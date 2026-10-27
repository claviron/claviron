use std::net::IpAddr;

use http::{HeaderMap, HeaderName};
use ipnet::IpNet;

/// Check if remote IP is a trusted proxy
fn is_trusted_proxy(
    remote: IpAddr,
    trusted: &[IpNet],
) -> bool {
    trusted.iter().any(|n| n.contains(&remote))
}

/// Resolve only the highest-priority header present. Malformed identity must
/// not fall through to a less-preferred, potentially attacker-controlled field.
fn extract_ip_from_headers(
    headers: &HeaderMap,
    remote: IpAddr,
    trusted: &[IpNet],
    order: &[HeaderName],
) -> Option<IpAddr> {
    for name in order {
        if !headers.contains_key(name) {
            continue;
        }
        if name == http::header::HeaderName::from_static("x-forwarded-for") {
            let mut client = remote;
            // Header occurrences and comma-separated hops both retain wire
            // order. Start at the socket side and never cross an untrusted hop.
            for value in headers.get_all(name).iter().rev() {
                let Ok(value) = value.to_str() else { return Some(client) };
                for hop in value.rsplit(',') {
                    if !is_trusted_proxy(client, trusted) {
                        return Some(client);
                    }
                    let Ok(address) = hop.trim().parse() else {
                        return Some(client);
                    };
                    client = address;
                }
            }
            return Some(client);
        }
        // X-Real-IP and custom identity fields name one address, not a chain.
        let mut values = headers.get_all(name).iter();
        let value = values.next()?;
        if values.next().is_some() {
            return None;
        }
        return value.to_str().ok()?.trim().parse().ok();
    }
    None
}

/// Resolve real IP from request headers (used in H1/H2 first request and H3 middleware)
pub fn resolve_real_ip_from_headers(
    headers: &HeaderMap,
    remote_ip: IpAddr,
    trusted_proxies: &[IpNet],
    header_order: &[HeaderName],
) -> IpAddr {
    if !trusted_proxies.is_empty()
        && is_trusted_proxy(remote_ip, trusted_proxies)
    {
        // Extract from headers
        extract_ip_from_headers(
            headers,
            remote_ip,
            trusted_proxies,
            header_order,
        )
        .unwrap_or(remote_ip)
    } else {
        // Not behind trusted proxy
        remote_ip
    }
}

#[cfg(test)]
#[path = "../../tests/resolvers/ip.rs"]
mod tests;
