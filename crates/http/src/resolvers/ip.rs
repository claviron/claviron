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

/// Extract real IP from proxy headers (X-Forwarded-For, X-Real-IP, etc.)
fn extract_ip_from_headers(
    headers: &HeaderMap,
    order: &[HeaderName],
) -> Option<IpAddr> {
    for name in order {
        if let Some(s) = headers.get(name).and_then(|v| v.to_str().ok()) {
            // XFF: "client, proxy1, proxy2" -> the IP on the left side will be used
            for cand in s.split(',').map(|p| p.trim()) {
                if let Ok(ip) = cand.parse() {
                    return Some(ip);
                }
            }
        }
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
        extract_ip_from_headers(headers, header_order).unwrap_or(remote_ip)
    } else {
        // Not behind trusted proxy
        remote_ip
    }
}

#[cfg(test)]
#[path = "../../tests/resolvers/ip.rs"]
mod tests;
