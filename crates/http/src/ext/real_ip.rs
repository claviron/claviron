use std::net::IpAddr;

/// Request-local client address resolved by trusted forwarding policy.
/// Accepted peer facts remain in `ConnCtx`.
#[derive(Debug, Clone, Copy)]
pub struct RealIpExt(pub IpAddr);

/// Request-local trusted forwarding, otherwise the accepted connection's IP.
pub fn request_real_ip(extensions: &http::Extensions) -> Option<IpAddr> {
    extensions.get::<RealIpExt>().map(|ip| ip.0).or_else(|| {
        extensions.get::<std::sync::Arc<super::ConnCtx>>().map(|connection| {
            connection.real_ip.unwrap_or_else(|| connection.remote.ip())
        })
    })
}
