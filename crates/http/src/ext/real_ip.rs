use std::net::IpAddr;

/// Resolved client address supplied by the transport or trusted proxy policy.
#[derive(Debug, Clone, Copy)]
pub struct RealIpExt(pub IpAddr);
