//! Forwarded client identity is request-local; socket/TLS facts stay unchanged.

use std::net::IpAddr;
use claviron_http::config::ProxyConfig;
use claviron_http::ext::RealIpExt;
use claviron_http::resolvers::resolve_real_ip_from_headers;
use super::*;

pub struct ProxyLayer {
    peer: IpAddr,
    config: ProxyConfig,
}

impl ProxyLayer {
    /// Select once for the accepted peer. Untrusted peers and absent header
    /// declarations contribute no callback or request context allocation.
    pub fn new(peer: IpAddr, config: ProxyConfig) -> Option<Self> {
        (!config.headers.is_empty() && config.trusts(peer)).then_some(Self { peer, config })
    }
}

impl<S, R> HostLayer<S, R> for ProxyLayer {
    fn prepare(self, layer: &mut HostLayerBuilder<S, R>) -> crate::Result<()> {
        layer.before(move |head, _| {
            let ip = resolve_real_ip_from_headers(
                &head.headers, self.peer, &self.config.trusted, &self.config.headers,
            );
            head.extensions.insert(RealIpExt(ip));
            ControlFlow::Continue(())
        });
        Ok(())
    }
}
