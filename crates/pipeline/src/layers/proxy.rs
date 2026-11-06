//! Forwarded client identity is request-local; socket/TLS facts stay unchanged.

use super::*;
use claviron_http::config::{ListenerConfig, ProxyConfig};
use claviron_http::ext::{ConnCtx, RealIpExt};
use claviron_http::resolvers::resolve_real_ip_from_headers;
use std::net::IpAddr;
use std::sync::Arc;

use crate::response::RequestRejection;

#[derive(Clone)]
pub struct ProxyLayer {
    source: Source,
}

#[derive(Clone)]
enum Source {
    Peer { peer: IpAddr, config: ProxyConfig },
    Listener(ProxyConfig),
}

impl ProxyLayer {
    /// Select once for the accepted peer. Untrusted peers and absent header
    /// declarations contribute no callback or request context allocation.
    pub fn new(
        peer: IpAddr,
        config: ProxyConfig,
    ) -> Option<Self> {
        (!config.headers.is_empty() && config.trusts(peer))
            .then_some(Self { source: Source::Peer { peer, config } })
    }

    /// Retain only this listener's configured forwarding.
    /// The accepted peer is borrowed at execution; transport facts stay fixed.
    pub fn for_listener(listener: &ListenerConfig) -> Option<Self> {
        let config = listener.proxy.as_ref()?;
        (!config.headers.is_empty() && !config.trusted.is_empty())
            .then(|| Self { source: Source::Listener(config.clone()) })
    }
}

impl<S, R: From<RequestRejection>> HostLayer<S, R> for ProxyLayer {
    fn prepare(
        self,
        layer: &mut HostLayerBuilder<S, R>,
    ) -> crate::Result<()> {
        match self.source {
            Source::Peer { peer, config } => {
                layer.before(move |head, _| {
                    head.extensions.insert(RealIpExt(
                        resolve_real_ip_from_headers(
                            &head.headers,
                            peer,
                            &config.trusted,
                            &config.headers,
                        ),
                    ));
                    ControlFlow::Continue(())
                });
            }
            Source::Listener(config) => {
                layer.before_context(move |head, _, context| {
                    let connection = context
                        .map(|context| context.connection.as_ref())
                        .or_else(|| {
                            head.extensions
                                .get::<Arc<ConnCtx>>()
                                .map(Arc::as_ref)
                        });
                    let Some(connection) = connection else {
                        return ControlFlow::Break(
                            RequestRejection::MissingIdentity.into(),
                        );
                    };
                    let peer = connection.remote.ip();
                    if connection.real_ip.is_none() && config.trusts(peer) {
                        let ip = resolve_real_ip_from_headers(
                            &head.headers,
                            peer,
                            &config.trusted,
                            &config.headers,
                        );
                        head.extensions.insert(RealIpExt(ip));
                    }
                    ControlFlow::Continue(())
                });
            }
        }
        Ok(())
    }
}
