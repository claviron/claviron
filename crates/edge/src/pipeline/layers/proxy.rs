//! Forwarded client identity is request-local; socket/TLS facts stay unchanged.

use super::*;
use claviron_http::config::{HttpSnapshot, ProxyConfig};
use claviron_http::ext::{ConnCtx, RealIpExt};
use claviron_http::resolvers::resolve_real_ip_from_headers;
use std::net::IpAddr;
use std::sync::Arc;

use crate::HostConfig;
use crate::service::RequestRejection;

#[derive(Clone)]
pub struct ProxyLayer {
    source: Source,
}

#[derive(Clone)]
enum Source {
    Peer { peer: IpAddr, config: ProxyConfig },
    Listeners(Arc<[(claviron_http::ListenerId, ProxyConfig)]>),
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

    /// Retain only configured forwarding on listeners that expose this host.
    /// The accepted peer is borrowed at execution; transport facts stay fixed.
    pub(crate) fn for_host(
        http: &HttpSnapshot,
        host: &HostConfig,
    ) -> Option<Self> {
        let mut listeners = Vec::new();
        for (id, listener) in &http.listeners {
            if host.allows_listener(id)
                && let Some(config) = &listener.proxy
                && !config.headers.is_empty()
                && !config.trusted.is_empty()
            {
                listeners.push((id.clone(), config.clone()));
            }
        }
        (!listeners.is_empty())
            .then(|| Self { source: Source::Listeners(listeners.into()) })
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
            Source::Listeners(listeners) => {
                layer.before_context(move |head, _, context| {
                    let connection = context
                        .map(|context| context.connection)
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
                    let Some((_, config)) = listeners
                        .iter()
                        .find(|(id, _)| *id == connection.listener)
                    else {
                        return ControlFlow::Continue(());
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
