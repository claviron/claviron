//! Request identity derived from the accepted peer and listener proxy policy.

use std::net::IpAddr;
use std::sync::Arc;

use claviron_http::config::ProxyConfig;
use claviron_http::ext::ConnCtx;
use claviron_http::resolvers::resolve_real_ip_from_headers;
use http::HeaderMap;

use super::*;
use crate::service::{RequestRejection, VhostCtx};

pub(crate) mod extensions;
use extensions::ContextExtensions;

/// The identity produced for one request. It is also installed in its typed
/// extensions; native authentication must retain this exact same context.
#[derive(Default)]
pub struct IdentityState {
    pub(super) connection: Option<Arc<ConnCtx>>,
}

impl IdentityState {
    pub fn connection(&self) -> Option<&Arc<ConnCtx>> {
        self.connection.as_ref()
    }
}

impl AsMut<IdentityState> for IdentityState {
    fn as_mut(&mut self) -> &mut IdentityState {
        self
    }
}

impl AsRef<IdentityState> for IdentityState {
    fn as_ref(&self) -> &IdentityState { self }
}

/// Prepares one identity action from the accepted connection and listener
/// policy. It captures host metadata without retaining the application.
///
/// A fixed direct identity is shared. Proxy identities remain request-local;
/// forwarding headers cannot change accepted TLS, protocol or listener facts.
pub struct IdentityPolicy {
    connection: Arc<ConnCtx>,
    proxy: Option<ProxyConfig>,
    extensions: ContextExtensions,
}

impl IdentityPolicy {
    pub fn new(
        connection: Arc<ConnCtx>,
        host: &VhostCtx,
        proxy: Option<ProxyConfig>,
    ) -> Self {
        let extensions = ContextExtensions::new(&connection, host);
        Self { connection, proxy, extensions }
    }

    pub(super) fn register<S: AsMut<IdentityState>, R>(
        self,
        layer: &mut HostLayerBuilder<S, R>,
    ) {
        let Self { connection, proxy, extensions } = self;
        if let Some(ip) = connection.real_ip {
            layer.before(move |head, state| {
                extensions.inject_fixed(head, &connection, ip);
                state.as_mut().connection = Some(connection.clone());
                ControlFlow::Continue(())
            });
            return;
        }

        let remote = connection.remote.ip();
        match proxy.filter(|proxy| {
            !proxy.headers.is_empty()
                && proxy.trusted.iter().any(|network| network.contains(&remote))
        }) {
            Some(proxy) => register_dynamic(
                layer,
                connection,
                extensions,
                move |headers| {
                    // Keep header precedence and right-to-left trusted-hop parsing
                    // in HTTP's shared resolver. The client identity is never cached.
                    resolve_real_ip_from_headers(
                        headers,
                        remote,
                        &proxy.trusted,
                        &proxy.headers,
                    )
                },
            ),
            None => {
                register_dynamic(layer, connection, extensions, move |_| remote)
            }
        }
    }
}

fn register_dynamic<S: AsMut<IdentityState>, R>(
    layer: &mut HostLayerBuilder<S, R>,
    connection: Arc<ConnCtx>,
    extensions: ContextExtensions,
    resolve: impl Fn(&HeaderMap) -> IpAddr + Send + Sync + 'static,
) {
    layer.before(move |head, state| {
        let ip = resolve(&head.headers);
        let mut context = connection.as_ref().clone();
        context.real_ip = Some(ip);
        let context = Arc::new(context);
        extensions.inject_dynamic(head, &context, ip);
        state.as_mut().connection = Some(context);
        ControlFlow::Continue(())
    });
}

impl<S: AsMut<IdentityState>, R: From<RequestRejection>> HostLayer<S, R>
    for IdentityPolicy
{
    fn prepare(
        self,
        layer: &mut HostLayerBuilder<S, R>,
    ) -> crate::Result<()> {
        self.register(layer);
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../../tests/pipeline/host/identity.rs"]
mod tests;
