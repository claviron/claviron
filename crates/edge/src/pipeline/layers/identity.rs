//! Request metadata derived from the accepted connection.

use std::sync::Arc;

use claviron_http::ext::ConnCtx;

use super::*;
use crate::service::VhostCtx;

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
    fn as_ref(&self) -> &IdentityState {
        self
    }
}

/// Prepare shared metadata for the accepted connection and selected host.
/// Forwarding headers do not alter this identity.
pub struct IdentityPolicy {
    connection: Arc<ConnCtx>,
    extensions: ContextExtensions,
}

impl IdentityPolicy {
    pub fn new(
        connection: Arc<ConnCtx>,
        host: &VhostCtx,
    ) -> Self {
        let extensions = ContextExtensions::new(&connection, host);
        Self { connection, extensions }
    }

    pub(super) fn register<S: AsMut<IdentityState>, R>(
        self,
        layer: &mut HostLayerBuilder<S, R>,
    ) {
        let Self { connection, extensions } = self;
        let ip = connection.real_ip.unwrap_or_else(|| connection.remote.ip());
        layer.before(move |head, state| {
            extensions.inject_fixed(head, &connection, ip);
            state.as_mut().connection = Some(connection.clone());
            ControlFlow::Continue(())
        });
    }
}

impl<S: AsMut<IdentityState>, R> HostLayer<S, R> for IdentityPolicy {
    fn prepare(
        self,
        layer: &mut HostLayerBuilder<S, R>,
    ) -> crate::Result<()> {
        self.register(layer);
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../../tests/pipeline/layers/identity.rs"]
mod tests;
