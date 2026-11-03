//! Prepared typed request metadata; no application or request body is retained.

use std::net::IpAddr;
use std::sync::Arc;

use claviron_http::ext::{ConnCtx, HostKey, HostName, RealIpExt};
use http::{Extensions, request::Parts};

use crate::service::VhostCtx;

/// Replace a request-owned value in its existing box, or allocate it once when
/// absent. The entry API keeps both cases to one type-map lookup.
fn set_context<T: Clone + Send + Sync + 'static>(
    extensions: &mut Extensions,
    value: T,
) {
    let mut incoming = Some(value);
    let slot = extensions.get_or_insert_with(|| {
        incoming.take().expect("extension initializer runs once")
    });
    if let Some(value) = incoming {
        // Publish the replacement before dropping the previous value, as
        // Extensions::insert does, without replacing the box itself.
        drop(std::mem::replace(slot, value));
    }
}

/// The map capacity and fixed values are prepared with the host binding.
/// Each request owns its own boxes; populated native maps retain their boxes.
pub(super) struct ContextExtensions {
    template: Extensions,
    fixed: FixedContext,
}

struct FixedContext {
    hostname: HostName,
    key: HostKey,
}

impl FixedContext {
    fn inject(
        &self,
        extensions: &mut Extensions,
    ) {
        set_context(extensions, self.hostname.clone());
        set_context(extensions, self.key.clone());
    }
}

impl ContextExtensions {
    pub(super) fn new(
        connection: &Arc<ConnCtx>,
        host: &VhostCtx,
    ) -> Self {
        let fixed = FixedContext {
            hostname: host.hostname.clone(),
            key: host.vhost_key.clone(),
        };
        let mut template = Extensions::new();
        template.insert(connection.clone());
        template.insert(RealIpExt(
            connection.real_ip.unwrap_or_else(|| connection.remote.ip()),
        ));
        fixed.inject(&mut template);
        Self { template, fixed }
    }

    pub(super) fn inject_fixed(
        &self,
        head: &mut Parts,
        connection: &Arc<ConnCtx>,
        ip: IpAddr,
    ) {
        if head.extensions.is_empty() {
            head.extensions = self.template.clone();
        } else {
            self.inject_populated(&mut head.extensions, connection, ip);
        }
    }

    fn inject_populated(
        &self,
        extensions: &mut Extensions,
        connection: &Arc<ConnCtx>,
        ip: IpAddr,
    ) {
        set_context(extensions, connection.clone());
        self.fixed.inject(extensions);
        set_context(extensions, RealIpExt(ip));
    }
}

#[cfg(test)]
#[path = "../../../../tests/pipeline/layers/identity/extensions.rs"]
mod tests;
