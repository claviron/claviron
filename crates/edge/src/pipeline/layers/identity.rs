//! Metadata exported when a prepared backend takes ownership of a request.

use claviron_http::policy::HttpPolicy;
use http::Extensions;

use super::*;
use crate::pipeline::host::HostContext;

pub(crate) mod extensions;

/// Export accepted connection/host facts and the scope's effective policy.
/// Forwarded client identity stays request-local; connection facts are shared.
#[derive(Clone, Copy)]
pub struct IdentityPolicy {
    pub(in crate::pipeline) policy: HttpPolicy,
}

impl IdentityPolicy {
    pub fn new(policy: HttpPolicy) -> Self {
        Self { policy }
    }

    pub(in crate::pipeline) fn inject(
        &self,
        values: &mut Extensions,
        context: Option<HostContext<'_>>,
    ) {
        extensions::inject(values, self.policy, context);
    }
}

impl<S, R> HostLayer<S, R> for IdentityPolicy {
    fn prepare(
        self,
        layer: &mut HostLayerBuilder<S, R>,
    ) -> crate::Result<()> {
        layer.identity = Some(self.policy);
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../../tests/pipeline/layers/identity.rs"]
mod tests;
