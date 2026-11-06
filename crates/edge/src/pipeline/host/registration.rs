//! Code-defined layer factories evaluated while compiling serving host scopes.

use std::sync::Arc;

use claviron_http::{config::ListenerConfig, ext::PathPrefix};
use claviron_runtime::AppState;
use claviron_types::Host;

use super::HostPipelineBuilder;
use crate::pipeline::extension::{PreparedScope, RouteLayer};
use crate::pipeline::routable::matches_protocol;
use crate::pipeline::{HostAppRegistry, RouteProtocol};
use crate::{Result, Site};

/// Borrowed preparation input for a code-defined host contribution.
/// Nothing here is retained by dispatch unless the factory explicitly owns it.
pub struct LayerContext<'a> {
    pub state: &'a AppState,
    pub host: &'a Host,
    /// The canonical host declaration, before path selection.
    pub host_config: &'a Site,
    /// The URL scope already selected by the host compiler.
    pub prefix: &'a PathPrefix,
    /// None denotes the protocol-independent lookup. A restricted registration
    /// enters only its matching listener plans.
    pub protocol: Option<RouteProtocol>,
    pub listener: Option<&'a ListenerConfig>,
    pub scope: PreparedScope<'a>,
}

pub(in crate::pipeline) trait LayerRegistration:
    Send + Sync
{
    fn matches(
        &self,
        context: &LayerContext<'_>,
        protocol: RouteProtocol,
    ) -> bool;
    fn prepare(
        &self,
        context: &LayerContext<'_>,
    ) -> Result<HostPipelineBuilder>;
}

struct Registration<S, F> {
    select: S,
    prepare: F,
}

impl<S, F> LayerRegistration for Registration<S, F>
where
    S: Fn(&Host, &Site, RouteProtocol) -> bool + Send + Sync,
    F: Fn(&LayerContext<'_>) -> Result<HostPipelineBuilder> + Send + Sync,
{
    fn matches(
        &self,
        context: &LayerContext<'_>,
        protocol: RouteProtocol,
    ) -> bool {
        (self.select)(context.host, context.host_config, protocol)
    }

    fn prepare(
        &self,
        context: &LayerContext<'_>,
    ) -> Result<HostPipelineBuilder> {
        (self.prepare)(context)
    }
}

impl HostAppRegistry {
    /// Register a shared layer without a configuration directive or provider.
    /// Select canonical hosts and protocol families at boot/reload. Selected
    /// layers run after directive policies and before Routable contributions
    /// and the terminal Rust router. Register before the first publication.
    pub fn register_layer<L>(
        &self,
        select: impl Fn(&Host, &Site, RouteProtocol) -> bool + Send + Sync + 'static,
        layer: L,
    ) where
        L: RouteLayer + 'static,
    {
        self.register_pipeline(select, move |context| {
            let mut builder = HostPipelineBuilder::new();
            builder.layer(crate::pipeline::layers::ScopedExtension(
                &layer,
                &context.scope,
            ))?;
            Ok(builder)
        });
    }

    /// Prepare a builder or compiled pipeline for each selected host scope.
    /// Its actions move into the native flat plan. Empty contributions add no
    /// dispatch node. The factory may borrow effective policy and listener
    /// settings while preparing its owned callbacks. Register before the first
    /// publication; a failed factory rejects the candidate publication.
    pub fn register_pipeline<P>(
        &self,
        select: impl Fn(&Host, &Site, RouteProtocol) -> bool + Send + Sync + 'static,
        prepare: impl Fn(&LayerContext<'_>) -> Result<P> + Send + Sync + 'static,
    ) where
        P: Into<HostPipelineBuilder>,
    {
        self.layer_registrations
            .write()
            .expect("host layer registrations lock poisoned")
            .push(Arc::new(Registration {
                select,
                prepare: move |context: &LayerContext<'_>| {
                    prepare(context).map(Into::into)
                },
            }));
    }

    pub(crate) fn prepare_registered_layers(
        &self,
        context: &LayerContext<'_>,
        builder: &mut HostPipelineBuilder,
    ) -> Result<()> {
        let registrations = self
            .layer_registrations
            .read()
            .expect("host layer registrations lock poisoned")
            .clone();
        for registration in registrations {
            if matches_protocol(context.protocol, |protocol| {
                registration.matches(context, protocol)
            }) {
                builder.append(registration.prepare(context)?);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../../tests/pipeline/host/registration.rs"]
mod tests;
