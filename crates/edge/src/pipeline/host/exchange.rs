//! Configured CORS admission and response headers, prepared as one layer.

use std::sync::Arc;

use claviron_http::config::cors::CorsConfig;
use http::{HeaderName, HeaderValue};

use super::*;

mod cors;

/// Request-local origin accepted by this exchange's CORS check.
#[derive(Default)]
pub struct ExchangeState {
    origin: Option<HeaderValue>,
}

impl ExchangeState {
    pub fn accepted_origin(&self) -> Option<&HeaderValue> {
        self.origin.as_ref()
    }
}

impl AsMut<ExchangeState> for ExchangeState {
    fn as_mut(&mut self) -> &mut ExchangeState {
        self
    }
}

/// Prepared actions for one effective host/prefix scope.
/// Only declared CORS and header actions contribute callbacks.
#[derive(Clone)]
pub struct ExchangeLayer {
    actions: Arc<ExchangeActions>,
}

struct ExchangeActions {
    cors: Option<CorsConfig>,
    set: Box<[(HeaderName, HeaderValue)]>,
    unset: Box<[HeaderName]>,
}

impl ExchangeLayer {
    /// Prepare an effective scope from already parsed header actions.
    ///
    /// Unsets run before sets, then accepted-origin CORS response headers run
    /// last. Config inheritance and prefix selection belong to host preparation.
    pub fn new(
        cors: Option<CorsConfig>,
        set: impl IntoIterator<Item = (HeaderName, HeaderValue)>,
        unset: impl IntoIterator<Item = HeaderName>,
    ) -> Self {
        Self {
            actions: Arc::new(ExchangeActions {
                cors,
                set: set.into_iter().collect(),
                unset: unset.into_iter().collect(),
            }),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.actions.cors.is_none()
            && self.actions.set.is_empty()
            && self.actions.unset.is_empty()
    }

    pub(crate) fn same_as(&self, other: &Self) -> bool {
        let (a, b) = (&self.actions, &other.actions);
        a.set == b.set && a.unset == b.unset
            && match (&a.cors, &b.cors) {
                (None, None) => true,
                (Some(a), Some(b)) => {
                    a.policy == b.policy
                        && a.credentials == b.credentials
                        && a.origins == b.origins
                }
                _ => false,
            }
    }

    pub(crate) fn into_pipeline(self) -> HostPipeline<ExchangeState> {
        let mut layer = HostLayerBuilder::default();
        self.register(&mut layer);
        HostPipelineBuilder { layers: vec![layer] }.build()
    }

    fn register<S: AsMut<ExchangeState>>(
        self,
        layer: &mut HostLayerBuilder<S>,
    ) {
        if let Some(cors) = &self.actions.cors {
            let cors = Arc::new(cors.clone());
            let request_cors = cors.clone();
            layer.before(move |head, state| {
                let (reply, origin) =
                    cors::check(&request_cors, &head.method, &head.headers);
                state.as_mut().origin = origin;
                match reply {
                    Some(reply) => ControlFlow::Break(reply),
                    None => ControlFlow::Continue(()),
                }
            });
            layer.after(move |head, state| {
                cors::finish(&cors, state.as_mut().origin.as_ref(), &mut head.headers);
            });
        }
        if !self.actions.set.is_empty() || !self.actions.unset.is_empty() {
            let actions = self.actions;
            // Response actions run in reverse order: headers before CORS.
            // They belong to the same layer, including a CORS rejection.
            layer.after(move |head, _| {
                for name in &actions.unset {
                    head.headers.remove(name);
                }
                for (name, value) in &actions.set {
                    head.headers.insert(name.clone(), value.clone());
                }
            });
        }
    }
}

impl<S: AsMut<ExchangeState>> HostLayer<S> for ExchangeLayer {
    fn prepare(self, layer: &mut HostLayerBuilder<S>) -> crate::Result<()> {
        self.register(layer);
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../../tests/pipeline/host/exchange.rs"]
mod tests;
