//! The Backend plan a scope starts from: Link when declared, and the file
//! fallback.

use std::sync::Arc;

use crate::pipeline::extension::RouteContext;
use crate::pipeline::host::{BackendHandler, DispatchStep};
use crate::pipeline::layers::prepare_fallback;

#[derive(Clone)]
pub(super) struct BackendPlan {
    pub steps: Vec<Arc<dyn BackendHandler>>,
    pub fallback: Option<Arc<dyn DispatchStep>>,
}

impl BackendPlan {
    pub(super) fn prepare(context: &RouteContext<'_>) -> Self {
        if context
            .setting(|scope| scope.redirect.as_ref())
            .is_some_and(|rules| !rules.is_empty())
        {
            return Self { steps: Vec::new(), fallback: None };
        }
        let state = context.state;
        let config = context.route;
        #[cfg(feature = "orbit")]
        let policy = context.policy();
        let steps = std::iter::empty::<Arc<dyn BackendHandler>>();
        #[cfg(feature = "orbit")]
        let steps = steps.chain(
            crate::pipeline::link::LinkBackend::new(
                state,
                config.orbit(),
                Some(context.prefix),
                policy.requests,
            )
            .map(|owner| Arc::new(owner) as Arc<dyn BackendHandler>),
        );
        Self {
            steps: steps.collect(),
            fallback: prepare_fallback(state, config),
        }
    }
}
