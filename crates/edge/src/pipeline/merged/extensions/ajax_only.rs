use super::{
    RouteCondition, RouteContext, RouteEffect, RouteExtension, RuleContext,
    directive,
};
use crate::config::HostConfig;
use serde::Deserialize;
use std::sync::Arc;

/// Admit AJAX requests, otherwise continue to the configured navigation fallback.
/// This is a route-entry condition, not an HTTP error response middleware.
#[derive(Clone, Copy, Debug)]
pub struct AjaxOnly;

#[derive(Deserialize)]
#[serde(transparent)]
struct AjaxOnlyConfig(bool);

pub struct AjaxCondition(bool);

impl RouteCondition for AjaxCondition {
    fn accepts(
        &self,
        request: &RuleContext<'_>,
    ) -> bool {
        !self.0
            || request.headers.get("x-requested-with").is_some_and(|value| {
                value.as_bytes().eq_ignore_ascii_case(b"XMLHttpRequest")
            })
    }
}

impl RouteExtension<HostConfig> for AjaxOnly {
    fn name(&self) -> &'static str {
        "ajax_only"
    }

    fn resolve(
        &self,
        context: &RouteContext<'_>,
        source: &serde_yaml::Value,
    ) -> claviron_runtime::registry::Result<RouteEffect> {
        directive::<AjaxOnlyConfig, _>(self.name(), |_, config| {
            Ok(RouteEffect::Condition(Arc::new(AjaxCondition(config.0))))
        })
        .resolve(context, source)
    }
}

#[cfg(test)]
#[path = "../../../../tests/extensions.rs"]
mod tests;
