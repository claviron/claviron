//! Prepared application selection from a borrowed HTTP head.
//!
//! The selected values belong to the caller: native handlers, router handles,
//! or other prepared owners use the same prefix/condition decision. This module
//! does not call them, adapt bodies, parse configuration or resolve services.

use std::sync::Arc;

use crate::config::routes::RouteTable;

/// The request fields a prepared entry condition can inspect.
pub struct RuleContext<'a> {
    pub method: &'a http::Method,
    pub uri: &'a http::Uri,
    pub headers: &'a http::HeaderMap,
}

/// A prepared predicate. It must not parse configuration or resolve services.
pub trait RouteCondition: Send + Sync {
    fn accepts(
        &self,
        request: &RuleContext<'_>,
    ) -> bool;
}

/// One prepared owner and its head-only entry conditions.
pub struct ConfiguredRoute<H> {
    pub pipeline: H,
    pub conditions: Vec<Arc<dyn RouteCondition>>,
}

/// An immutable prefix table with explicit miss and rejected-entry owners.
///
/// Prepare this table at boot/reload and retain it with the application binding.
/// Selection borrows one owner; it neither clones a handle nor allocates a
/// future. A rejected selected entry does not retry a shorter prefix. Its
/// explicit `rejected` owner receives the original request at the call site.
#[derive(Clone)]
pub struct RouteDispatch<H> {
    pub entries: Arc<RouteTable<ConfiguredRoute<H>>>,
    pub fallback: H,
    pub rejected: H,
}

impl<H> RouteDispatch<H> {
    /// Select once by longest segment prefix, then check only that entry.
    /// The returned borrow belongs to the table, not to the request head.
    pub fn select(&self, request: &RuleContext<'_>) -> &H {
        match self.entries.find(request.uri.path()) {
            Some(route) => {
                if route.conditions.iter().all(|condition| condition.accepts(request)) {
                    &route.pipeline
                } else {
                    &self.rejected
                }
            }
            None => &self.fallback,
        }
    }
}

#[cfg(test)]
#[path = "../tests/dispatch.rs"]
mod tests;
