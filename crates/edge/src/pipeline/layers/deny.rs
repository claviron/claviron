//! Optional URI deny matching. An empty matcher produces no layer.

use regex::RegexSet;
use crate::service::RequestRejection;
use super::*;

pub struct DenyLayer {
    matcher: RegexSet,
}

impl DenyLayer {
    pub fn new(matcher: RegexSet) -> Option<Self> {
        if matcher.is_empty() {
            return None;
        }
        Some(Self { matcher })
    }
}

impl<S, R: From<RequestRejection>> HostLayer<S, R> for DenyLayer {
    fn prepare(self, layer: &mut HostLayerBuilder<S, R>) -> crate::Result<()> {
        layer.before(move |head, _| {
            if self.matcher.is_match(head.uri.path()) {
                ControlFlow::Break(RequestRejection::AccessDenied.into())
            } else { ControlFlow::Continue(()) }
        });
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../../tests/pipeline/layers/deny.rs"]
mod tests;
