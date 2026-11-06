//! Optional URI deny matching. An empty matcher produces no layer.

use super::*;
use crate::response::RequestRejection;
use regex::RegexSet;

#[derive(Clone)]
pub struct DenyLayer {
    matcher: RegexSet,
}

impl DenyLayer {
    pub fn from_matcher(matcher: &RegexSet) -> Option<Self> {
        (!matcher.is_empty()).then(|| Self { matcher: matcher.clone() })
    }

    pub fn new(matcher: RegexSet) -> Option<Self> {
        if matcher.is_empty() {
            return None;
        }
        Some(Self { matcher })
    }
}

impl<S, R: From<RequestRejection>> HostLayer<S, R> for DenyLayer {
    fn prepare(
        self,
        layer: &mut HostLayerBuilder<S, R>,
    ) -> crate::Result<()> {
        layer.before(move |head, _| {
            if self.matcher.is_match(head.uri.path()) {
                ControlFlow::Break(RequestRejection::AccessDenied.into())
            } else {
                ControlFlow::Continue(())
            }
        });
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../tests/layers/deny.rs"]
mod tests;
