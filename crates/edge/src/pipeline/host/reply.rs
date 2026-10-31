//! Prepared response-body bounds, independent of the application owner.

use std::time::Duration;

use claviron_http::policy::ResponsePolicy;

use super::*;

/// Only the declared response-body size and inter-frame idle limits.
///
/// Preparation returns `None` when neither applies. Transport writes, response
/// buffering and the reply-head deadline keep their existing owners.
#[derive(Clone, Copy, Debug)]
pub struct ReplyPolicy {
    max_body_size: Option<usize>,
    body_idle_timeout: Option<Duration>,
}

impl ReplyPolicy {
    /// Select active body guarding once, after listener/host narrowing.
    pub fn new(policy: ResponsePolicy) -> Option<Self> {
        if policy.max_body_size.is_none() && policy.body_idle_timeout.is_none() {
            None
        } else {
            Some(Self {
                max_body_size: policy.max_body_size,
                body_idle_timeout: policy.body_idle_timeout,
            })
        }
    }

    /// Retain the response head and transfer its body into HTTP's guard.
    /// This neither polls nor collects the stream and starts no timer here.
    pub fn finish(&self, response: RouteResponse) -> RouteResponse {
        response.map(|body| self.guard(body))
    }

    fn guard(&self, body: RouteBody) -> RouteBody {
        claviron_http::body::guard_reply(
            body,
            self.max_body_size,
            self.body_idle_timeout,
        )
    }
}

impl<S> HostLayer<S> for ReplyPolicy {
    fn prepare(self, layer: &mut HostLayerBuilder<S>) -> crate::Result<()> {
        layer.body(move |body, _| self.guard(body));
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../../tests/pipeline/host/reply.rs"]
mod tests;
