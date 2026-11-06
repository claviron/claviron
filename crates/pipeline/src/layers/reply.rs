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
    bounds: Bounds,
}

#[derive(Clone, Copy, Debug)]
enum Bounds {
    Size(usize),
    Idle(Duration),
    Both(usize, Duration),
}

impl ReplyPolicy {
    /// Select active body guarding once, after listener/host narrowing.
    pub fn new(policy: ResponsePolicy) -> Option<Self> {
        let bounds = match (policy.max_body_size, policy.body_idle_timeout) {
            (None, None) => return None,
            (Some(size), None) => Bounds::Size(size),
            (None, Some(timeout)) => Bounds::Idle(timeout),
            (Some(size), Some(timeout)) => Bounds::Both(size, timeout),
        };
        Some(Self { bounds })
    }

    /// Retain the response head and transfer its body into HTTP's guard.
    /// This neither polls nor collects the stream and starts no timer here.
    pub fn finish(
        &self,
        response: RouteResponse,
    ) -> RouteResponse {
        response.map(|body| self.guard(body))
    }

    fn guard(
        &self,
        body: RouteBody,
    ) -> RouteBody {
        use claviron_http::body::{
            guard_idle, guard_size, guard_size_and_idle,
        };
        match self.bounds {
            Bounds::Size(size) => guard_size(body, size),
            Bounds::Idle(timeout) => guard_idle(body, timeout),
            Bounds::Both(size, timeout) => {
                guard_size_and_idle(body, size, timeout)
            }
        }
    }
}

impl<S> HostLayer<S> for ReplyPolicy {
    fn prepare(
        self,
        layer: &mut HostLayerBuilder<S>,
    ) -> crate::Result<()> {
        use claviron_http::body::{
            guard_idle, guard_size, guard_size_and_idle,
        };
        match self.bounds {
            Bounds::Size(size) => {
                layer.body(move |body, _| guard_size(body, size));
            }
            Bounds::Idle(timeout) => {
                layer.body(move |body, _| guard_idle(body, timeout));
            }
            Bounds::Both(size, timeout) => {
                layer.body(move |body, _| {
                    guard_size_and_idle(body, size, timeout)
                });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../tests/layers/reply.rs"]
mod tests;
