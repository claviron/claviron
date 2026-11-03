//! Request-body bounds selected at boot/reload before application work.

use super::*;
use claviron_http::ServiceResult;
use claviron_http::body::{
    declared_length_within, guard_idle, guard_size, guard_size_and_idle,
};
use claviron_http::policy::RequestPolicy;
use http::HeaderMap;
use http_body::Body;
use std::time::Duration;

pub struct RequestBodyLayer {
    bounds: Bounds,
}

enum Bounds {
    Size(usize),
    Idle(Duration),
    Both(usize, Duration),
}

impl RequestBodyLayer {
    pub fn new(policy: RequestPolicy) -> Option<Self> {
        let bounds = match (policy.max_body_size, policy.body_read_timeout) {
            (None, None) => return None,
            (Some(size), None) => Bounds::Size(size),
            (None, Some(timeout)) => Bounds::Idle(timeout),
            (Some(size), Some(timeout)) => Bounds::Both(size, timeout),
        };
        Some(Self { bounds })
    }
}

impl<S> HostLayer<S> for RequestBodyLayer {
    fn prepare(
        self,
        layer: &mut HostLayerBuilder<S>,
    ) -> crate::Result<()> {
        match self.bounds {
            Bounds::Size(size) => register(
                layer,
                move |headers| {
                    declared_length_within(headers, Some(size)).is_ok()
                },
                move |body| guard_size(body, size),
            ),
            Bounds::Idle(timeout) => {
                register(layer, |_| true, move |body| guard_idle(body, timeout))
            }
            Bounds::Both(size, timeout) => register(
                layer,
                move |headers| {
                    declared_length_within(headers, Some(size)).is_ok()
                },
                move |body| guard_size_and_idle(body, size, timeout),
            ),
        }
        Ok(())
    }
}

fn register<S>(
    layer: &mut HostLayerBuilder<S>,
    accepts: impl Fn(&HeaderMap) -> bool + Send + Sync + 'static,
    guard: impl Fn(RouteBody) -> RouteBody + Send + Sync + 'static,
) {
    layer.request(move |request| {
        if !accepts(request.headers()) {
            return DispatchCall::ready(ServiceResult::Handled(
                claviron_http::err_status(http::StatusCode::PAYLOAD_TOO_LARGE),
            ));
        }
        if request.body().is_end_stream() {
            return DispatchCall::ready(ServiceResult::Unhandled(request));
        }
        DispatchCall::ready(ServiceResult::Unhandled(request.map(&guard)))
    });
}

#[cfg(test)]
#[path = "../../../tests/pipeline/layers/body.rs"]
mod tests;
