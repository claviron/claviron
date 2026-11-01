//! Request body bounds are an explicit layer, not transport adapter defaults.

use std::time::Duration;
use claviron_http::body::{GuardedBody, declared_length_within};
use claviron_http::policy::RequestPolicy;
use claviron_http::ServiceResult;
use http_body::Body;
use http_body_util::BodyExt;
use super::*;

pub struct RequestBodyLayer {
    max_size: Option<usize>,
    idle: Option<Duration>,
}

impl RequestBodyLayer {
    pub fn new(policy: RequestPolicy) -> Option<Self> {
        (policy.max_body_size.is_some() || policy.body_idle_timeout.is_some())
            .then_some(Self { max_size: policy.max_body_size, idle: policy.body_idle_timeout })
    }
}

impl<S> HostLayer<S> for RequestBodyLayer {
    fn prepare(self, layer: &mut HostLayerBuilder<S>) -> crate::Result<()> {
        layer.request(move |request| {
            if declared_length_within(request.headers(), self.max_size).is_err() {
                return DispatchFuture::ready(ServiceResult::Handled(
                    claviron_http::err_status(http::StatusCode::PAYLOAD_TOO_LARGE),
                ));
            }
            if request.body().is_end_stream() {
                return DispatchFuture::ready(ServiceResult::Unhandled(request));
            }
            let request = request.map(|body| {
                GuardedBody::new(body, self.max_size, self.idle).boxed_unsync()
            });
            DispatchFuture::ready(ServiceResult::Unhandled(request))
        });
        Ok(())
    }
}
