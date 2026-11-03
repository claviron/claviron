//! Optional listener entry before host lookup; installed once per connection.

use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;

use claviron_http::{RouteResponse, err_status};
use futures_util::future::Either;
use http::{Request, StatusCode};

use super::handler::H1Service;
use crate::pipeline::layers::Http01Layer;
use crate::service::CleartextRequest;

pub(super) struct Http01Service {
    next: H1Service,
    layer: Arc<Http01Layer>,
}

impl Http01Service {
    pub(super) fn new(
        next: H1Service,
        layer: Arc<Http01Layer>,
    ) -> Self {
        Self { next, layer }
    }

    pub(super) fn call<B>(
        &self,
        request: Request<B>,
    ) -> impl Future<Output = Result<RouteResponse, Infallible>> + Send + use<B>
    where
        B: http_body::Body<Data = bytes::Bytes> + Send + 'static,
        B::Error: std::error::Error + Send + Sync + 'static,
    {
        let Some(reply) = self.layer.select(request.method(), request.uri())
        else {
            return Either::Left(self.next.call(request));
        };
        let valid = CleartextRequest::new(request).is_ok();
        Either::Right(async move {
            Ok(if valid {
                reply.respond().await
            } else {
                err_status(StatusCode::BAD_REQUEST)
            })
        })
    }
}

#[cfg(test)]
#[path = "../../../tests/tcp/unit/http01.rs"]
mod tests;
