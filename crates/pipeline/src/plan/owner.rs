//! Native route policies surrounding the selected application and backend.

use std::sync::Arc;

use claviron_http::{RouteBody, RouteFuture, RouteResponse, ServiceResult};
use http::Request;

use super::{ContextFuture, ContextHandler, HostContext, HostPipeline};

/// The production execution owner selects exchange support at preparation.
pub(super) struct CompiledPipeline<S, const EXCHANGE: bool>(
    pub HostPipeline<S>,
);

impl<S: Default + Send + 'static, const EXCHANGE: bool> ContextHandler
    for CompiledPipeline<S, EXCHANGE>
{
    fn call(
        self: Arc<Self>,
        request: Request<RouteBody>,
    ) -> RouteFuture {
        Box::pin(async move { self.0.run::<EXCHANGE>(request, None).await })
    }

    fn call_in<'a>(
        &'a self,
        request: Request<RouteBody>,
        context: Option<HostContext<'a>>,
    ) -> ContextFuture<'a> {
        Box::pin(self.0.run::<EXCHANGE>(request, context))
    }
}

impl<S: Default + Send + 'static> ContextHandler for HostPipeline<S> {
    fn call(
        self: Arc<Self>,
        request: Request<RouteBody>,
    ) -> RouteFuture {
        Box::pin(async move {
            if self.has_exchange {
                self.run::<true>(request, None).await
            } else {
                self.run::<false>(request, None).await
            }
        })
    }

    fn call_in<'a>(
        &'a self,
        request: Request<RouteBody>,
        context: Option<HostContext<'a>>,
    ) -> ContextFuture<'a> {
        if self.has_exchange {
            Box::pin(self.run::<true>(request, context))
        } else {
            Box::pin(self.run::<false>(request, context))
        }
    }
}

impl<S: Default + Send + 'static> HostPipeline<S> {
    async fn run<const EXCHANGE: bool>(
        &self,
        request: Request<RouteBody>,
        context: Option<HostContext<'_>>,
    ) -> Result<RouteResponse, std::convert::Infallible> {
        Ok(match self.prepare_in(request, S::default(), context) {
            ServiceResult::Handled(reply) => reply.finish(),
            ServiceResult::Unhandled(request) => {
                request
                    .dispatch_plan::<EXCHANGE, _, _>(|_| async {
                        claviron_http::err_status(http::StatusCode::NOT_FOUND)
                    })
                    .await
            }
        })
    }
}
