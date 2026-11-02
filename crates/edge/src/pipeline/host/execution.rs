use std::sync::Arc;

use claviron_http::{RouteBody, RouteFuture};
use http::Request;

use super::{ContextFuture, ContextHandler, HostContext, HostPipeline};
use crate::pipeline::RustRouter;

/// A host's prepared execution, published together with its declarations.
///
/// Router selection never bypasses configured layers. Only the host compiler
/// may retain a direct Rust call when preparation contributes no preceding work.
#[derive(Clone)]
pub struct HostExecution {
    inner: Execution,
}

#[derive(Clone)]
enum Execution {
    Router(RustRouter),
    Pipeline(Arc<dyn ContextHandler>),
}

impl HostExecution {
    pub(in crate::pipeline) fn router(router: RustRouter) -> Self {
        Self { inner: Execution::Router(router) }
    }

    pub(crate) fn kind(&self) -> &'static str {
        match &self.inner {
            Execution::Router(router) => router.kind(),
            Execution::Pipeline(_) => "pipeline",
        }
    }

    #[cfg(feature = "axum")]
    pub(crate) fn into_axum(self) -> Result<axum::Router, Self> {
        match self.inner {
            Execution::Router(RustRouter::Axum(router)) => Ok(router),
            _ => Err(self),
        }
    }

    pub fn handle(
        &self,
        request: Request<RouteBody>,
    ) -> RouteFuture {
        match &self.inner {
            Execution::Router(router) => router.handle(request),
            Execution::Pipeline(pipeline) => pipeline.clone().call(request),
        }
    }

    pub fn handle_in<'a>(
        &'a self,
        request: Request<RouteBody>,
        context: Option<HostContext<'a>>,
    ) -> ContextFuture<'a> {
        match &self.inner {
            Execution::Router(router) => router.call_in(request, context),
            Execution::Pipeline(pipeline) => pipeline.call_in(request, context),
        }
    }
}

impl<S: Default + Send + 'static> HostPipeline<S> {
    /// Retain this completed plan as the transport-independent host owner.
    pub fn into_execution(self) -> HostExecution {
        HostExecution { inner: Execution::Pipeline(Arc::new(self)) }
    }
}
