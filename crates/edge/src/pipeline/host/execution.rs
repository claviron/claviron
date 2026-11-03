use std::sync::Arc;

use claviron_http::{RouteBody, RouteFuture};
use http::Request;

use super::{
    Backend, ContextFuture, ContextHandler, HostContext, HostPipeline,
};
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
    #[cfg(feature = "axum")]
    Axum(Arc<Backend<axum::Router>>),
    Router(Arc<Backend<RustRouter>>),
    Pipeline(Arc<dyn ContextHandler>),
}

impl HostExecution {
    pub(in crate::pipeline) fn router(
        router: RustRouter,
        policy: claviron_http::policy::HttpPolicy,
    ) -> Self {
        let inner = match router {
            #[cfg(feature = "axum")]
            RustRouter::Axum(owner) => {
                Execution::Axum(Arc::new(Backend::new(owner, policy)))
            }
            owner => Execution::Router(Arc::new(Backend::new(owner, policy))),
        };
        Self { inner }
    }

    pub(crate) fn kind(&self) -> &'static str {
        match &self.inner {
            #[cfg(feature = "axum")]
            Execution::Axum(_) => "axum",
            Execution::Router(router) => router.owner.kind(),
            Execution::Pipeline(_) => "pipeline",
        }
    }

    #[cfg(feature = "axum")]
    pub(crate) fn into_axum(self) -> Result<Arc<Backend<axum::Router>>, Self> {
        match self.inner {
            Execution::Axum(owner) => Ok(owner),
            _ => Err(self),
        }
    }

    pub fn handle(
        &self,
        request: Request<RouteBody>,
    ) -> RouteFuture {
        match &self.inner {
            #[cfg(feature = "axum")]
            Execution::Axum(router) => call_axum(router, request, None),
            Execution::Router(router) => {
                router.owner.handle(router.prepare_request(request, None))
            }
            Execution::Pipeline(pipeline) => pipeline.clone().call(request),
        }
    }

    pub fn handle_in<'a>(
        &'a self,
        request: Request<RouteBody>,
        context: Option<HostContext<'a>>,
    ) -> ContextFuture<'a> {
        match &self.inner {
            #[cfg(feature = "axum")]
            Execution::Axum(router) => call_axum(router, request, context),
            Execution::Router(router) => {
                router.owner.handle(router.prepare_request(request, context))
            }
            Execution::Pipeline(pipeline) => pipeline.call_in(request, context),
        }
    }
}

#[cfg(feature = "axum")]
fn call_axum(
    backend: &Backend<axum::Router>,
    request: Request<RouteBody>,
    context: Option<HostContext<'_>>,
) -> RouteFuture {
    use futures_util::FutureExt;
    let request = backend.prepare_request(request, context);
    Box::pin(
        super::ApplicationHandler::call(backend.owner.clone(), request).map(Ok),
    )
}

impl<S: Default + Send + 'static> HostPipeline<S> {
    /// Retain this completed plan as the transport-independent host owner.
    pub fn into_execution(self) -> HostExecution {
        HostExecution { inner: Execution::Pipeline(Arc::new(self)) }
    }
}
