use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use claviron_http::{
    RouteBody, RouteFuture, RouteResponse,
    ext::{ConnCtx, HostKey, HostName},
};
use http::Request;

use crate::service::RequestArrival;

/// Accepted transport facts borrowed for one pipeline execution.
#[derive(Clone, Copy)]
pub struct HostContext<'a> {
    pub connection: &'a Arc<ConnCtx>,
    pub arrival: RequestArrival,
    pub(crate) host: Option<(&'a HostName, &'a HostKey)>,
}

impl<'a> HostContext<'a> {
    pub fn new(
        connection: &'a Arc<ConnCtx>,
        arrival: RequestArrival,
    ) -> Self {
        Self { connection, arrival, host: None }
    }

    pub fn with_host(
        mut self,
        hostname: &'a HostName,
        key: &'a HostKey,
    ) -> Self {
        self.host = Some((hostname, key));
        self
    }
}

pub type ContextFuture<'a> = Pin<
    Box<dyn Future<Output = Result<RouteResponse, Infallible>> + Send + 'a>,
>;

/// A prepared owner that can borrow accepted facts without cloning their Arc.
pub trait ContextHandler: Send + Sync + 'static {
    fn call(
        self: std::sync::Arc<Self>,
        request: Request<RouteBody>,
    ) -> RouteFuture;

    fn times_its_head(&self) -> bool {
        false
    }

    fn call_in<'a>(
        &'a self,
        request: Request<RouteBody>,
        context: Option<HostContext<'a>>,
    ) -> ContextFuture<'a>;
}
