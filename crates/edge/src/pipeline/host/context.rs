use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use claviron_http::{
    RouteBody, RouteFuture, RouteResponse,
    ext::{ConnCtx, HostKey, HostName},
    policy::HttpPolicy,
};
use http::Request;

use crate::service::RequestArrival;

/// Accepted transport facts borrowed for one pipeline execution, and, once a
/// backend is reached, the effective policy of the scope that selected it.
///
/// Nothing here is written into the request. An owner that keeps a value
/// beyond its call takes it before it returns.
#[derive(Clone, Copy)]
pub struct HostContext<'a> {
    pub connection: &'a Arc<ConnCtx>,
    pub arrival: RequestArrival,
    pub(crate) host: Option<(&'a HostName, &'a HostKey)>,
    pub(crate) policy: Option<&'a HttpPolicy>,
}

impl<'a> HostContext<'a> {
    pub fn new(
        connection: &'a Arc<ConnCtx>,
        arrival: RequestArrival,
    ) -> Self {
        Self { connection, arrival, host: None, policy: None }
    }

    pub fn with_host(
        mut self,
        hostname: &'a HostName,
        key: &'a HostKey,
    ) -> Self {
        self.host = Some((hostname, key));
        self
    }

    /// Bind the policy of the scope whose backend is about to be called.
    pub(in crate::pipeline) fn with_policy(
        mut self,
        policy: &'a HttpPolicy,
    ) -> Self {
        self.policy = Some(policy);
        self
    }

    /// The host name the request was accepted for, when one was selected.
    pub fn hostname(&self) -> Option<&'a HostName> {
        self.host.map(|(hostname, _)| hostname)
    }

    /// The effective policy of the backend's scope. Absent before a backend
    /// is reached.
    pub fn policy(&self) -> Option<&'a HttpPolicy> {
        self.policy
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
