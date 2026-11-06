//! Who answers a listener that selects no host.

use std::future::Future;
use std::sync::Arc;

use claviron_http::ext::{ConnCtx, RequestArrival};
use claviron_http::{OtherHandler, RouteBody, RouteResponse};
use claviron_pipeline::HostContext;
use claviron_pipeline::plan::HostExecution;
use http::Request;

/// The one owner of every request a `Single` listener accepts: either a
/// [`Handler`] or a site compiled by `claviron_pipeline::compile`.
pub trait Owner: Send + Sync + 'static {
    /// Answer one request. `connection` is absent where the connection has no
    /// addresses to describe it. The returned future owns what it needs.
    fn call(
        &self,
        request: Request<RouteBody>,
        connection: Option<&Arc<ConnCtx>>,
        arrival: RequestArrival,
    ) -> impl Future<Output = RouteResponse> + Send + use<Self>;
}

/// A handler that receives the request as the connection produced it, with
/// only its body erased and nothing else attached.
pub struct Handler<H>(pub H);

impl<H: OtherHandler> Owner for Handler<H> {
    fn call(
        &self,
        request: Request<RouteBody>,
        _connection: Option<&Arc<ConnCtx>>,
        _arrival: RequestArrival,
    ) -> impl Future<Output = RouteResponse> + Send + use<H> {
        let answering = self.0.call(request);
        async move {
            match answering.await {
                Ok(response) => response,
                Err(never) => match never {},
            }
        }
    }
}

/// A site compiled by `claviron_pipeline::compile`: its policies, files and
/// backends run on every request, with the connection's facts lent to them.
impl Owner for HostExecution {
    fn call(
        &self,
        request: Request<RouteBody>,
        connection: Option<&Arc<ConnCtx>>,
        arrival: RequestArrival,
    ) -> impl Future<Output = RouteResponse> + Send + use<> {
        let execution = self.clone();
        let connection = connection.cloned();
        async move {
            let context = connection
                .as_ref()
                .map(|connection| HostContext::new(connection, arrival));
            match execution.handle_in(request, context).await {
                Ok(response) => response,
                Err(never) => match never {},
            }
        }
    }
}
