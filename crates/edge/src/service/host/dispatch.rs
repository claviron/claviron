//! Execute a published host plan without reselecting or skipping its layers.

use super::*;
#[cfg(all(feature = "tcp", feature = "axum"))]
use claviron_http::RouteFuture;
#[cfg(feature = "axum")]
use claviron_pipeline::plan::ApplicationHandler;
#[cfg(any(feature = "tcp", feature = "axum"))]
use futures_util::FutureExt;
#[cfg(feature = "axum")]
use futures_util::future::{self, Either};
#[cfg(feature = "tcp")]
use std::convert::Infallible;
use std::future::Future;

impl PreparedRequest<RouteBody> {
    fn into_dispatch(
        self
    ) -> (HostExecution, Request<RouteBody>, Arc<HostPolicy>, RequestArrival)
    {
        (self.service.app, self.request, self.service.policy, self.arrival)
    }

    /// Dispatch to the admitted application without allocating a host future.
    ///
    /// Each owner has its own concrete future. The unboxed sum returned here
    /// supports drivers that await inline; TCP uses `into_tcp_future` to keep
    /// its host-plan state independent of Axum without boxing it a second time.
    /// The selected owner receives the request only when this future is polled.
    pub fn dispatch(self) -> impl Future<Output = RouteResponse> + Send {
        let (app, request, connection, arrival) = self.into_dispatch();
        #[cfg(feature = "axum")]
        {
            match app.into_axum() {
                Ok(router) => {
                    Either::Left(call(router, request, connection, arrival))
                }
                Err(execution) => Either::Right(call_execution(
                    execution, request, connection, arrival,
                )),
            }
        }
        #[cfg(not(feature = "axum"))]
        call_execution(app, request, connection, arrival)
    }

    /// Keep host execution inline through the TCP service boundary.
    /// A direct Axum future is erased without enlarging other host plans.
    #[cfg(all(feature = "tcp", feature = "axum"))]
    pub fn into_tcp_future(
        self
    ) -> Either<
        RouteFuture,
        impl Future<Output = Result<RouteResponse, Infallible>> + Send,
    > {
        let (app, request, connection, arrival) = self.into_dispatch();
        match app.into_axum() {
            Ok(router) => {
                let answering: RouteFuture = Box::pin(
                    call(router, request, connection, arrival).map(Ok),
                );
                Either::Left(answering)
            }
            Err(execution) => Either::Right(
                call_execution(execution, request, connection, arrival).map(Ok),
            ),
        }
    }

    /// A native-only build retains the same inline transport future without
    /// compiling an Axum alternative or introducing another future box.
    #[cfg(all(feature = "tcp", not(feature = "axum")))]
    pub fn into_tcp_future(
        self
    ) -> impl Future<Output = Result<RouteResponse, Infallible>> + Send {
        let (execution, request, connection, arrival) = self.into_dispatch();
        call_execution(execution, request, connection, arrival).map(Ok)
    }
}

async fn call_execution(
    owner: HostExecution,
    request: Request<RouteBody>,
    connection: Arc<HostPolicy>,
    arrival: RequestArrival,
) -> RouteResponse {
    match owner.handle_in(request, Some(connection.context(arrival))).await {
        Ok(response) => response,
        Err(never) => match never {},
    }
}

#[cfg(feature = "axum")]
fn call<H: ApplicationHandler + Clone + Sync + 'static>(
    backend: Arc<claviron_pipeline::plan::Backend<H>>,
    request: Request<RouteBody>,
    connection: Arc<HostPolicy>,
    arrival: RequestArrival,
) -> impl Future<Output = RouteResponse> + Send {
    future::lazy(move |_| {
        let request =
            backend.prepare_request(request, Some(connection.context(arrival)));
        backend.owner.clone().call(request)
    })
    .flatten()
}
