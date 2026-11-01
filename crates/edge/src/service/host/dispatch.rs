//! Select an application before constructing its host dispatch future.

use super::*;
use crate::pipeline::host::ApplicationHandler;
use futures_util::{FutureExt, future};
#[cfg(all(feature = "tcp", feature = "axum"))]
use claviron_http::RouteFuture;
#[cfg(feature = "axum")]
use futures_util::future::Either;
#[cfg(feature = "tcp")]
use std::convert::Infallible;
use std::future::Future;

impl PreparedRequest<RouteBody> {
    fn into_dispatch(self) -> (RustRouter, Request<RouteBody>) {
        (self.service.app, self.request)
    }

    /// Dispatch to the admitted application without allocating a host future.
    ///
    /// Each owner has its own concrete future. The unboxed sum returned here
    /// supports drivers that await inline; TCP uses `into_tcp_future` to keep
    /// its Other state independent of Axum without boxing it a second time.
    /// The selected owner receives the request only when this future is polled.
    pub fn dispatch(self) -> impl Future<Output = RouteResponse> + Send {
        let (app, request) = self.into_dispatch();
        match app {
            #[cfg(feature = "axum")]
            RustRouter::Axum(router) => Either::Left(call(router, request)),
            RustRouter::Other(service) => {
                let answering = call(service, request);
                #[cfg(feature = "axum")]
                {
                    Either::Right(answering)
                }
                #[cfg(not(feature = "axum"))]
                {
                    answering
                }
            }
        }
    }

    /// Keep Other dispatch inline through the TCP service boundary.
    /// Only Axum is erased here; its concrete state must not enlarge Other.
    #[cfg(all(feature = "tcp", feature = "axum"))]
    pub(crate) fn into_tcp_future(
        self
    ) -> Either<
        RouteFuture,
        impl Future<Output = Result<RouteResponse, Infallible>> + Send,
    > {
        let (app, request) = self.into_dispatch();
        match app {
            RustRouter::Axum(router) => {
                let answering: RouteFuture =
                    Box::pin(call(router, request).map(Ok));
                Either::Left(answering)
            }
            RustRouter::Other(service) => {
                Either::Right(call(service, request).map(Ok))
            }
        }
    }

    /// A native-only build retains the same inline transport future without
    /// compiling an Axum alternative or introducing another future box.
    #[cfg(all(feature = "tcp", not(feature = "axum")))]
    pub(crate) fn into_tcp_future(
        self
    ) -> impl Future<Output = Result<RouteResponse, Infallible>> + Send {
        let (RustRouter::Other(service), request) = self.into_dispatch();
        call(service, request).map(Ok)
    }
}

fn call<H: ApplicationHandler>(
    owner: H,
    request: Request<RouteBody>,
) -> impl Future<Output = RouteResponse> + Send {
    future::lazy(move |_| owner.call(request)).flatten()
}

#[cfg(test)]
#[path = "../../../tests/service/host/dispatch.rs"]
mod tests;
