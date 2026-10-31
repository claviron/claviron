//! Select an application before constructing its host dispatch future.

use super::*;
use crate::pipeline::host::HostExecution;
#[cfg(all(feature = "tcp", feature = "axum"))]
use claviron_http::RouteFuture;
#[cfg(feature = "tcp")]
use futures_util::FutureExt;
#[cfg(feature = "axum")]
use futures_util::future::Either;
#[cfg(feature = "tcp")]
use std::convert::Infallible;
use std::future::Future;

impl PreparedRequest<RouteBody> {
    fn into_dispatch(self) -> (AppKind, HostExecution) {
        let HostApp {
            app,
            admission,
            exchange_policy,
            server_header,
            #[cfg(feature = "rate-limit")]
            quotas,
        } = self.service.app;
        (
            app,
            HostExecution {
                request: self.request,
                host: self.service.policy,
                connection: self.connection,
                access: self.access,
                policy: self.policy,
                admission,
                exchange_policy,
                server_header,
                #[cfg(feature = "rate-limit")]
                quotas,
                #[cfg(feature = "rate-limit")]
                quota_snapshot: self.quota_snapshot,
            },
        )
    }

    /// Dispatch to the admitted application without allocating a host future.
    ///
    /// Each owner has its own concrete future. The unboxed sum returned here
    /// supports drivers that await inline; TCP uses `into_tcp_future` to keep
    /// its Bypass state independent of Axum without boxing it a second time.
    /// The activity and admission guards remain owned until the reply's head,
    /// including when the caller cancels or the handler panics.
    pub fn dispatch(self) -> impl Future<Output = RouteResponse> + Send {
        let (app, request) = self.into_dispatch();
        match app {
            #[cfg(feature = "axum")]
            AppKind::Axum(router) => Either::Left(request.dispatch(router)),
            AppKind::Bypass(service) => {
                let answering = request.dispatch(service);
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

    /// Keep Bypass dispatch inline through the native TCP service boundary.
    /// Only Axum is erased here; its concrete state must not enlarge Bypass.
    #[cfg(all(feature = "tcp", feature = "axum"))]
    pub(crate) fn into_tcp_future(
        self
    ) -> Either<
        RouteFuture,
        impl Future<Output = Result<RouteResponse, Infallible>> + Send,
    > {
        let (app, request) = self.into_dispatch();
        match app {
            AppKind::Axum(router) => {
                let answering: RouteFuture =
                    Box::pin(request.dispatch(router).map(Ok));
                Either::Left(answering)
            }
            AppKind::Bypass(service) => {
                Either::Right(request.dispatch(service).map(Ok))
            }
        }
    }

    /// A native-only build retains the same inline transport future without
    /// compiling an Axum alternative or introducing another future box.
    #[cfg(all(feature = "tcp", not(feature = "axum")))]
    pub(crate) fn into_tcp_future(
        self,
    ) -> impl Future<Output = Result<RouteResponse, Infallible>> + Send {
        let (AppKind::Bypass(service), request) = self.into_dispatch();
        request.dispatch(service).map(Ok)
    }
}

#[cfg(test)]
#[path = "../../../tests/service/host/dispatch.rs"]
mod tests;
