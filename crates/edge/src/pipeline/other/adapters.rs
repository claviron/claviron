//! Backend-specific futures are erased at their own request boundary.

use claviron_http::{RouteBody, RouteFuture};
use http::Request;

use super::OtherHandler;

#[cfg(feature = "php_fcgi")]
impl OtherHandler for claviron_php_fcgi::PhpFpmAdapter {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> RouteFuture {
        let adapter = self.clone();
        Box::pin(async move { adapter.handle(request).await })
    }
}

#[cfg(feature = "php_embed")]
impl OtherHandler for claviron_php_embed::PhpScriptAdapter {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> RouteFuture {
        let adapter = self.clone();
        Box::pin(async move { adapter.handle(request).await })
    }
}

#[cfg(feature = "php_embed")]
impl OtherHandler for claviron_php_embed::PhpApplicationAdapter {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> RouteFuture {
        let adapter = self.clone();
        Box::pin(async move { adapter.handle(request).await })
    }
}

#[cfg(feature = "broadcast")]
impl OtherHandler for claviron_pusher::PusherAdapter {
    fn call(
        &self,
        request: Request<RouteBody>,
    ) -> RouteFuture {
        let adapter = self.clone();
        Box::pin(async move { adapter.handle(request).await })
    }
}
