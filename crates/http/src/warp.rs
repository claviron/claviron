//! Warp filters as an HTTP service, independent of Edge and application runners.

use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http::Request;
use http_body::{Body, Frame, SizeHint};
use pin_project_lite::pin_project;
use sync_wrapper::SyncWrapper;
use tower::Service;

use crate::RouteBodyError;

/// A prepared Warp filter, independent of host or ingress ownership.
#[derive(Clone)]
pub struct Router {
    service: tower::util::BoxCloneSyncService<
        Request<crate::RouteBody>,
        crate::RouteResponse,
        Infallible,
    >,
}

impl Router {
    pub fn handle(
        &self,
        request: Request<crate::RouteBody>,
    ) -> crate::RouteFuture {
        // The private service is Warp's always-ready filter adapter.
        self.service.clone().call(request)
    }
}

/// Prepare Warp as the explicitly named Warp choice of a RustRouter.
pub fn router<F>(filter: F) -> Router
where
    F: ::warp::Filter + Clone + Send + Sync + 'static,
    F::Extract: ::warp::Reply,
    F::Future: 'static,
{
    use tower::ServiceExt;
    Router {
        service: tower::util::BoxCloneSyncService::new(
            service(filter)
                .map_response(|response| response.map(crate::boxed_body)),
        ),
    }
}

/// Adapt Warp without its listener, body buffering or a synchronization lock.
///
/// Warp rejections remain HTTP responses. Callers own route selection and any
/// explicit fallback; a 404 does not return the original request body.
pub fn service<F, B>(
    filter: F
) -> impl Service<
    Request<B>,
    Response = ::warp::reply::Response,
    Error = Infallible,
    Future: Send + 'static,
> + Clone
+ Send
+ Sync
+ 'static
where
    F: ::warp::Filter + Clone + Send + Sync + 'static,
    F::Extract: ::warp::Reply,
    F::Future: 'static,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<RouteBodyError>,
{
    let service = ::warp::service(filter);
    tower::service_fn(move |request: Request<B>| {
        let mut service = service.clone();
        service.call(request.map(SyncBody::new))
    })
}

// Warp requires Sync even though body polling uses exclusive access. Keep
// the body exclusively owned and cache shared metadata; no mutex or extra
// body allocation is needed, including for bodies that are not Unpin.
pin_project! {
    struct SyncBody<B> {
        #[pin]
        inner: SyncWrapper<B>,
        hint: SizeHint,
        ended: bool,
    }
}

impl<B: Body> SyncBody<B> {
    fn new(body: B) -> Self {
        let hint = body.size_hint();
        let ended = body.is_end_stream();
        Self { inner: SyncWrapper::new(body), hint, ended }
    }
}

impl<B: Body> Body for SyncBody<B> {
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<B::Data>, B::Error>>> {
        let this = self.project();
        let mut body = this.inner.get_pin_mut();
        let result = body.as_mut().poll_frame(cx);
        *this.hint = body.size_hint();
        *this.ended = body.is_end_stream();
        result
    }

    fn is_end_stream(&self) -> bool {
        self.ended
    }
    fn size_hint(&self) -> SizeHint {
        self.hint
    }
}

#[cfg(test)]
#[path = "../tests/warp.rs"]
mod tests;
