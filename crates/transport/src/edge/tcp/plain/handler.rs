//! Cleartext authority selection and handoff to the selected host.

use std::cell::RefCell;
use std::convert::Infallible;
use std::future::{Future, ready};
use std::sync::Arc;

use claviron_edge::transport::{CleartextHostCache, CleartextRequest};
use claviron_http::ext::ConnCtx;
use claviron_http::session::SessionSpawner;
use claviron_http::{RouteResponse, ServiceResult, err_status};
use futures_util::future::Either;
use hyper::{Request, StatusCode};

use super::server::ServerSnapshot;

pub(super) struct H1Service {
    snapshot: Arc<ServerSnapshot>,
    host_cache: RefCell<CleartextHostCache>,
    sessions: SessionSpawner,
}

impl H1Service {
    pub(super) fn new(
        snapshot: Arc<ServerSnapshot>,
        ctx: Arc<ConnCtx>,
        sessions: SessionSpawner,
    ) -> Self {
        let host_cache =
            CleartextHostCache::new(ctx, snapshot.listener.policy());
        Self { snapshot, host_cache: RefCell::new(host_cache), sessions }
    }

    pub(super) fn call<B>(
        &self,
        request: Request<B>,
    ) -> impl Future<Output = Result<RouteResponse, Infallible>> + Send + use<B>
    where
        B: http_body::Body<Data = bytes::Bytes> + Send + 'static,
        B::Error: std::error::Error + Send + Sync + 'static,
    {
        match self.prepare(request) {
            Err(status) => Either::Left(ready(Ok(err_status(status)))),
            Ok(answering) => Either::Right(answering),
        }
    }
}

impl crate::tcp::plain::Requests for H1Service {
    fn call(
        &self,
        mut request: Request<hyper::body::Incoming>,
    ) -> impl Future<Output = Result<RouteResponse, Infallible>> + Send + use<>
    {
        crate::tcp::attach_sessions(&mut request, &self.sessions);
        H1Service::call(self, request)
    }
}

impl H1Service {
    fn prepare<B>(
        &self,
        request: Request<B>,
    ) -> Result<
        impl Future<Output = Result<RouteResponse, Infallible>> + Send + use<B>,
        StatusCode,
    >
    where
        B: http_body::Body<Data = bytes::Bytes> + Send + 'static,
        B::Error: std::error::Error + Send + Sync + 'static,
    {
        let table = &self.snapshot.hosts;
        let mut cache = self.host_cache.borrow_mut();
        // A request that repeats the last admitted authority needs neither
        // validation nor host selection again.
        let request = match cache.repeated(request, table) {
            ServiceResult::Handled(prepared) => {
                drop(cache);
                return Ok(Either::Left(crate::edge::tcp::service::dispatch(
                    prepared,
                )));
            }
            ServiceResult::Unhandled(request) => request,
        };
        let request = CleartextRequest::new(request)
            .map_err(|_| StatusCode::BAD_REQUEST)?;
        let Some(mut host) = cache.resolve(request.authority(), table) else {
            return Err(StatusCode::NOT_FOUND);
        };
        let Some(app) = host.application.take() else {
            let connection = cache.connection().clone();
            drop(cache);
            let snapshot = self.snapshot.clone();
            let (request, _) = request.into_parts();
            return Ok(Either::Right(async move {
                Ok(snapshot
                    .non_serving
                    .call(request, host, connection, snapshot.listener.policy())
                    .await)
            }));
        };
        let host = cache.bind(app, host);
        cache.admit(request.request(), table, &host);
        drop(cache);
        Ok(Either::Left(crate::edge::tcp::service::dispatch(
            host.request_cleartext(request),
        )))
    }
}
