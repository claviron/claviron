//! Cleartext authority selection and handoff to the selected host.

use std::cell::RefCell;
use std::convert::Infallible;
use std::future::{Future, ready};
use std::sync::Arc;

use crate::service::{CleartextHostCache, CleartextRequest};
use claviron_http::ext::ConnCtx;
use claviron_http::{RouteResponse, err_status};
use futures_util::future::Either;
use hyper::{Request, StatusCode};

use super::server::ServerSnapshot;

pub(super) struct H1Service {
    snapshot: Arc<ServerSnapshot>,
    host_cache: RefCell<CleartextHostCache>,
}

impl H1Service {
    pub(super) fn new(
        snapshot: Arc<ServerSnapshot>,
        ctx: Arc<ConnCtx>,
    ) -> Self {
        let host_cache =
            CleartextHostCache::new(ctx, snapshot.listener.policy());
        Self { snapshot, host_cache: RefCell::new(host_cache) }
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
        let request = CleartextRequest::new(request)
            .map_err(|_| StatusCode::BAD_REQUEST)?;
        let table = &self.snapshot.hosts;
        let mut cache = self.host_cache.borrow_mut();
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
        drop(cache);
        Ok(Either::Left(crate::tcp::service::dispatch(
            host.request_cleartext(request),
        )))
    }
}
