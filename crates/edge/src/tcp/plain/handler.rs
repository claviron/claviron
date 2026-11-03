//! Cleartext authority selection and handoff to the selected host.

use std::cell::RefCell;
use std::convert::Infallible;
use std::future::{Future, ready};
use std::sync::Arc;

use crate::service::{CleartextHostCache, CleartextRequest};
use crate::transport::VhostResolver;
use claviron_http::ext::ConnCtx;
use claviron_http::{RouteResponse, err_status};
use futures_util::future::Either;
use hyper::{Request, StatusCode};
use tracing::warn;

use super::server::ServerSnapshot;

pub(super) struct H1Service {
    vhosts: Arc<VhostResolver>,
    host_cache: RefCell<CleartextHostCache>,
}

impl H1Service {
    pub(super) fn new(
        snapshot: Arc<ServerSnapshot>,
        vhosts: Arc<VhostResolver>,
        ctx: Arc<ConnCtx>,
    ) -> Self {
        let host_cache =
            CleartextHostCache::new(ctx, snapshot.listener.policy());
        Self { vhosts, host_cache: RefCell::new(host_cache) }
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
        let table = self.vhosts.snapshot();
        let mut cache = self.host_cache.borrow_mut();
        let Some(mut host) = cache.resolve(request.authority(), &table) else {
            return Err(StatusCode::NOT_FOUND);
        };
        let Some(app) = host.application.take() else {
            warn!(host = %request.authority().authority(), "H1 host application unavailable");
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        };
        let host = cache.bind(app, host);
        drop(cache);
        Ok(crate::tcp::service::dispatch(host.request_cleartext(request)))
    }
}
