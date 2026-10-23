//! `ResourceAdapter` — file-first wrapper around a user-registered handler.
//!
//! Probes `claviron-files` first (statics / try_files / SPA);
//! on `Unhandled` falls through to the registered handler. HostService has
//! already applied ACL, fixed-response and redirect policy before this adapter. The
//! match runs HERE, not in the user's handler — the handler stays
//! policy-free, only sees requests that the filesystem couldn't claim.
//!
//! Handlers registered via this adapter are **GET-style only**: they
//! receive a [`RequestContext`] (uri + headers + real_ip) — the body is
//! deliberately not exposed because this adapter is intended for
//! resource lookup (db-backed assets, computed responses, redirects).
//! The adapter itself takes the bypass-pipeline body type
//! (`Request<RouteBody>`) for uniform dispatch alongside FCGI / SAPI /
//! Pusher, then drops the body before invoking the handler.
//!
//! For body-aware handlers (POST/PUT bodies parsed in user code), use
//! `register_app_router` (axum pipeline) instead.

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use claviron_http::ext::RealIpExt;
use claviron_http::{
    RequestContext, RouteBody, RouteResponse, ServiceResult, err_status,
};
use claviron_runtime::AppState;
use http::{Request, StatusCode};

use crate::site::FileSite;

/// User-implementable handler trait — invoked when the file probe returns
/// `Unhandled`. Receives a view of the request (uri + headers + real_ip);
/// the body is intentionally not exposed because this trait is for
/// **GET-style resource lookup** (db-backed assets, computed responses,
/// redirects from a router table, etc.).
///
/// For POST/PUT handlers that need body parsing, use `register_app_router`
/// (axum pipeline) instead.
#[async_trait]
pub trait Handler: Send + Sync + 'static {
    async fn handle(
        &self,
        state: &AppState,
        site: &dyn FileSite,
        ctx: RequestContext<'_>,
    ) -> Result<RouteResponse, StatusCode>;
}

/// Cheap-clone alias used by `ResourceAdapter`. `Arc<dyn Handler>` lets
/// the service derive `Clone` and pass the handler through transports
/// without boxing per-request.
pub type SharedHandler = Arc<dyn Handler>;

#[derive(Clone)]
pub struct ResourceAdapter {
    handler: SharedHandler,
    state: AppState,
    site: Arc<dyn FileSite>,
}

impl ResourceAdapter {
    pub fn new(
        handler: SharedHandler,
        state: AppState,
        site: Arc<dyn FileSite>,
    ) -> Self {
        Self { handler, state, site }
    }

    pub fn from_arc<H>(
        handler: Arc<H>,
        state: AppState,
        site: Arc<dyn FileSite>,
    ) -> Self
    where
        H: Handler + 'static,
    {
        Self::new(handler, state, site)
    }

    pub fn from_handler<H>(
        handler: H,
        state: AppState,
        site: Arc<dyn FileSite>,
    ) -> Self
    where
        H: Handler + 'static,
    {
        Self::new(Arc::new(handler), state, site)
    }

    /// Build a `ResourceAdapter` from a plain async fn — the most common
    /// case for app-level CDN handlers that just want to read uri/headers
    /// and return a response. Wraps the fn in an internal `Handler` impl
    /// so callers don't have to write the trait by hand.
    ///
    /// Signature shape:
    /// ```ignore
    /// async fn my_handler(
    ///     state: &AppState,
    ///     site: &dyn FileSite,
    ///     ctx: RequestContext<'_>,
    /// ) -> Result<RouteResponse, StatusCode>;
    /// ```
    pub fn from_fn<F>(
        f: F,
        state: AppState,
        site: Arc<dyn FileSite>,
    ) -> Self
    where
        F: for<'a> AsyncResourceFn<'a> + Send + Sync + 'static,
    {
        Self::new(Arc::new(FnHandler(f)), state, site)
    }

    pub async fn handle(
        &self,
        req: Request<RouteBody>,
    ) -> Result<RouteResponse, Infallible> {
        self.handle_with_site(req, self.site.as_ref()).await
    }

    /// Use the file policy already selected by the caller's route dispatch.
    /// The fallback handler and runtime stay shared; no adapter is allocated
    /// and no second route selection is performed for this request.
    pub async fn handle_with_site(
        &self,
        req: Request<RouteBody>,
        site: &dyn FileSite,
    ) -> Result<RouteResponse, Infallible> {
        let site = site.route_for(req.uri().path()).unwrap_or(site);
        // File-first probe. Disk-served `statics:` / `try_files:` /
        // SPA hits return `Handled` and skip the
        // handler entirely; only `Unhandled` paths fall through.
        let real_ip = req.extensions().get::<RealIpExt>().map(|e| e.0);
        let ctx =
            RequestContext { uri: req.uri(), headers: req.headers(), real_ip };

        match crate::try_handle_files(&self.state, site, ctx).await {
            Ok(ServiceResult::Handled(resp)) => Ok(resp),
            Ok(ServiceResult::Unhandled(_)) => {
                // File didn't claim the request; delegate to the user
                // handler with a request view (body is dropped — this
                // trait is GET-style only).
                let real_ip = req.extensions().get::<RealIpExt>().map(|e| e.0);
                let ctx = RequestContext {
                    uri: req.uri(),
                    headers: req.headers(),
                    real_ip,
                };
                match self.handler.handle(&self.state, site, ctx).await {
                    Ok(resp) => Ok(resp),
                    Err(status) => Ok(err_status(status)),
                }
            }
            Err(status) => Ok(err_status(status)),
        }
    }
}

/// Helper trait so [`ResourceAdapter::from_fn`] can accept any fn-like
/// thing whose async return future borrows from `RequestContext<'a>`
/// without writing out the higher-ranked trait bound at every call site.
pub trait AsyncResourceFn<'a>: Send + Sync {
    fn call(
        &'a self,
        state: &'a AppState,
        site: &'a dyn FileSite,
        ctx: RequestContext<'a>,
    ) -> Pin<
        Box<dyn Future<Output = Result<RouteResponse, StatusCode>> + Send + 'a>,
    >;
}

impl<'a, F, Fut> AsyncResourceFn<'a> for F
where
    F: Fn(&'a AppState, &'a dyn FileSite, RequestContext<'a>) -> Fut
        + Send
        + Sync,
    Fut: Future<Output = Result<RouteResponse, StatusCode>> + Send + 'a,
{
    fn call(
        &'a self,
        state: &'a AppState,
        site: &'a dyn FileSite,
        ctx: RequestContext<'a>,
    ) -> Pin<
        Box<dyn Future<Output = Result<RouteResponse, StatusCode>> + Send + 'a>,
    > {
        Box::pin((self)(state, site, ctx))
    }
}

struct FnHandler<F>(F);

#[async_trait]
impl<F> Handler for FnHandler<F>
where
    F: for<'a> AsyncResourceFn<'a> + Send + Sync + 'static,
{
    async fn handle(
        &self,
        state: &AppState,
        site: &dyn FileSite,
        ctx: RequestContext<'_>,
    ) -> Result<RouteResponse, StatusCode> {
        self.0.call(state, site, ctx).await
    }
}
