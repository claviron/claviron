//! Cleartext host admission with connection-owned native dispatch.

use std::cell::RefCell;
use std::convert::Infallible;
use std::future::{Future, ready};
#[cfg(feature = "acme")]
use std::path::PathBuf;
use std::sync::Arc;

use crate::config::NonSslPolicy;
use crate::service::{CleartextHostCache, CleartextRequest};
#[cfg(feature = "acme")]
use crate::transport::RequestAuthority;
use crate::transport::VhostResolver;
use claviron_http::ext::ConnCtx;
use claviron_http::listener::HttpProtocol;
use claviron_http::{RouteResponse, err_status};
use futures_util::FutureExt;
use futures_util::future::Either;
use hyper::body::Incoming;
use hyper::{Request, StatusCode};
use tracing::warn;

#[cfg(feature = "acme")]
use super::acme;
use super::error::Result;
use super::redirect::handle_redirect;
use super::server::ServerSnapshot;

pub(super) struct H1Service {
    snapshot: Arc<ServerSnapshot>,
    vhosts: Arc<VhostResolver>,
    host_cache: RefCell<CleartextHostCache>,
}

enum ResponseSource<F> {
    Ready(RouteResponse),
    Application(F),
    #[cfg(feature = "acme")]
    Challenge {
        request: Request<Incoming>,
        directory: Arc<PathBuf>,
    },
}

impl H1Service {
    pub(super) fn new(
        snapshot: Arc<ServerSnapshot>,
        vhosts: Arc<VhostResolver>,
        ctx: Arc<ConnCtx>,
    ) -> Self {
        let host_cache = CleartextHostCache::new(
            ctx,
            vhosts.clone(),
            snapshot.listener.policy(),
        );
        Self {
            snapshot,
            vhosts,
            host_cache: RefCell::new(host_cache),
        }
    }

    pub(super) fn call(
        &self,
        request: Request<Incoming>,
    ) -> impl Future<Output = Result<RouteResponse>> + Send + use<> {
        match self.prepare(request) {
            Err(error) => Either::Left(ready(Err(error))),
            Ok(ResponseSource::Ready(response)) => {
                Either::Left(ready(Ok(response)))
            }
            Ok(ResponseSource::Application(answering)) => {
                let answering = answering.map(|Ok(response)| Ok(response));
                #[cfg(feature = "acme")]
                {
                    Either::Right(Either::Left(answering))
                }
                #[cfg(not(feature = "acme"))]
                {
                    Either::Right(answering)
                }
            }
            #[cfg(feature = "acme")]
            Ok(ResponseSource::Challenge { request, directory }) => {
                Either::Right(Either::Right(async move {
                    Ok(acme::serve_acme_challenge(&request, &directory)
                        .await
                        .map(claviron_http::boxed_body))
                }))
            }
        }
    }

    fn prepare(
        &self,
        req: Request<Incoming>,
    ) -> Result<
        ResponseSource<
            impl Future<Output = std::result::Result<RouteResponse, Infallible>>
            + Send
            + use<>,
        >,
    > {
        let snapshot = &self.snapshot;
        // A configured HTTP-01 handler short-circuits ordinary host selection.
        #[cfg(feature = "acme")]
        if let Some(challenges_dir) = &snapshot.http01_directory
            && req.uri().path().starts_with(acme::ACME_PREFIX)
        {
            if RequestAuthority::from_cleartext(&req).is_err() {
                return Ok(ResponseSource::Ready(err_status(
                    StatusCode::BAD_REQUEST,
                )));
            }
            return Ok(ResponseSource::Challenge {
                request: req,
                directory: challenges_dir.clone(),
            });
        }
        #[cfg(not(feature = "acme"))]
        let _ = &snapshot.http01_directory;
        let req = match CleartextRequest::new(req) {
            Ok(request) => request,
            Err(_) => {
                return Ok(ResponseSource::Ready(err_status(
                    StatusCode::BAD_REQUEST,
                )));
            }
        };
        let authority = req.authority();
        let table = self.vhosts.snapshot();
        let mut host_cache = self.host_cache.borrow_mut();
        let Some(mut vctx) = host_cache.resolve(authority, &table) else {
            return Ok(ResponseSource::Ready(err_status(
                StatusCode::NOT_FOUND,
            )));
        };
        let host = &vctx.vhost_config;
        if !host.allows_transport(&snapshot.listener.id, HttpProtocol::H1) {
            return Ok(ResponseSource::Ready(err_status(
                StatusCode::MISDIRECTED_REQUEST,
            )));
        }
        match host.transport.non_ssl {
            NonSslPolicy::Reject => {
                Ok(ResponseSource::Ready(err_status(StatusCode::FORBIDDEN)))
            }
            NonSslPolicy::Redirect => handle_redirect(
                authority,
                req.request().uri(),
                snapshot.redirect_port,
            )
            .map(ResponseSource::Ready),
            NonSslPolicy::Serve => {
                // Cleartext cannot satisfy a TLS client certificate policy.
                if host.tls.client_auth.is_required() {
                    return Ok(ResponseSource::Ready(err_status(
                        StatusCode::FORBIDDEN,
                    )));
                }
                let Some(app) = vctx.application.take() else {
                    warn!(listener = %snapshot.listener.id, host = %authority.authority(), "H1 host application unavailable");
                    return Ok(ResponseSource::Ready(err_status(
                        StatusCode::SERVICE_UNAVAILABLE,
                    )));
                };
                let host = host_cache.bind(app, vctx);
                drop(host_cache);
                let response =
                    crate::tcp::service::dispatch(host.prepare_cleartext(req));
                Ok(ResponseSource::Application(response))
            }
        }
    }
}
