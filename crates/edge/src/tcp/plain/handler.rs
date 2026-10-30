//! Cleartext host admission with connection-owned native dispatch.

use std::cell::RefCell;
use std::future::Future;
#[cfg(feature = "acme")]
use std::path::PathBuf;
use std::sync::Arc;

use crate::config::NonSslPolicy;
use crate::service::{CleartextRequest, HostIdentityCache};
#[cfg(feature = "acme")]
use crate::transport::RequestAuthority;
use crate::transport::{
    HostAppResolver, HostIngress, HostService, VhostResolver,
};
use claviron_http::ext::ConnCtx;
use claviron_http::listener::HttpProtocol;
use claviron_http::session::SessionSpawner;
use claviron_http::{RouteFuture, RouteResponse, err_status};
use claviron_runtime::AppState;
use hyper::body::Incoming;
use hyper::{Request, StatusCode};
use tracing::warn;

#[cfg(feature = "acme")]
use super::acme;
use super::error::{Error, Result};
use super::redirect::handle_redirect;
use super::server::ServerSnapshot;
use crate::tcp::service::RequestDispatch;

pub(super) struct H1Service {
    snapshot: Arc<ServerSnapshot>,
    vhosts: Arc<VhostResolver>,
    ctx: Arc<ConnCtx>,
    dispatch: RequestDispatch,
    host_identity: RefCell<HostIdentityCache>,
}

enum ResponseSource {
    Ready(RouteResponse),
    Application(RouteFuture),
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
        state: AppState,
        ctx: Arc<ConnCtx>,
        sessions: SessionSpawner,
    ) -> Self {
        Self {
            snapshot,
            vhosts,
            ctx,
            dispatch: RequestDispatch::new(
                Some(HttpProtocol::H1),
                state,
                sessions,
            ),
            host_identity: RefCell::default(),
        }
    }

    pub(super) fn call(
        &self,
        request: Request<Incoming>,
    ) -> impl Future<Output = Result<RouteResponse>> + Send + use<> {
        let source = self.prepare(request);
        async move {
            match source? {
                ResponseSource::Ready(response) => Ok(response),
                ResponseSource::Application(answering) => {
                    let Ok(response) = answering.await;
                    Ok(response)
                }
                #[cfg(feature = "acme")]
                ResponseSource::Challenge { request, directory } => {
                    Ok(acme::serve_acme_challenge(&request, &directory)
                        .await
                        .map(claviron_http::boxed_body))
                }
            }
        }
    }

    fn prepare(
        &self,
        req: Request<Incoming>,
    ) -> Result<ResponseSource> {
        let snapshot = &self.snapshot;
        // A configured HTTP-01 handler short-circuits ordinary host/deny policy.
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
        let req = snapshot
            .deny
            .check_request(req)
            .map_err(|_| Error::Rejected("deny-drop"))?;
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
        let Some(mut vctx) =
            self.host_identity.borrow_mut().resolve(authority, &table)
        else {
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
                let Some(app) = vctx.application.take().or_else(|| {
                    self.dispatch
                        .state()
                        .registry_ref()
                        .resolve::<HostAppResolver>()
                        .and_then(|resolver| resolver.resolve(&vctx.vhost_key))
                }) else {
                    warn!(listener = %snapshot.listener.id, host = %authority.authority(), "H1 host application unavailable");
                    return Ok(ResponseSource::Ready(err_status(
                        StatusCode::SERVICE_UNAVAILABLE,
                    )));
                };
                let host = HostService::new(
                    app,
                    self.ctx.clone(),
                    vctx,
                    HostIngress {
                        proxy: snapshot.listener.proxy.clone(),
                        deny: snapshot.deny.clone(),
                    },
                    self.vhosts.clone(),
                    snapshot.listener.policy(),
                );
                let response = self.dispatch.call_cleartext(host, req);
                Ok(ResponseSource::Application(response))
            }
        }
    }
}
