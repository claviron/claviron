//! What a cleartext request is allowed to do.
//!
//! The mirror of `secure/handler.rs`: HTTP-01 first, then deny, authority,
//! host admission and the host's non-SSL policy — reject, redirect, or serve.

use std::sync::Arc;

use claviron_edge::config::NonSslPolicy;
use claviron_edge::transport::{
    HostAppResolver, HostIngress, HostService, RequestAuthority, VhostResolver,
    resolve_request_authority,
};
use claviron_http::ext::ConnCtx;
use claviron_http::listener::HttpProtocol;
use claviron_http::session::SessionSpawner;
use claviron_http::{RouteResponse, err_status};
use claviron_runtime::AppState;
use hyper::body::Incoming;
use hyper::{Request, StatusCode};
use tracing::warn;

#[cfg(feature = "acme")]
use super::acme;
use super::error::{Error, Result};
use super::redirect::handle_redirect;
use super::server::{HttpServer, ServerSnapshot};
use crate::service::RequestService;

impl HttpServer {
    pub(super) async fn handle_request(
        req: Request<Incoming>,
        snapshot: Arc<ServerSnapshot>,
        vhosts: Arc<VhostResolver>,
        state: AppState,
        ctx: Arc<ConnCtx>,
        sessions: SessionSpawner,
    ) -> Result<RouteResponse> {
        // A configured HTTP-01 handler short-circuits ordinary host/deny policy.
        #[cfg(feature = "acme")]
        if let Some(challenges_dir) = &snapshot.http01_directory
            && req.uri().path().starts_with(acme::ACME_PREFIX)
        {
            if RequestAuthority::from_cleartext(&req).is_err() {
                return Ok(err_status(StatusCode::BAD_REQUEST));
            }
            return Ok(acme::serve_acme_challenge(&req, challenges_dir)
                .await
                .map(claviron_http::boxed_body));
        }
        #[cfg(not(feature = "acme"))]
        let _ = &snapshot.http01_directory;
        // Drop blocked traffic before resolving a host, then carry the checked
        // request so shared host admission does not run this matcher again.
        let req = snapshot
            .deny
            .check_request(req)
            .map_err(|_| Error::Rejected("deny-drop"))?;
        let authority = match RequestAuthority::from_cleartext(req.request()) {
            Ok(authority) => authority,
            Err(_) => return Ok(err_status(StatusCode::BAD_REQUEST)),
        };
        let Some(vctx) =
            resolve_request_authority(&authority, &vhosts.snapshot())
        else {
            return Ok(err_status(StatusCode::NOT_FOUND));
        };
        let host = &vctx.vhost_config;
        if !host.allows_transport(&snapshot.listener.id, HttpProtocol::H1) {
            return Ok(err_status(StatusCode::MISDIRECTED_REQUEST));
        }
        match host.transport.non_ssl {
            NonSslPolicy::Reject => Ok(err_status(StatusCode::FORBIDDEN)),
            NonSslPolicy::Redirect => handle_redirect(
                &authority,
                req.request().uri(),
                snapshot.redirect_port,
            ),
            NonSslPolicy::Serve => {
                // Cleartext cannot satisfy a TLS client certificate policy.
                if host.tls.client_auth.is_required() {
                    return Ok(err_status(StatusCode::FORBIDDEN));
                }
                let Some(app) = vctx.application.clone().or_else(|| {
                    state
                        .registry_ref()
                        .resolve::<HostAppResolver>()
                        .and_then(|resolver| resolver.resolve(&vctx.vhost_key))
                }) else {
                    warn!(listener = %snapshot.listener.id, host = %authority.authority(), "H1 host application unavailable");
                    return Ok(err_status(StatusCode::SERVICE_UNAVAILABLE));
                };
                let service = RequestService::new(
                    HostService::new(
                        app,
                        ctx,
                        vctx,
                        HostIngress {
                            proxy: snapshot.listener.proxy.clone(),
                            deny: snapshot.deny.clone(),
                        },
                        vhosts,
                        snapshot.listener.policy(),
                    ),
                    state,
                    sessions,
                );
                let Ok(response) = service.call_checked(req).await;
                Ok(response)
            }
        }
    }
}
