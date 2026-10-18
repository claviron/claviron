//! The HTTP/1 upgrade door to a broadcast session: an embedded edge's or a
//! runner's own listener, where the WebSocket is spoken here.

use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use claviron_http::upgrade::{
    H1UpgradeAccept, H1UpgradeContext, H1UpgradeHandler, H1UpgradeReject,
    H1UpgradeResult, H1UpgradeState, is_h1_header_upgrade,
    websocket_response_headers,
};
use claviron_runtime::{AppState, registry};
use http::Request;
use hyper::upgrade::Upgraded;

use crate::service::BroadcastService;
use crate::session::BroadcastSession;
use crate::stream::FastWebSocketStream;

pub struct BroadcastUpgradeHandler {
    pub(crate) service: Arc<BroadcastService>,
    pub(crate) application: Arc<str>,
}

#[async_trait]
impl H1UpgradeHandler for BroadcastUpgradeHandler {
    fn name(&self) -> &'static str {
        "broadcast"
    }

    fn is_upgrade(
        &self,
        request: &Request<Body>,
    ) -> bool {
        is_h1_header_upgrade(request, "websocket")
    }

    async fn accept(
        &self,
        _state: &AppState,
        ctx: &H1UpgradeContext,
    ) -> H1UpgradeResult {
        let mut session = self
            .service
            .accept_session(Arc::clone(&self.application))
            .map_err(|refusal| {
                H1UpgradeReject::new(
                    refusal.status(),
                    Some(refusal.to_string()),
                )
            })?;
        session
            .authenticate_headers(&ctx.host.to_string(), &ctx.headers)
            .await
            .map_err(|status| H1UpgradeReject::new(status, None))?;
        let headers = websocket_response_headers(ctx, None)
            .map_err(|error| H1UpgradeReject::bad_request(error.to_string()))?;
        Ok(H1UpgradeAccept::with_state(headers, session))
    }

    async fn handle_upgraded(
        &self,
        _state: AppState,
        ctx: H1UpgradeContext,
        accepted: Option<H1UpgradeState>,
        upgraded: Upgraded,
    ) -> registry::Result<()> {
        let session = accepted
            .ok_or_else(|| {
                registry::Error::msg("Broadcast accepted session missing")
            })?
            .take::<BroadcastSession>()?;
        session
            .run_with_shutdown(
                FastWebSocketStream::new(upgraded),
                ctx.sessions.wait_graceful_shutdown(),
            )
            .await
            .map_err(|error| registry::Error::recoverable(error.to_string()))
    }
}
