//! Application-owned verification of an existing session, never session creation.

use std::future::Future;

use async_trait::async_trait;
use http::{HeaderMap, StatusCode};

use crate::BroadcastSession;
use crate::stream::{Message, MessageStream};

/// Optional admission over the application's existing session authority.
/// Unconfigured applications retain Pusher-compatible anonymous connections.
#[async_trait]
pub trait SessionAuthenticator: Send + Sync + 'static {
    /// `vhost` is the canonical ingress key supplied by the transport, not a header.
    async fn authenticate_headers(
        &self,
        vhost: &str,
        headers: &HeaderMap,
    ) -> Result<(), StatusCode>;

    async fn authenticate_token(
        &self,
        vhost: &str,
        token: &str,
    ) -> Result<(), StatusCode>;
}

#[derive(serde::Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
enum Authentication {
    #[serde(rename = "broadcast.authenticate")]
    Session { token: String },
}

impl BroadcastSession {
    /// H1/H2 admission checks the existing cookie/Bearer credential before IO
    /// is handed off. An application callback must never create a session.
    pub async fn authenticate_headers(
        &mut self,
        vhost: &str,
        headers: &HeaderMap,
    ) -> Result<(), StatusCode> {
        if let Some(authenticator) = &self.authenticator {
            authenticator.authenticate_headers(vhost, headers).await?;
        }
        self.authenticated = true;
        Ok(())
    }

    /// WebTransport cannot carry browser cookies or custom auth headers.
    /// Consume its first bounded record before sending any Pusher message.
    /// Returns false on rejection or cooperative shutdown, without a new timer.
    pub async fn authenticate_stream<S: MessageStream>(
        &mut self,
        vhost: &str,
        stream: &mut S,
        shutdown: impl Future<Output = ()> + Send,
    ) -> crate::Result<bool> {
        let Some(authenticator) = self.authenticator.clone() else {
            self.authenticated = true;
            return Ok(true);
        };
        tokio::pin!(shutdown);
        let authenticate = async {
            let Some(Message::Text(text)) =
                stream.receive().await.map_err(|_| StatusCode::BAD_REQUEST)?
            else {
                return Err(StatusCode::UNAUTHORIZED);
            };
            // Never return/log decoder text: it could include the credential.
            let Authentication::Session { token } = serde_json::from_str(&text)
                .map_err(|_| StatusCode::BAD_REQUEST)?;
            authenticator.authenticate_token(vhost, &token).await
        };
        let result = tokio::select! {
            biased;
            _ = &mut shutdown => return Ok(false),
            _ = self.cancellation.cancelled() => return Ok(false),
            result = authenticate => result,
        };
        match result {
            Ok(()) => {
                self.authenticated = true;
                Ok(true)
            }
            Err(status) => {
                let (code, reason) = if status.is_server_error() {
                    (1011, "session verification unavailable")
                } else {
                    (1008, "existing session required")
                };
                tokio::select! {
                    biased;
                    _ = &mut shutdown => {},
                    _ = self.cancellation.cancelled() => {},
                    result = stream.send(Message::Close { code, reason: reason.into() }) => { result?; }
                }
                Ok(false)
            }
        }
    }
}
