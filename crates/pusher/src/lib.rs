mod auth;
mod config;
#[cfg(feature = "http")]
mod http;
mod protocol;

pub use config::{
    HttpTargetConfig, PusherAppId, PusherAppKey, PusherAppSecret, PusherConfig,
    PusherTarget,
};
#[cfg(feature = "http")]
pub use http::adapter::PusherAdapter;
pub use protocol::{
    ClientMessage, PresenceMember, PresenceMemberError, PresenceSnapshot,
    PublishedEvent, ServerMessage, SubscriptionRequest, TriggerEvent,
    TriggerEventError, subscription,
};

#[cfg(feature = "axum")]
pub fn pusher_routes(cfg: std::sync::Arc<PusherConfig>) -> axum::Router {
    use crate::http::routes::http_pusher_router;
    http_pusher_router(cfg)
}
pub use auth::{
    AuthenticationError, ChannelAuthorizationError, authenticate,
    authenticate_at, authentication_query, authentication_query_at,
    authorize_channel, verify_channel_authorization,
};
