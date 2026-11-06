//! Layers bound to Edge's host table; the rest are
//! `claviron_pipeline::layers`.

pub use claviron_pipeline::layers::*;

#[cfg(feature = "orbit")]
mod link;
#[cfg(feature = "rate-limit")]
mod quota;
#[cfg(feature = "websocket")]
mod websocket;

#[cfg(feature = "rate-limit")]
pub(crate) use quota::QuotaRegistrations;
#[cfg(feature = "rate-limit")]
pub use quota::{HostQuotas, QuotaPolicy, QuotaState};
#[cfg(feature = "websocket")]
pub use websocket::WebSocketStage;
