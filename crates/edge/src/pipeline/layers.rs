//! Layers bound to Edge's host table; the rest are
//! `claviron_pipeline::layers`.

pub use claviron_pipeline::layers::*;

#[cfg(feature = "orbit")]
mod link;
#[cfg(feature = "websocket")]
mod websocket;

#[cfg(feature = "websocket")]
pub use websocket::WebSocketStage;
