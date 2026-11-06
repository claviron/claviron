//! The request execution contract: what an application or embedded runtime
//! implements to answer requests.
//!
//! A backend implements [`DispatchStep`] and [`BackendHandler`] here and needs
//! nothing else from Claviron's serving side: neither virtual hosts nor
//! listeners nor connections. Who selected it, and how the request arrived, is
//! lent through [`HostContext`] for the duration of a call.
//!
//! See `ARCHITECTURE.md` for what is here and what is still on its way.

pub mod accept;
#[cfg(feature = "axum")]
pub mod axum;
mod backend;
mod context;
mod dispatch;
pub mod error;
pub mod extension;
pub mod factory;
mod handler;
pub mod layers;
#[cfg(feature = "access-log")]
pub mod log;
pub mod plan;
pub mod response;
pub mod site;
#[cfg(feature = "warp")]
pub mod warp;

pub use backend::{
    BackendContract, BackendHandler, BackendIdentity, BackendReply,
};
pub use context::{ContextFuture, ContextHandler, HostContext};
pub use dispatch::{
    DispatchCall, DispatchFuture, DispatchResult, DispatchStep,
};
pub use error::{Error, Result};
pub use handler::{
    HostHandler, HostHandlerFuture, HostHandlerResult, HostRequest,
};
pub use site::Site;
