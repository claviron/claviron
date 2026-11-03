//! Concrete host actions and adapters prepared for HostPipeline.

use super::host::{
    DispatchCall, DispatchStep, HostLayerBuilder, HostPipelineBuilder,
};
use claviron_http::{RouteBody, RouteResponse};
use std::ops::ControlFlow;

mod acl;
mod app;
mod body;
mod call;
mod cors;
mod deny;
#[cfg(any(feature = "lua", feature = "php_embed", feature = "php_fcgi"))]
mod embed;
mod extension;
mod fallback;
mod files;
mod headers;
mod identity;
#[cfg(feature = "orbit")]
mod link;
#[cfg(feature = "access-log")]
mod log;
mod protocol;
mod proxy;
#[cfg(feature = "rate-limit")]
mod quota;
mod redirect;
mod reply;
mod selection;
#[cfg(feature = "upstream")]
mod upstream;
#[cfg(feature = "websocket")]
mod websocket;

pub use acl::AclLayer;
pub use body::RequestBodyLayer;
pub use cors::{CorsLayer, CorsState};
pub use deny::DenyLayer;
pub(super) use fallback::prepare_fallback;
pub(super) use files::scope_steps;
pub use files::{StaticStage, TryFilesStage};
pub use headers::{HeaderClearLayer, HeaderSetLayer, prepare_headers};
pub use identity::IdentityPolicy;
#[cfg(feature = "access-log")]
pub use log::{AccessLogLayer, AccessLogState};
#[cfg(feature = "http3")]
pub use protocol::EarlyDataLayer;
pub use protocol::{AuthorityLayer, RejectLayer, UpgradeLayer};
pub use proxy::ProxyLayer;
#[cfg(feature = "rate-limit")]
pub(crate) use quota::QuotaRegistrations;
#[cfg(feature = "rate-limit")]
pub use quota::{HostQuotas, QuotaPolicy, QuotaState};
pub use redirect::{HttpsRedirectLayer, RedirectLayer};
pub use reply::ReplyPolicy;
pub use selection::RouteSelectionLayer;
#[cfg(feature = "websocket")]
pub use websocket::WebSocketStage;

/// Cold preparation contract. An inactive layer contributes no actions.
///
/// Failure discards this layer's contributions without changing the pipeline
/// builder. Layer preparation never receives a request or a body. `R` preserves
/// a typed early decision. Consuming request and response hooks require
/// `RouteResponse`.
pub trait HostLayer<S = (), R = RouteResponse> {
    fn prepare(
        self,
        layer: &mut HostLayerBuilder<S, R>,
    ) -> crate::Result<()>;
}
