//! Concrete host actions and adapters prepared for HostPipeline.

use super::host::{
    DispatchFuture, DispatchStep, HostLayerBuilder, HostPipelineBuilder,
};
use claviron_http::{RouteBody, RouteResponse};
#[cfg(feature = "access-log")]
use http::Request;
#[cfg(feature = "acme")]
use http::Response;
use std::ops::ControlFlow;

#[cfg(feature = "access-log")]
mod log;
mod acl;
mod app;
mod call;
mod cors;
mod deny;
#[cfg(any(feature = "lua", feature = "php_embed", feature = "php_fcgi"))]
mod embed;
mod extension;
mod fallback;
mod files;
mod headers;
#[cfg(feature = "acme")]
mod http01;
mod identity;
#[cfg(feature = "orbit")]
mod link;
mod protocol;
mod proxy;
#[cfg(feature = "rate-limit")]
mod quota;
mod redirect;
mod reply;
mod request_body;
mod selection;
#[cfg(feature = "upstream")]
mod upstream;
#[cfg(feature = "websocket")]
mod websocket;

#[cfg(feature = "access-log")]
pub use log::{AccessLogLayer, AccessLogState};
pub use acl::AclLayer;
pub use cors::{CorsLayer, CorsState};
pub use deny::DenyLayer;
pub(super) use fallback::prepare_fallback;
pub(super) use files::scope_steps;
pub use files::{StaticStage, TryFilesStage};
pub use headers::{HeaderClearLayer, HeaderSetLayer, prepare_headers};
#[cfg(feature = "acme")]
pub use http01::Http01Layer;
pub use identity::{IdentityPolicy, IdentityState};
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
pub use request_body::RequestBodyLayer;
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
