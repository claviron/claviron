//! Concrete host actions and adapters prepared for HostPipeline.

use crate::plan::{
    DispatchCall, DispatchStep, HostLayerBuilder, HostPipelineBuilder,
};
use claviron_http::{RouteBody, RouteResponse};
use std::ops::ControlFlow;

pub mod acl;
mod app;
mod body;
mod call;
mod cors;
mod deny;
mod extension;
pub use extension::ScopedExtension;
mod fallback;
mod files;
mod headers;
mod identity;
#[cfg(feature = "access-log")]
mod log;
#[cfg(feature = "metrics")]
mod metrics;
mod proxy;
#[cfg(feature = "rate-limit")]
mod quota;
mod redirect;
mod reply;
mod selection;

pub use acl::AclLayer;
pub use body::RequestBodyLayer;
pub use cors::{CorsLayer, CorsState};
pub use deny::DenyLayer;
pub use fallback::prepare_fallback;
pub use files::scope_steps;
pub use files::{StaticStage, TryFilesStage};
pub use headers::{HeaderClearLayer, HeaderSetLayer, prepare_headers};
pub use identity::IdentityPolicy;
#[cfg(feature = "access-log")]
pub use log::{AccessLogLayer, AccessLogState};
#[cfg(feature = "metrics")]
pub use metrics::{MetricsLayer, MetricsState};
pub use proxy::ProxyLayer;
#[cfg(feature = "rate-limit")]
pub use quota::{HostQuotas, QuotaPolicy, QuotaRegistrations, QuotaState};
pub use redirect::RedirectLayer;
pub use reply::ReplyPolicy;
pub use selection::RouteSelectionLayer;

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
