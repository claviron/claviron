pub mod accept;
pub mod apps;
pub use claviron_pipeline::factory as backend;
mod connect;
pub use claviron_pipeline::extension;
pub mod host;
pub mod layers;
#[cfg(feature = "orbit")]
pub mod link;
mod non_serving;
mod routable;
pub mod routing;
#[cfg(feature = "webtransport")]
mod wt;

pub use apps::{HostAppRegistry, HostAppResolver, HostAppSnapshot, RustRouter};
#[cfg(feature = "websocket")]
pub use claviron_http::upgrade::{
    H1UpgradeAccept, H1UpgradeAuthenticated, H1UpgradeBinding,
    H1UpgradeContext, H1UpgradeHandler, H1UpgradeRegistry, H1UpgradeReject,
    H1UpgradeResult, H1UpgradeRoute, H1UpgradeState, is_h1_header_upgrade,
    websocket_accept_key, websocket_response_headers,
};
pub use claviron_pipeline::accept::{AcceptLayer, Accepted};
pub use connect::{
    AnyAuthenticated, AuthContext, H2ConnectAccept, H2ConnectBinding,
    H2ConnectHandler, H2ConnectRegistry, H2ConnectReject, H2ConnectResult,
};
pub use host::{
    ApplicationHandler, Backend, BackendContract, BackendHandler,
    BackendIdentity, BackendReply, DispatchCall, DispatchResult, DispatchStep,
    HostExecution, HostHandler, HostHandlerFuture, HostHandlerResult,
    HostLayerBuilder, HostPipelineBuilder, HostRequest,
};
pub use layers::{HostLayer, IdentityPolicy};
#[cfg(feature = "tcp")]
pub use non_serving::NonServingPipeline;
pub use non_serving::{InterceptsNonServing, NonServingContext};
pub use routable::{
    HostBindingProvider, Routable, RouteBinding, RouteProtocol,
};
#[cfg(feature = "webtransport")]
pub use wt::{
    H3WebTransportBiStream, H3WebTransportSession, WebTransportAccept,
    WebTransportBinding, WebTransportHandler, WebTransportRegistry,
    WebTransportReject, WebTransportResult, webtransport_session_ended,
};
