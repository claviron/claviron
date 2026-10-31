pub(crate) mod apps;
pub mod other;
pub mod embed;
mod connect;
pub mod host;
#[cfg(feature = "axum")]
pub mod app;
pub mod ext;
pub mod routing;
#[cfg(feature = "orbit")]
pub mod link;
#[cfg(feature = "upstream")]
pub mod upstream;
mod routable;
#[cfg(feature = "webtransport")]
mod wt;

pub use apps::{
    Admission, RustRouter, HostApp, HostAppRegistry, HostAppResolver,
    HostAppSnapshot,
};
#[cfg(feature = "websocket")]
pub use claviron_http::upgrade::{
    H1UpgradeAccept, H1UpgradeAuthenticated, H1UpgradeBinding,
    H1UpgradeContext, H1UpgradeHandler, H1UpgradeRegistry, H1UpgradeReject,
    H1UpgradeResult, H1UpgradeRoute, H1UpgradeState, is_h1_header_upgrade,
    websocket_accept_key, websocket_response_headers,
};
pub use connect::{
    AnyAuthenticated, AuthContext, H2ConnectAccept, H2ConnectBinding,
    H2ConnectHandler, H2ConnectRegistry, H2ConnectReject, H2ConnectResult,
};
pub use host::{
    AccessPolicy, AccessState, AdmissionPolicy, ApplicationHandler,
    HostHandler, ApplicationStage, HostHandlerFuture,
    HostRequest, HostHandlerResult,
    DispatchChain, DispatchFuture, DispatchResult, DispatchStep,
    StaticStage, TryFilesStage,
    ExchangeLayer, ExchangeState,
    HostLayer, HostLayerBuilder, HostPipeline, HostPipelineBuilder,
    HostRequestState, IdentityPolicy, IdentityState, IngressPolicy, ReplyPolicy,
    IngressState,
};
#[cfg(feature = "websocket")]
pub use host::WebSocketStage;
#[cfg(feature = "access-log")]
pub use host::{AccessLogLayer, AccessLogState};
#[cfg(feature = "rate-limit")]
pub use host::{QuotaPolicy, QuotaState};
pub use routable::{HostBindingProvider, RouteBinding, Routable};
#[cfg(feature = "axum")]
pub use routable::RouteLayerContext;
#[cfg(feature = "webtransport")]
pub use wt::{
    H3WebTransportBiStream, H3WebTransportSession, WebTransportAccept,
    WebTransportBinding, WebTransportHandler, WebTransportRegistry,
    WebTransportReject, WebTransportResult, webtransport_session_ended,
};
