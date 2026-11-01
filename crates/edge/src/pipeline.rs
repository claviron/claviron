#[cfg(feature = "axum")]
pub mod app;
pub(crate) mod apps;
mod connect;
pub mod embed;
pub mod ext;
pub mod host;
#[cfg(feature = "orbit")]
pub mod link;
pub mod other;
mod routable;
pub mod routing;
#[cfg(feature = "upstream")]
pub mod upstream;
#[cfg(feature = "webtransport")]
mod wt;

pub use apps::{
    Admission, HostAppRegistry, HostAppResolver, HostAppSnapshot,
    RustRouter,
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
#[cfg(feature = "websocket")]
pub use host::WebSocketStage;
#[cfg(feature = "access-log")]
pub use host::{AccessLogLayer, AccessLogState};
pub use host::{
    AclLayer, DenyLayer, ProxyLayer, RedirectLayer, HttpsRedirectLayer, AdmissionPolicy, CapacityLayer, CapacityState, AsyncHeadAction, ApplicationHandler,
    DispatchFuture, DispatchResult,
    DispatchStep, CorsLayer, CorsState, HeaderClearLayer, HeaderSetLayer, prepare_headers, HostHandler, HostHandlerFuture,
    HostHandlerResult, HostLayer, HostLayerBuilder, HostPipeline,
    HostPipelineBuilder, HostRequest, IdentityPolicy,
    IdentityState, AuthorityLayer, RejectLayer, UpgradeLayer, ReplyPolicy, RequestBodyLayer, StaticStage,
    TryFilesStage,
};
#[cfg(feature = "rate-limit")]
pub use host::{QuotaPolicy, QuotaState};
pub use routable::{HostBindingProvider, Routable, RouteBinding};
#[cfg(feature = "webtransport")]
pub use wt::{
    H3WebTransportBiStream, H3WebTransportSession, WebTransportAccept,
    WebTransportBinding, WebTransportHandler, WebTransportRegistry,
    WebTransportReject, WebTransportResult, webtransport_session_ended,
};

#[cfg(feature = "http3")]
pub use host::EarlyDataLayer;

#[cfg(feature = "acme")]
pub use host::Http01Layer;
