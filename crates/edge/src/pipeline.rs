pub(crate) mod apps;
pub mod bypass;
mod connect;
pub mod host;
#[cfg(feature = "axum")]
pub mod merged;
#[cfg(feature = "orbit")]
mod orbit;
mod routable;
#[cfg(feature = "webtransport")]
mod wt;

pub use apps::{
    Admission, AppKind, HostApp, HostAppRegistry, HostAppResolver,
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
    AccessPolicy, AccessState, HostLayer, HostLayerBuilder, HostPipeline, HostPipelineBuilder,
    HostRequestState, IdentityPolicy, IdentityState, IngressPolicy,
    IngressState,
};
#[cfg(feature = "rate-limit")]
pub use host::{QuotaPolicy, QuotaState};
#[cfg(feature = "orbit")]
pub use orbit::build_application_router as build_orbit_application_router;
pub use routable::{HostBindingProvider, RouteBinding};
#[cfg(feature = "axum")]
pub use routable::{Routable, RouteLayerContext};
#[cfg(feature = "webtransport")]
pub use wt::{
    H3WebTransportBiStream, H3WebTransportSession, WebTransportAccept,
    WebTransportBinding, WebTransportHandler, WebTransportRegistry,
    WebTransportReject, WebTransportResult, webtransport_session_ended,
};
