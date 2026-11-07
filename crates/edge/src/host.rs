pub mod accept;
pub mod apps;
pub(crate) mod compile;
mod connect;
#[cfg(all(feature = "orbit", feature = "webtransport"))]
mod link_webtransport;
mod non_serving;
mod routable;
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
pub(crate) use claviron_pipeline::accept::AcceptLayer;
pub use connect::{AnyAuthenticated, AuthContext};
#[cfg(feature = "tcp")]
pub use non_serving::NonServingPipeline;
pub use non_serving::{InterceptsNonServing, NonServingContext};
pub(crate) use routable::RouteProtocol;
pub use routable::{HostBindingProvider, Routable, RouteBinding};
#[cfg(feature = "webtransport")]
pub use wt::{
    H3WebTransportBiStream, H3WebTransportSession, WebTransportAccept,
    WebTransportBinding, WebTransportHandler, WebTransportRegistry,
    WebTransportReject, WebTransportResult, webtransport_session_ended,
};
