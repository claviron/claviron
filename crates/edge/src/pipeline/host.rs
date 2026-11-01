//! Host-local actions prepared at boot/reload and visited in declaration order.
//!
//! [`HostLayer`] prepares head checks, consuming request calls and response
//! actions into one flat plan. Empty layers add no runtime entry. Active actions
//! use indirect calls; the plan does not discover layers or test config flags.
//! A consuming action returns `Unhandled` with the same request to continue.
//! Only reached layers decorate a response. Cancellation drops the current
//! request/future and caller-owned state; the plan never reads or copies bodies.

use std::ops::ControlFlow;

use claviron_http::{RouteBody, RouteResponse};
use http::{
    Request, Response, request::Parts as RequestParts,
    response::Parts as ResponseParts,
};

mod acl;
mod deny;
mod proxy;
mod redirect;
#[cfg(feature = "access-log")]
mod access_log;
mod admission;
mod application;
mod backend;
mod binding;
mod builder;
pub(crate) mod compile;
mod dispatch;
mod cors;
mod headers;
mod extension;
mod files;
mod handler;
mod identity;
mod protocol;
#[cfg(feature = "rate-limit")]
mod quota;
mod reply;
mod request_body;
#[cfg(feature = "websocket")]
mod websocket;

pub use acl::AclLayer;
pub use deny::DenyLayer;
pub use proxy::ProxyLayer;
pub use redirect::RedirectLayer;
#[cfg(feature = "access-log")]
pub use access_log::{AccessLogLayer, AccessLogState};
pub use admission::AdmissionPolicy;
pub use application::ApplicationHandler;
pub(crate) use binding::HostPolicy;
pub use dispatch::{
    DispatchFuture, DispatchResult, DispatchStep,
};
pub use cors::{CorsLayer, CorsState};
pub use headers::{HeaderClearLayer, HeaderSetLayer, prepare_headers};
pub(crate) use extension::bind_other;
pub(crate) use files::fallback_config;
pub use files::{StaticStage, TryFilesStage};
pub use handler::{
    HostHandler, HostHandlerFuture, HostHandlerResult,
    HostRequest,
};
pub use identity::{IdentityPolicy, IdentityState};
pub use protocol::{AuthorityLayer, RejectLayer, UpgradeLayer};
#[cfg(feature = "http3")]
pub use protocol::EarlyDataLayer;
#[cfg(feature = "rate-limit")]
pub(crate) use quota::QuotaRegistrations;
#[cfg(feature = "rate-limit")]
pub use quota::{HostQuotas, QuotaPolicy, QuotaState};
pub use reply::ReplyPolicy;
pub use request_body::RequestBodyLayer;
#[cfg(feature = "websocket")]
pub use websocket::WebSocketStage;
mod request;
mod response;

type HeadAction<S, R> =
    Box<dyn Fn(&mut RequestParts, &mut S) -> ControlFlow<R> + Send + Sync>;
type RequestAction =
    Box<dyn Fn(Request<RouteBody>) -> DispatchFuture + Send + Sync>;

enum Action<S, R> {
    Head(HeadAction<S, R>),
    Request(RequestAction),
}

type ResponseAction<S> = Box<dyn Fn(&mut ResponseParts, &mut S) + Send + Sync>;
type BodyAction<S> = Box<dyn Fn(RouteBody, &mut S) -> RouteBody + Send + Sync>;

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

/// Actions contributed by one logical layer, allocated only while building.
///
/// Its response/body actions become eligible when its first request action is
/// entered, including when that action replies early. A response-only layer
/// becomes eligible at its declared position before the next head action or
/// terminal handler. Later layers never decorate an earlier reply.
pub struct HostLayerBuilder<S = (), R = RouteResponse> {
    before: Vec<Action<S, R>>,
    after: Vec<ResponseAction<S>>,
    body: Vec<BodyAction<S>>,
}

/// Collects prepared layers in declaration order. No runtime is constructed.
pub struct HostPipelineBuilder<S = (), R = RouteResponse> {
    layers: Vec<HostLayerBuilder<S, R>>,
}

/// Immutable prepared actions; retain this value with the host binding.
///
/// No plan or action is cloned by `prepare` or dispatch. Concurrent requests
/// borrow the same immutable plan and own independent `S` values and bodies.
pub struct HostPipeline<S = (), R = RouteResponse> {
    before: Box<[PreparedAction<S, R>]>,
    after: Box<[ResponseAction<S>]>,
    body: Box<[BodyAction<S>]>,
    complete: ReplyScope,
}

struct PreparedAction<S, R> {
    action: Action<S, R>,
    scope: ReplyScope,
}

#[derive(Clone, Copy, Default)]
struct ReplyScope {
    after: usize,
    body: usize,
}

/// An unclaimed request. Its private body has not been exposed to head actions.
///
/// Drop releases the body and request state. `map_body` transfers the existing
/// body once; dispatch consumes the request and gives it to one terminal owner.
pub struct PreparedRequest<'p, B, S = (), R = RouteResponse> {
    pipeline: &'p HostPipeline<S, R>,
    head: RequestParts,
    body: B,
    state: S,
    next: usize,
}

/// An early reply plus the request state and already-entered response scope.
///
/// `finish` applies that scope. Dropping this value instead releases its
/// response body and state without running success-only response actions.
pub struct PreparedReply<'p, S = (), R = RouteResponse> {
    pipeline: &'p HostPipeline<S, R>,
    response: R,
    state: S,
    scope: ReplyScope,
}

#[cfg(test)]
#[path = "../../tests/pipeline/host.rs"]
mod tests;
