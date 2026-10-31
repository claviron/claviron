//! Prepared host actions shared by host admission and application dispatch.
//!
//! A [`HostLayer`] contributes synchronous head/response actions at preparation
//! time. [`HostPipelineBuilder::build`] flattens only those actions into an
//! immutable plan. There is no request-time layer discovery, `enabled` test,
//! recursive `Next`, per-layer future, or request-local type-erased state map.
//! `S` is caller-owned, typed exchange state; the default `()` occupies no space.
//!
//! [`HostPipeline::prepare_with_state`] owns an arbitrary, non-Clone request
//! body. Head actions borrow only its parts. An unclaimed request is returned
//! as [`claviron_http::ServiceResult::Unhandled`], still owning that same body.
//! Mapping the body is explicit, and only dispatch hands it to the terminal
//! handler. The plan never polls, collects, decodes, clones or re-boxes it.
//!
//! Actions are erased/allocated once at build time. Each active action still
//! costs an indirect call; this module does not claim otherwise. Dispatch keeps
//! its surrounding future concrete and adds no future box, task or timer.
//! Request/response body erasure and an application's existing future erasure
//! remain the caller's responsibility.
//!
//! [`IngressPolicy`] is the first native admission layer. It retains typed
//! rejection reasons until the transport frames its reply. [`IdentityPolicy`]
//! then prepares typed connection/host extensions and request-local proxy IP.
//! [`AccessPolicy`] applies live canonical-host ACL/redirect decisions next.
//! With `rate-limit`, `QuotaPolicy` prepares the selected application's rate
//! admission after access. [`ExchangeLayer`] prepares configured CORS and reply
//! headers. [`ReplyPolicy`] prepares only declared response-body guards.
//! [`AdmissionPolicy`] supplies prepared queue/head timing to HTTP's existing
//! asynchronous admission boundary, outside synchronous head actions.
//! With `access-log`, `AccessLogLayer` retains the producer/host declaration
//! while channel selection and each record's lifetime remain request-local.
//! [`ApplicationHandler`] owns the terminal call with a concrete future.
//! With `websocket`, `WebSocketStage` precedes applications only when H1 and
//! its handshake are allowed and exact endpoints were prepared for that host.
//! [`StaticStage`] and [`TryFilesStage`] prepare distinct native file steps
//! after Rust interception. Their misses retain the unread body capability.
//! Native execution composes the prepared policies around that call;
//! `HostService` remains the admission/transport handoff and owner selector.

use std::ops::ControlFlow;

use claviron_http::{RouteBody, RouteResponse};
use http::{
    Request, Response, request::Parts as RequestParts,
    response::Parts as ResponseParts,
};

mod access;
#[cfg(feature = "access-log")]
mod access_log;
mod admission;
mod application;
mod binding;
mod builder;
mod dispatch;
mod exchange;
mod execution;
mod files;
mod identity;
mod intercept;
pub(crate) mod ingress;
#[cfg(feature = "rate-limit")]
mod quota;
mod reply;
mod state;
#[cfg(feature = "websocket")]
mod websocket;

pub use access::{AccessPolicy, AccessState};
#[cfg(feature = "access-log")]
pub use access_log::{AccessLogLayer, AccessLogState};
pub use admission::AdmissionPolicy;
pub use application::ApplicationHandler;
pub use dispatch::{DispatchChain, DispatchFuture, DispatchResult, DispatchStep};
pub(crate) use dispatch::compose_dispatch_bypass;
#[cfg(feature = "axum")]
pub(crate) use dispatch::{compose_dispatch_router, dispatch_router};
pub(crate) use binding::HostPolicy;
pub use exchange::{ExchangeLayer, ExchangeState};
pub(crate) use execution::HostExecution;
pub use files::{StaticStage, TryFilesStage};
#[cfg(feature = "axum")]
pub(crate) use files::compose_configured_router;
#[cfg(feature = "axum")]
pub(crate) use files::fallback_config;
#[cfg(feature = "upstream")]
pub(crate) use files::prepare_upstream_files;
pub use identity::{IdentityPolicy, IdentityState};
pub use intercept::{
    ApplicationInterceptor, ApplicationStage, InterceptFuture,
    InterceptRequest, InterceptResult,
};
pub(crate) use intercept::compose_bypass;
#[cfg(feature = "axum")]
pub(crate) use intercept::compose_router;
pub use ingress::{IngressPolicy, IngressState};
#[cfg(feature = "rate-limit")]
pub use quota::{QuotaPolicy, QuotaState};
pub use reply::ReplyPolicy;
pub use state::HostRequestState;
pub(crate) use state::IdentifiedRequest;
#[cfg(feature = "websocket")]
pub use websocket::WebSocketStage;
#[cfg(feature = "websocket")]
pub(crate) use websocket::compose_websocket_bypass;
#[cfg(all(feature = "websocket", feature = "axum"))]
pub(crate) use websocket::compose_websocket_router;
mod request;
mod response;

type HeadAction<S, R> =
    Box<dyn Fn(&mut RequestParts, &mut S) -> ControlFlow<R> + Send + Sync>;
type ResponseAction<S> = Box<dyn Fn(&mut ResponseParts, &mut S) + Send + Sync>;
type BodyAction<S> = Box<dyn Fn(RouteBody, &mut S) -> RouteBody + Send + Sync>;

/// Cold preparation contract. An inactive layer contributes no actions.
///
/// Failure discards this layer's contributions without changing the pipeline
/// builder. Layer preparation never receives a request or a body. `R` preserves
/// a typed early decision; response-head/body hooks require `RouteResponse`.
pub trait HostLayer<S = (), R = RouteResponse> {
    fn prepare(
        self,
        layer: &mut HostLayerBuilder<S, R>,
    ) -> crate::Result<()>;
}

/// Actions contributed by one logical layer, allocated only while building.
///
/// Its response/body actions become eligible when its first head action is
/// entered, including when that action replies early. A response-only layer
/// becomes eligible at its declared position before the next head action or
/// terminal handler. Later layers never decorate an earlier reply.
pub struct HostLayerBuilder<S = (), R = RouteResponse> {
    before: Vec<HeadAction<S, R>>,
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
    before: Box<[PreparedHeadAction<S, R>]>,
    after: Box<[ResponseAction<S>]>,
    body: Box<[BodyAction<S>]>,
    complete: ReplyScope,
}

struct PreparedHeadAction<S, R> {
    action: HeadAction<S, R>,
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

/// Response actions armed by a borrowed request head.
///
/// The caller keeps its body and owns any early reply. Finish exactly one
/// eventual response, including an admission/timeout response. Dropping this
/// scope releases its state without executing response callbacks.
pub struct PreparedResponse<'p, S = (), R = RouteResponse> {
    pipeline: &'p HostPipeline<S, R>,
    state: S,
    scope: ReplyScope,
}

#[cfg(test)]
#[path = "../../tests/pipeline/host.rs"]
mod tests;
