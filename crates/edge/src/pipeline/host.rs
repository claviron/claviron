//! Host-local actions prepared at boot/reload and visited in declaration order.
//!
//! [`HostLayer`] prepares head checks, consuming request calls and response
//! actions into one flat plan. Empty layers add no runtime entry. Active actions
//! use indirect calls; the plan does not discover layers or test config flags.
//! A consuming action returns `Unhandled` with the same request to continue.
//! Only reached layers decorate a response. Cancellation drops the current
//! request/future and caller-owned state; the plan never reads or copies bodies.

use std::{num::NonZeroUsize, ops::ControlFlow};

use claviron_http::{RouteBody, RouteResponse};
use http::{
    Request, Response, request::Parts as RequestParts,
    response::Parts as ResponseParts,
};

use super::layers::HostLayer;

mod application;
mod backend;
mod binding;
mod builder;
pub(crate) mod compile;
mod context;
mod dispatch;
mod execution;
mod handler;
mod owner;
mod registration;
mod request;
mod response;

pub use application::ApplicationHandler;
pub use backend::{
    Backend, BackendContract, BackendHandler, BackendIdentity, BackendReply,
};
pub(crate) use binding::HostPolicy;
pub use context::{ContextFuture, ContextHandler, HostContext};
pub use dispatch::{
    DispatchCall, DispatchFuture, DispatchResult, DispatchStep,
};
pub use execution::HostExecution;
pub use handler::{
    HostHandler, HostHandlerFuture, HostHandlerResult, HostRequest,
};
pub use registration::LayerContext;
pub(in crate::pipeline) use registration::LayerRegistration;
pub use response::ReplyHooks;

type HeadAction<S, R> =
    Box<dyn Fn(&mut RequestParts, &mut S) -> ControlFlow<R> + Send + Sync>;
type RequestAction = Box<
    dyn for<'a> Fn(Request<RouteBody>, Option<HostContext<'a>>) -> DispatchCall
        + Send
        + Sync,
>;
type ContextHeadAction<S, R> = Box<
    dyn for<'a> Fn(
            &mut RequestParts,
            &mut S,
            Option<HostContext<'a>>,
        ) -> ControlFlow<R>
        + Send
        + Sync,
>;

enum Action<S, R> {
    Head(HeadAction<S, R>),
    ContextHead(ContextHeadAction<S, R>),
    Request(RequestAction),
    ExternalRequest(RequestAction, tokio_util::sync::CancellationToken),
    ContextRequest(Box<dyn ContextHandler>),
    AsyncHead(Box<dyn AsyncHeadAction<S>>),
    Exchange(Box<dyn ExchangeAction>),
}

type ResponseAction<S> = Box<dyn Fn(&mut ResponseParts, &mut S) + Send + Sync>;
type BodyAction<S> = Box<dyn Fn(RouteBody, &mut S) -> RouteBody + Send + Sync>;

/// One active asynchronous head check. The future borrows its prepared owner
/// and request state, so capacity declarations need no per-call owner clone.
pub trait AsyncHeadAction<S>: Send + Sync {
    fn call<'a>(
        &'a self,
        head: &'a mut RequestParts,
        state: &'a mut S,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = ControlFlow<RouteResponse>>
                + Send
                + 'a,
        >,
    >;
}

/// Head admission that can retain work for the same request's response.
pub trait ExchangeAction: Send + Sync {
    fn call<'a>(
        &'a self,
        head: &'a mut RequestParts,
        reply: &'a mut ReplyHooks,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = ControlFlow<RouteResponse>>
                + Send
                + 'a,
        >,
    >;
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
    pub(in crate::pipeline) identity: Option<claviron_http::policy::HttpPolicy>,
}

/// Collects prepared layers in declaration order. No runtime is constructed.
pub struct HostPipelineBuilder<S = (), R = RouteResponse> {
    before: Vec<PreparedAction<S, R>>,
    after: Vec<ResponseAction<S>>,
    body: Vec<BodyAction<S>>,
    identity: Option<NonZeroUsize>,
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
    has_exchange: bool,
    identity: Option<NonZeroUsize>,
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
    context: Option<HostContext<'p>>,
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
