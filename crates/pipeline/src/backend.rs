//! What a configured execution owner declares about itself.

use std::sync::Arc;

use claviron_http::{RouteBody, RustRouter};
use claviron_runtime::ExecutorSpec;
use http::Request;

use crate::{
    DispatchCall, DispatchStep, HostContext, HostHandler, HostRequest,
};

/// An application, embedded runtime or remote execution owner.
/// Internal Static/TryFiles operations implement only `DispatchStep`.
/// Register this owner through `Backend`, so its metadata contract is prepared
/// with the host plan rather than rediscovered during request execution.
pub trait BackendHandler: DispatchStep {
    /// Declared once during preparation. Each backend supplies its identity,
    /// execution placement and response lifetime.
    fn contract(&self) -> BackendContract;

    /// The router this owner is, when it is one: where nothing else stands
    /// around it, it is called directly, with no plan and no erased call. Asked
    /// once, during preparation.
    fn direct(&self) -> Option<RustRouter> {
        None
    }
}

/// Request metadata exported at the application boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendIdentity {
    /// Keep the request unchanged; native layers can borrow HostContext.
    None,
    /// Export the prepared policy, connection and selected hostname.
    Request,
}

/// How the pipeline delivers a backend's HTTP response.
#[derive(Clone)]
pub enum BackendReply {
    /// The execution owner cooperatively completes its own output.
    Owner,
    /// End this edge's external SSE body on the supplied owner's shutdown.
    /// Finite responses retain their normal completion and drain.
    ExternalEventStream(tokio_util::sync::CancellationToken),
    /// Apply the prepared response delivery policy at the relay boundary.
    /// External SSE replies end on the supplied shutdown token.
    Relay(tokio_util::sync::CancellationToken),
}

/// Mandatory cold declaration of the request and response handoff.
#[derive(Clone)]
pub struct BackendContract {
    /// Where this owner runs an exchange. A Tokio task on the caller's executor
    /// is still `Inherit`. An engine exposes a configuration key only when it
    /// supports an execution choice.
    pub execution: ExecutorSpec,
    pub identity: BackendIdentity,
    pub reply: BackendReply,
}

impl DispatchStep for RustRouter {
    fn call(
        &self,
        request: Request<RouteBody>,
        _context: Option<HostContext<'_>>,
    ) -> DispatchCall {
        DispatchCall::response(self.handle(request))
    }
}

impl BackendHandler for RustRouter {
    fn contract(&self) -> BackendContract {
        BackendContract {
            execution: ExecutorSpec::Inherit,
            identity: if RustRouter::reads_identity(self) {
                BackendIdentity::Request
            } else {
                BackendIdentity::None
            },
            reply: BackendReply::Owner,
        }
    }

    fn direct(&self) -> Option<RustRouter> {
        Some(self.clone())
    }
}

impl DispatchStep for Arc<dyn HostHandler> {
    fn call(
        &self,
        request: Request<RouteBody>,
        _context: Option<HostContext<'_>>,
    ) -> DispatchCall {
        DispatchCall::application(HostHandler::call(
            self.as_ref(),
            HostRequest::new(request),
        ))
    }
}

impl BackendHandler for Arc<dyn HostHandler> {
    fn contract(&self) -> BackendContract {
        BackendContract {
            execution: ExecutorSpec::Inherit,
            identity: BackendIdentity::None,
            reply: BackendReply::Owner,
        }
    }
}

impl<T: DispatchStep + ?Sized> DispatchStep for Arc<T> {
    fn call(
        &self,
        request: Request<RouteBody>,
        context: Option<HostContext<'_>>,
    ) -> DispatchCall {
        self.as_ref().call(request, context)
    }
}

impl<T: BackendHandler + ?Sized> BackendHandler for Arc<T> {
    fn contract(&self) -> BackendContract {
        self.as_ref().contract()
    }

    fn direct(&self) -> Option<RustRouter> {
        (**self).direct()
    }
}
