use std::future::Future;

use claviron_http::ServiceResult;
use futures_util::{FutureExt, future};

use super::*;

impl<R> HostPipeline<(), R> {
    /// Prepare a request with no additional exchange state.
    pub fn prepare<B>(
        &self,
        request: Request<B>,
    ) -> ServiceResult<PreparedReply<'_, (), R>, PreparedRequest<'_, B, (), R>>
    {
        self.prepare_with_state(request, ())
    }
}

impl<S, R> HostPipeline<S, R> {
    /// Borrow the head for active actions; retain the original body untouched.
    ///
    /// `S` belongs to this request, not to the shared plan. It may contain
    /// typed RAII guards. A reply drops the unclaimed request body immediately;
    /// cancellation drops whatever body/state its current owner still holds.
    pub fn prepare_with_state<B>(
        &self,
        request: Request<B>,
        mut state: S,
    ) -> ServiceResult<PreparedReply<'_, S, R>, PreparedRequest<'_, B, S, R>>
    {
        let (mut head, body) = request.into_parts();
        for step in &self.before {
            if let ControlFlow::Break(response) =
                (step.action)(&mut head, &mut state)
            {
                return ServiceResult::Handled(PreparedReply {
                    pipeline: self,
                    response,
                    state,
                    scope: step.scope,
                });
            }
        }
        ServiceResult::Unhandled(PreparedRequest {
            pipeline: self,
            head,
            body,
            state,
        })
    }
}

impl<'p, B, S, R> PreparedRequest<'p, B, S, R> {
    /// Inspect the admitted head without gaining access to the body.
    pub fn head(&self) -> &RequestParts {
        &self.head
    }

    /// Transfer the body explicitly, for example at native RouteBody erasure.
    ///
    /// The mapper owns the old body and returns the new representation. The
    /// pipeline itself performs no conversion, buffering or body inspection.
    pub fn map_body<C>(
        self,
        map: impl FnOnce(B) -> C,
    ) -> PreparedRequest<'p, C, S, R> {
        PreparedRequest {
            pipeline: self.pipeline,
            head: self.head,
            body: map(self.body),
            state: self.state,
        }
    }

    /// Hand this request to one terminal handler on first poll, not construction.
    ///
    /// No task or future box is added. Before first poll the deferred closure
    /// owns the request; afterwards the handler future owns it. `S` remains
    /// beside that future until reply-head completion or cancellation. A body
    /// action can then move the guards it needs into the returned response body.
    pub fn dispatch<F, A>(
        self,
        handler: F,
    ) -> impl Future<Output = RouteResponse> + Send + 'p
    where
        B: Send + 'p,
        S: Send + 'p,
        F: FnOnce(Request<B>) -> A + Send + 'p,
        A: Future<Output = RouteResponse> + Send + 'p,
    {
        let Self { pipeline, head, body, state } = self;
        future::lazy(move |_| handler(Request::from_parts(head, body)))
            .flatten()
            .map(move |response| {
                pipeline.finish(response, state, pipeline.complete)
            })
    }
}
