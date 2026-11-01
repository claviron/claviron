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
        let mut next = 0;
        while let Some(step) = self.before.get(next) {
            let Action::Head(action) = &step.action else { break };
            if let ControlFlow::Break(response) = action(&mut head, &mut state) {
                return ServiceResult::Handled(PreparedReply {
                    pipeline: self, response, state, scope: step.scope,
                });
            }
            next += 1;
        }
        ServiceResult::Unhandled(PreparedRequest {
            pipeline: self, head, body, state, next,
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
            next: self.next,
        }
    }

}

impl<'p, S: Send + 'p> PreparedRequest<'p, RouteBody, S> {
    /// Continue the flat plan, then transfer an unclaimed request to this owner.
    pub fn dispatch_to<H>(
        self,
        application: H,
    ) -> impl Future<Output = RouteResponse> + Send + 'p
    where
        H: ApplicationHandler + 'p,
    {
        self.dispatch(move |request| application.call(request))
    }

    /// Visit remaining active actions without middleware recursion or a task.
    /// A request action owns the body while pending. Returning Unhandled moves
    /// it back into this driver; cancellation drops that owner and its guards.
    pub async fn dispatch<F, A>(self, handler: F) -> RouteResponse
    where
        F: FnOnce(Request<RouteBody>) -> A + Send + 'p,
        A: Future<Output = RouteResponse> + Send + 'p,
    {
        let Self { pipeline, mut head, mut body, mut state, next } = self;
        for step in &pipeline.before[next..] {
            match &step.action {
                Action::Head(action) => {
                    if let ControlFlow::Break(response) = action(&mut head, &mut state) {
                        return pipeline.finish(response, state, step.scope);
                    }
                }
                Action::AsyncHead(action) => {
                    if let ControlFlow::Break(response) = action.call(&mut head, &mut state).await {
                        return pipeline.finish(response, state, step.scope);
                    }
                }
                Action::Request(action) => {
                    match action(Request::from_parts(head, body)).await {
                        ServiceResult::Handled(response) => {
                            return pipeline.finish(response, state, step.scope);
                        }
                        ServiceResult::Unhandled(request) => {
                            (head, body) = request.into_parts();
                        }
                    }
                }
            }
        }
        let response = future::lazy(move |_| handler(Request::from_parts(head, body)))
            .flatten().await;
        pipeline.finish(response, state, pipeline.complete)
    }
}

#[cfg(feature = "rate-limit")]
impl<S> HostPipeline<S, crate::service::HostResponse> {
    pub(crate) fn check_head(
        &self,
        head: &mut RequestParts,
        state: &mut S,
    ) -> Result<(), crate::service::HostResponse> {
        for step in &self.before {
            let Action::Head(action) = &step.action else {
                unreachable!("typed head decisions cannot register request actions")
            };
            if let ControlFlow::Break(reply) = action(head, state) {
                return Err(reply);
            }
        }
        Ok(())
    }
}
