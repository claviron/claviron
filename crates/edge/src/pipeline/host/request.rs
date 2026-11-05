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
        state: S,
    ) -> ServiceResult<PreparedReply<'_, S, R>, PreparedRequest<'_, B, S, R>>
    {
        self.prepare_in(request, state, None)
    }

    /// Execute active head actions with borrowed transport facts. The context
    /// remains outside request extensions and cannot outlive this exchange.
    pub fn prepare_in<'p, B>(
        &'p self,
        request: Request<B>,
        mut state: S,
        context: Option<HostContext<'p>>,
    ) -> ServiceResult<PreparedReply<'p, S, R>, PreparedRequest<'p, B, S, R>>
    {
        let (mut head, body) = request.into_parts();
        let mut next = 0;
        while let Some(step) = self.before.get(next) {
            let decision = match &step.action {
                Action::Head(action) => action(&mut head, &mut state),
                Action::ContextHead(action) => {
                    action(&mut head, &mut state, context)
                }
                _ => break,
            };
            if let ControlFlow::Break(response) = decision {
                return ServiceResult::Handled(PreparedReply {
                    pipeline: self,
                    response,
                    state,
                    scope: step.scope,
                });
            }
            next += 1;
        }
        ServiceResult::Unhandled(PreparedRequest {
            pipeline: self,
            head,
            body,
            state,
            next,
            context,
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
            context: self.context,
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
    pub fn dispatch<F, A>(
        self,
        handler: F,
    ) -> impl Future<Output = RouteResponse> + Send + 'p
    where
        F: FnOnce(Request<RouteBody>) -> A + Send + 'p,
        A: Future<Output = RouteResponse> + Send + 'p,
    {
        if self.pipeline.has_exchange {
            future::Either::Left(self.dispatch_plan::<true, _, _>(handler))
        } else {
            future::Either::Right(self.dispatch_plan::<false, _, _>(handler))
        }
    }

    pub(super) async fn dispatch_plan<const EXCHANGE: bool, F, A>(
        self,
        handler: F,
    ) -> RouteResponse
    where
        F: FnOnce(Request<RouteBody>) -> A + Send + 'p,
        A: Future<Output = RouteResponse> + Send + 'p,
    {
        let Self { pipeline, mut head, mut body, mut state, next, context } =
            self;
        let mut reply = ReplyHooks::default();
        let (response, scope) = 'finish: {
            for step in &pipeline.before[next..] {
                let decision = match &step.action {
                    Action::Head(action) => action(&mut head, &mut state),
                    Action::ContextHead(action) => {
                        action(&mut head, &mut state, context)
                    }
                    Action::ContextRequest(action) => {
                        let response = match action
                            .call_in(Request::from_parts(head, body), context)
                            .await
                        {
                            Ok(response) => response,
                            Err(never) => match never {},
                        };
                        break 'finish (response, step.scope);
                    }
                    Action::AsyncHead(action) => {
                        action.call(&mut head, &mut state).await
                    }
                    Action::Exchange(action) => {
                        if EXCHANGE {
                            action.call(&mut head, &mut reply).await
                        } else {
                            unreachable!(
                                "exchange action in a plan without exchange"
                            )
                        }
                    }
                    Action::Request(action) => {
                        let decision =
                            match action(Request::from_parts(head, body)) {
                                DispatchCall::Ready(decision) => decision,
                                DispatchCall::Pending(work) => work.await,
                            };
                        match decision {
                            ServiceResult::Handled(response) => {
                                break 'finish (response, step.scope);
                            }
                            ServiceResult::Unhandled(request) => {
                                (head, body) = request.into_parts();
                                continue;
                            }
                        }
                    }
                };
                if let ControlFlow::Break(response) = decision {
                    break 'finish (response, step.scope);
                }
            }
            let response =
                future::lazy(move |_| handler(Request::from_parts(head, body)))
                    .flatten()
                    .await;
            (response, pipeline.complete)
        };
        let response =
            if EXCHANGE { reply.apply(response).await } else { response };
        pipeline.finish(response, state, scope)
    }
}
