use super::*;

impl<S, R> HostPipeline<S, R> {
    pub(super) fn finish(
        &self,
        response: RouteResponse,
        mut state: S,
        scope: ReplyScope,
    ) -> RouteResponse {
        if scope.after == 0 && scope.body == 0 {
            return response;
        }
        let (mut head, mut body) = response.into_parts();
        for action in self.after[..scope.after].iter().rev() {
            action(&mut head, &mut state);
        }
        for action in self.body[..scope.body].iter().rev() {
            body = action(body, &mut state);
        }
        Response::from_parts(head, body)
    }
}

impl<S, R> PreparedReply<'_, S, R> {
    /// Convert a typed early decision at the native response boundary.
    pub fn finish_with(
        self,
        reply: impl FnOnce(R) -> RouteResponse,
    ) -> RouteResponse {
        self.pipeline.finish(reply(self.response), self.state, self.scope)
    }
}

impl<S> PreparedReply<'_, S> {
    /// Apply only the response/body actions reached before this early reply.
    pub fn finish(self) -> RouteResponse {
        self.pipeline.finish(self.response, self.state, self.scope)
    }
}

type ReplyFuture =
    std::pin::Pin<Box<dyn std::future::Future<Output = RouteResponse> + Send>>;

enum ReplyWork {
    Head(Box<dyn FnOnce(&mut http::HeaderMap) + Send>),
    Response(Box<dyn FnOnce(RouteResponse) -> ReplyFuture + Send>),
}

struct ReplyHook {
    work: ReplyWork,
    previous: Option<Box<Self>>,
}

/// Request-owned response work, applied in reverse registration order before
/// the prepared response layers. An empty stack allocates nothing.
#[derive(Default)]
pub struct ReplyHooks(Option<Box<ReplyHook>>);

impl ReplyHooks {
    pub fn headers(
        &mut self,
        action: impl FnOnce(&mut http::HeaderMap) + Send + 'static,
    ) {
        self.push(ReplyWork::Head(Box::new(action)));
    }

    pub fn response<F>(
        &mut self,
        action: impl FnOnce(RouteResponse) -> F + Send + 'static,
    ) where
        F: std::future::Future<Output = RouteResponse> + Send + 'static,
    {
        self.push(ReplyWork::Response(Box::new(move |response| {
            Box::pin(action(response))
        })));
    }

    fn push(
        &mut self,
        work: ReplyWork,
    ) {
        self.0 = Some(Box::new(ReplyHook { work, previous: self.0.take() }));
    }

    pub(super) async fn apply(
        &mut self,
        mut response: RouteResponse,
    ) -> RouteResponse {
        while let Some(hook) = self.0.take() {
            self.0 = hook.previous;
            match hook.work {
                ReplyWork::Head(action) => action(response.headers_mut()),
                ReplyWork::Response(action) => {
                    response = action(response).await
                }
            }
        }
        response
    }
}
