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

impl<S, R> PreparedResponse<'_, S, R> {
    /// Apply the armed response actions once, without collecting its body.
    pub fn finish(self, response: RouteResponse) -> RouteResponse {
        self.pipeline.finish(response, self.state, self.scope)
    }
}
