use super::*;

impl<S, R> Default for HostLayerBuilder<S, R> {
    fn default() -> Self {
        Self { before: Vec::new(), after: Vec::new(), body: Vec::new() }
    }
}

impl<S, R> HostLayerBuilder<S, R> {
    /// Register a synchronous head-only action. Break replies; Continue passes.
    pub fn before(
        &mut self,
        action: impl Fn(&mut RequestParts, &mut S) -> ControlFlow<R>
        + Send
        + Sync
        + 'static,
    ) -> &mut Self {
        self.before.push(Box::new(action));
        self
    }
}

impl<S> HostLayerBuilder<S> {
    /// Register a response-head action, without moving or borrowing its body.
    ///
    /// Entered response actions run in reverse registration order.
    pub fn after(
        &mut self,
        action: impl Fn(&mut ResponseParts, &mut S) + Send + Sync + 'static,
    ) -> &mut Self {
        self.after.push(Box::new(action));
        self
    }

    /// Register explicit response-body ownership transfer.
    ///
    /// A wrapper may take an owned guard from `S` and retain it until body
    /// completion/drop. Do not collect the body or copy its frames. Entered
    /// wrappers run in reverse registration order; no wrapper is added by
    /// the pipeline itself.
    pub fn body(
        &mut self,
        action: impl Fn(RouteBody, &mut S) -> RouteBody + Send + Sync + 'static,
    ) -> &mut Self {
        self.body.push(Box::new(action));
        self
    }
}

impl<S, R> Default for HostPipelineBuilder<S, R> {
    fn default() -> Self {
        Self { layers: Vec::new() }
    }
}

impl<S, R> HostPipelineBuilder<S, R> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Prepare a layer atomically. Failed preparation leaves this builder alone.
    pub fn layer(
        &mut self,
        layer: impl HostLayer<S, R>,
    ) -> crate::Result<&mut Self> {
        let mut prepared = HostLayerBuilder::default();
        layer.prepare(&mut prepared)?;
        self.layers.push(prepared);
        Ok(self)
    }

    /// Flatten active actions once. Empty/inactive layers leave no runtime node.
    pub fn build(self) -> HostPipeline<S, R> {
        let mut before = Vec::new();
        let mut after = Vec::new();
        let mut body = Vec::new();
        for layer in self.layers {
            after.extend(layer.after);
            body.extend(layer.body);
            let scope = ReplyScope { after: after.len(), body: body.len() };
            before.extend(
                layer
                    .before
                    .into_iter()
                    .map(|action| PreparedHeadAction { action, scope }),
            );
        }
        let complete = ReplyScope { after: after.len(), body: body.len() };
        HostPipeline {
            before: before.into_boxed_slice(),
            after: after.into_boxed_slice(),
            body: body.into_boxed_slice(),
            complete,
        }
    }
}
