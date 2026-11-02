use super::*;

impl<S, R> Default for HostLayerBuilder<S, R> {
    fn default() -> Self {
        Self { before: Vec::new(), after: Vec::new(), body: Vec::new() }
    }
}

impl<S, R> HostLayerBuilder<S, R> {
    /// Read accepted facts only for a layer that explicitly requests them.
    pub fn before_context(
        &mut self,
        action: impl for<'a> Fn(
            &mut RequestParts,
            &mut S,
            Option<HostContext<'a>>,
        ) -> ControlFlow<R>
        + Send
        + Sync
        + 'static,
    ) -> &mut Self {
        self.before.push(Action::ContextHead(Box::new(action)));
        self
    }
    /// Register a synchronous head-only action. Break replies; Continue passes.
    pub fn before(
        &mut self,
        action: impl Fn(&mut RequestParts, &mut S) -> ControlFlow<R>
        + Send
        + Sync
        + 'static,
    ) -> &mut Self {
        self.before.push(Action::Head(Box::new(action)));
        self
    }
}

impl<S> HostLayerBuilder<S> {
    pub fn request_context(
        &mut self,
        owner: impl ContextHandler,
    ) -> &mut Self {
        self.before.push(Action::ContextRequest(Box::new(owner)));
        self
    }
    /// Register an active wait over the head/state. The body stays with the
    /// driver. No callback or future exists when this layer is omitted.
    pub fn before_async(
        &mut self,
        action: impl AsyncHeadAction<S> + 'static,
    ) -> &mut Self {
        self.before.push(Action::AsyncHead(Box::new(action)));
        self
    }

    /// Transfer the request to one prepared asynchronous owner. Unhandled must
    /// return that same request; a response status never means fallthrough.
    /// A ready miss needs no future allocation. Only active work is erased by
    /// the owner's DispatchFuture; this callback is allocated at preparation.
    pub fn request(
        &mut self,
        action: impl Fn(Request<RouteBody>) -> DispatchFuture
        + Send
        + Sync
        + 'static,
    ) -> &mut Self {
        self.before.push(Action::Request(Box::new(action)));
        self
    }
}

impl<S, R> HostLayerBuilder<S, R> {
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

#[cfg(feature = "rate-limit")]
struct UnitHead(Box<dyn AsyncHeadAction<()>>);

#[cfg(feature = "rate-limit")]
impl<S: AsMut<()>> AsyncHeadAction<S> for UnitHead {
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
    > {
        self.0.call(head, state.as_mut())
    }
}

#[cfg(feature = "rate-limit")]
impl<R: 'static> HostPipelineBuilder<(), R> {
    /// Preserve state-independent extension actions in a typed host plan.
    pub(crate) fn with_state<S: AsMut<()> + 'static>(
        self
    ) -> HostPipelineBuilder<S, R> {
        let layers = self
            .layers
            .into_iter()
            .map(|layer| HostLayerBuilder {
                before: layer
                    .before
                    .into_iter()
                    .map(|action| match action {
                        Action::Head(action) => Action::Head(Box::new(
                            move |head, state: &mut S| {
                                action(head, state.as_mut())
                            },
                        )),
                        Action::ContextHead(action) => Action::ContextHead(
                            Box::new(move |head, state: &mut S, context| {
                                action(head, state.as_mut(), context)
                            }),
                        ),
                        Action::Request(action) => Action::Request(action),
                        Action::ContextRequest(action) => {
                            Action::ContextRequest(action)
                        }
                        Action::AsyncHead(action) => {
                            Action::AsyncHead(Box::new(UnitHead(action)))
                        }
                    })
                    .collect(),
                after: layer
                    .after
                    .into_iter()
                    .map(|action| {
                        Box::new(
                            move |head: &mut ResponseParts, state: &mut S| {
                                action(head, state.as_mut())
                            },
                        ) as ResponseAction<S>
                    })
                    .collect(),
                body: layer
                    .body
                    .into_iter()
                    .map(|action| {
                        Box::new(move |body: RouteBody, state: &mut S| {
                            action(body, state.as_mut())
                        }) as BodyAction<S>
                    })
                    .collect(),
            })
            .collect();
        HostPipelineBuilder { layers }
    }
}

impl<S, R> HostPipelineBuilder<S, R> {
    #[cfg(feature = "rate-limit")]
    pub(crate) fn prepend_layer(
        &mut self,
        layer: impl HostLayer<S, R>,
    ) -> crate::Result<()> {
        let mut prepared = HostLayerBuilder::default();
        layer.prepare(&mut prepared)?;
        self.layers.insert(0, prepared);
        Ok(())
    }
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

    pub(crate) fn is_empty(&self) -> bool {
        self.layers.iter().all(|layer| {
            layer.before.is_empty()
                && layer.after.is_empty()
                && layer.body.is_empty()
        })
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
                    .map(|action| PreparedAction { action, scope }),
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

impl<S, R> From<HostLayerBuilder<S, R>> for HostPipelineBuilder<S, R> {
    fn from(layer: HostLayerBuilder<S, R>) -> Self {
        Self { layers: vec![layer] }
    }
}
