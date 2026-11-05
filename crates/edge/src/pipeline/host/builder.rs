use super::*;

impl<S, R> Default for HostLayerBuilder<S, R> {
    fn default() -> Self {
        Self {
            before: Vec::new(),
            after: Vec::new(),
            body: Vec::new(),
            identity: None,
        }
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

    /// Admit from the request head and optionally register response work.
    pub fn exchange(
        &mut self,
        action: impl ExchangeAction + 'static,
    ) -> &mut Self {
        self.before.push(Action::Exchange(Box::new(action)));
        self
    }

    /// Transfer the request to one prepared asynchronous owner. Unhandled must
    /// return the request to the driver. An owner that explicitly inspected a
    /// buffered payload must restore it before returning. A response status
    /// never means fallthrough.
    /// A ready miss needs no future allocation. Only active work is erased by
    /// the owner's DispatchCall; this callback is allocated at preparation.
    pub fn request(
        &mut self,
        action: impl Fn(Request<RouteBody>) -> DispatchCall + Send + Sync + 'static,
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
        Self {
            before: Vec::new(),
            after: Vec::new(),
            body: Vec::new(),
            identity: None,
        }
    }
}

struct StateHead<S, F> {
    action: Box<dyn AsyncHeadAction<S>>,
    project: F,
}

impl<S, T, F> AsyncHeadAction<T> for StateHead<S, F>
where
    F: Fn(&mut T) -> &mut S + Send + Sync,
{
    fn call<'a>(
        &'a self,
        head: &'a mut RequestParts,
        state: &'a mut T,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = ControlFlow<RouteResponse>>
                + Send
                + 'a,
        >,
    > {
        self.action.call(head, (self.project)(state))
    }
}

#[cfg(any(feature = "rate-limit", feature = "access-log"))]
impl<R: 'static> HostPipelineBuilder<(), R> {
    /// Preserve state-independent extension actions in a typed host plan.
    pub(crate) fn with_state<S: AsMut<()> + 'static>(
        self
    ) -> HostPipelineBuilder<S, R> {
        self.map_state(AsMut::as_mut)
    }
}

impl<S: 'static, R: 'static> HostPipelineBuilder<S, R> {
    /// Embed existing state in a selected plan without adding a runtime node.
    pub(crate) fn map_state<T: 'static, F>(
        self,
        project: F,
    ) -> HostPipelineBuilder<T, R>
    where
        F: Fn(&mut T) -> &mut S + Copy + Send + Sync + 'static,
    {
        HostPipelineBuilder {
            before: self
                .before
                .into_iter()
                .map(|step| PreparedAction {
                    scope: step.scope,
                    action: match step.action {
                        Action::Head(action) => Action::Head(Box::new(
                            move |head, state: &mut T| {
                                action(head, project(state))
                            },
                        )),
                        Action::ContextHead(action) => Action::ContextHead(
                            Box::new(move |head, state: &mut T, context| {
                                action(head, project(state), context)
                            }),
                        ),
                        Action::Request(action) => Action::Request(action),
                        Action::Exchange(action) => Action::Exchange(action),
                        Action::ContextRequest(action) => {
                            Action::ContextRequest(action)
                        }
                        Action::AsyncHead(action) => {
                            Action::AsyncHead(Box::new(StateHead {
                                action,
                                project,
                            }))
                        }
                    },
                })
                .collect(),
            after: self
                .after
                .into_iter()
                .map(|action| {
                    Box::new(move |head: &mut ResponseParts, state: &mut T| {
                        action(head, project(state))
                    }) as ResponseAction<T>
                })
                .collect(),
            body: self
                .body
                .into_iter()
                .map(|action| {
                    Box::new(move |body: RouteBody, state: &mut T| {
                        action(body, project(state))
                    }) as BodyAction<T>
                })
                .collect(),
            identity: self.identity,
        }
    }
}

impl<S, R> HostPipelineBuilder<S, R> {
    /// Move a builder or a compiled pipeline into this flat plan.
    /// Request/response action order and reached reply scopes are preserved.
    /// Both contributions share the same per-request state and reply type.
    pub fn append(
        &mut self,
        prepared: impl Into<Self>,
    ) -> &mut Self {
        let mut prepared = prepared.into();
        if self.is_empty() {
            *self = prepared;
            return self;
        }
        let offset =
            ReplyScope { after: self.after.len(), body: self.body.len() };
        let identity = prepared.identity.map(NonZeroUsize::get);
        for (index, mut step) in prepared.before.into_iter().enumerate() {
            if identity == Some(index + 1) {
                if self.identity.is_some() {
                    continue;
                }
                self.identity = NonZeroUsize::new(self.before.len() + 1);
            }
            step.scope.after += offset.after;
            step.scope.body += offset.body;
            self.before.push(step);
        }
        self.after.append(&mut prepared.after);
        self.body.append(&mut prepared.body);
        self
    }

    #[cfg(feature = "rate-limit")]
    pub(crate) fn prepend_layer(
        &mut self,
        layer: impl HostLayer<S, R>,
    ) -> crate::Result<()> {
        let mut prefix = Self::new();
        prefix.layer(layer)?;
        prefix.append(std::mem::take(self));
        *self = prefix;
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
        self.push_layer(prepared);
        Ok(self)
    }

    fn push_layer(
        &mut self,
        mut layer: HostLayerBuilder<S, R>,
    ) {
        self.after.append(&mut layer.after);
        self.body.append(&mut layer.body);
        let scope =
            ReplyScope { after: self.after.len(), body: self.body.len() };
        if let Some(policy) = layer.identity
            && self.identity.is_none()
        {
            self.identity = NonZeroUsize::new(self.before.len() + 1);
            let policy = super::super::layers::IdentityPolicy::new(policy);
            self.before.push(PreparedAction {
                action: Action::ContextHead(Box::new(
                    move |head, _, context| {
                        policy.inject(&mut head.extensions, context);
                        ControlFlow::Continue(())
                    },
                )),
                scope,
            });
        }
        self.before.extend(
            layer
                .before
                .into_iter()
                .map(|action| PreparedAction { action, scope }),
        );
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.before.is_empty() && self.after.is_empty() && self.body.is_empty()
    }

    /// Retain active actions without introducing runtime composition nodes.
    pub fn build(self) -> HostPipeline<S, R> {
        let complete =
            ReplyScope { after: self.after.len(), body: self.body.len() };
        let has_exchange = self
            .before
            .iter()
            .any(|step| matches!(step.action, Action::Exchange(_)));
        HostPipeline {
            before: self.before.into_boxed_slice(),
            after: self.after.into_boxed_slice(),
            body: self.body.into_boxed_slice(),
            complete,
            has_exchange,
            identity: self.identity,
        }
    }
}

impl<S, R> From<HostLayerBuilder<S, R>> for HostPipelineBuilder<S, R> {
    fn from(layer: HostLayerBuilder<S, R>) -> Self {
        let mut builder = Self::new();
        builder.push_layer(layer);
        builder
    }
}

impl<S, R> From<HostPipeline<S, R>> for HostPipelineBuilder<S, R> {
    fn from(pipeline: HostPipeline<S, R>) -> Self {
        Self {
            before: pipeline.before.into_vec(),
            after: pipeline.after.into_vec(),
            body: pipeline.body.into_vec(),
            identity: pipeline.identity,
        }
    }
}
