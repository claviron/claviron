//! Named listener generations, independent of their request owner.

use std::sync::Arc;

use async_trait::async_trait;
use claviron_http::config::ListenerConfig;
use claviron_runtime::{
    AppState, ExecutionSpec, Reloadable, Resolve, RunContext, Runnable,
    RunnableInstance, ServiceStartPolicy, execution::execution_spec, registry,
};

/// An application-owned adapter supplies a listener's prepared inputs.
/// Transport owns the Runnable; the adapter owns selection and publication.
#[async_trait]
pub trait Adapter: Send + Sync + 'static {
    type Deps: Resolve<AppState>;

    fn config(&self) -> Arc<ListenerConfig>;

    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        None
    }

    async fn serve(
        self: Arc<Self>,
        deps: Self::Deps,
        state: AppState,
        context: RunContext,
    ) -> registry::Result<()>;
}

/// One managed listener. Edge and single-application servers supply adapters
/// to this same generation and execution boundary.
pub struct Listener<A>(Arc<A>);

impl<A: Adapter> Listener<A> {
    pub fn new(adapter: A) -> Self {
        Self(Arc::new(adapter))
    }

    pub fn adapter(&self) -> Arc<A> {
        self.0.clone()
    }

    pub fn instance(self: &Arc<Self>) -> RunnableInstance<AppState> {
        let name: Arc<str> = format!("Listener:{}", self.0.config().id).into();
        RunnableInstance::new(name, self.clone())
    }
}

impl<A> std::ops::Deref for Listener<A> {
    type Target = A;
    fn deref(&self) -> &A {
        &self.0
    }
}

#[async_trait]
impl<A: Adapter> Runnable<AppState, A::Deps> for Listener<A> {
    fn execution_spec(
        &self,
        _: &AppState,
    ) -> ExecutionSpec {
        execution_spec(self.0.config().execution)
    }

    fn start_policy(
        &self,
        state: &AppState,
    ) -> ServiceStartPolicy {
        if state.is_master() {
            ServiceStartPolicy::Unavailable
        } else {
            ServiceStartPolicy::Automatic
        }
    }

    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        self.0.as_reloadable()
    }

    async fn run(
        self: Arc<Self>,
        deps: A::Deps,
        state: AppState,
        context: RunContext,
    ) -> registry::Result<()> {
        self.0.clone().serve(deps, state, context).await
    }
}
