//! Configured execution owners, independent of route controls and hooks.

use std::sync::Arc;

use claviron_http::{
    config::ListenerConfig, ext::PathPrefix, policy::HttpPolicy,
};
use claviron_runtime::{
    AppState,
    registry::{Error, Result},
};
use claviron_types::Host;
use serde::de::DeserializeOwned;

use super::{DispatchStep, RouteProtocol};
use crate::HostConfig;

/// An application, embedded runtime or remote execution owner.
/// Internal Static/TryFiles operations implement only `DispatchStep`.
/// Register this owner through `Backend`, so its metadata contract is prepared
/// with the host plan rather than rediscovered during request execution.
pub trait BackendHandler: DispatchStep {
    /// Declared once during preparation. There is no implicit identity export
    /// or response-lifetime policy for a newly registered backend.
    fn contract(&self) -> BackendContract;
}

/// Request metadata exported at the application boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendIdentity {
    /// Keep the request unchanged; native layers can borrow HostContext.
    None,
    /// Export the prepared policy, connection and selected hostname.
    Request,
}

/// Who completes a streaming response when execution is draining.
#[derive(Clone)]
pub enum BackendReply {
    /// The execution owner cooperatively completes its own output.
    Owner,
    /// End this edge's external SSE body on the supplied owner's shutdown.
    /// Finite responses retain their normal completion and drain.
    ExternalEventStream(tokio_util::sync::CancellationToken),
}

/// Mandatory cold declaration of the request and response handoff.
#[derive(Clone)]
pub struct BackendContract {
    pub identity: BackendIdentity,
    pub reply: BackendReply,
}

/// Effective input borrowed while preparing one listener/host/prefix backend.
/// No registry lookup or configuration decoding is performed by dispatch.
pub struct BackendContext<'a> {
    pub state: &'a AppState,
    pub host: &'a Host,
    pub host_config: &'a HostConfig,
    pub route: &'a HostConfig,
    pub prefix: &'a PathPrefix,
    pub policy: HttpPolicy,
    pub protocol: Option<RouteProtocol>,
    pub listener: Option<&'a ListenerConfig>,
}

/// Compile one backend directive into its ordered execution owners.
/// Files and TryFiles precede these owners; native file fallback follows them.
/// An inactive declaration returns no owners and installs no request action.
pub trait BackendFactory: Send + Sync {
    fn name(&self) -> &'static str;
    fn prepare(
        &self,
        context: &BackendContext<'_>,
    ) -> Result<Vec<Arc<dyn BackendHandler>>>;
}

/// Decode a backend's configuration once, then prepare its concrete owners.
/// An absent or null directive prepares no owner.
pub fn factory<C, F>(
    name: &'static str,
    prepare: F,
) -> Arc<dyn BackendFactory>
where
    C: DeserializeOwned + 'static,
    F: Fn(&BackendContext<'_>, C) -> Result<Vec<Arc<dyn BackendHandler>>>
        + Send
        + Sync
        + 'static,
{
    Arc::new(TypedFactory {
        name,
        prepare,
        config: std::marker::PhantomData::<fn() -> C>,
    })
}

struct TypedFactory<C, F> {
    name: &'static str,
    prepare: F,
    config: std::marker::PhantomData<fn() -> C>,
}

impl<C, F> BackendFactory for TypedFactory<C, F>
where
    C: DeserializeOwned,
    F: Fn(&BackendContext<'_>, C) -> Result<Vec<Arc<dyn BackendHandler>>>
        + Send
        + Sync,
{
    fn name(&self) -> &'static str {
        self.name
    }
    fn prepare(
        &self,
        context: &BackendContext<'_>,
    ) -> Result<Vec<Arc<dyn BackendHandler>>> {
        let Some(source) = context
            .route
            .extensions
            .get(self.name)
            .filter(|source| !source.is_null())
        else {
            return Ok(Vec::new());
        };
        let config =
            serde_yaml::from_value(source.clone()).map_err(|error| {
                Error::msg(format!(
                    "backend '{}' at '{}': {error}",
                    self.name, context.prefix
                ))
            })?;
        (self.prepare)(context, config)
    }
}
