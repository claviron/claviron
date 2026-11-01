//! Prepared access-record selection and request/body lifetime ownership.

use std::sync::Arc;

use claviron_http::ext::{ConnCtx, HostKey};
use http::{Method, StatusCode, Version};

use super::*;
use crate::access::{AccessGuard, AccessLogTarget};

/// One optional access record, owned until an early reply or stream drop/end.
#[derive(Default)]
pub struct AccessLogState {
    guard: Option<AccessGuard>,
}

impl AsMut<AccessLogState> for AccessLogState {
    fn as_mut(&mut self) -> &mut AccessLogState {
        self
    }
}

impl AccessLogState {
    /// Mark the actual capacity admission point, not merely head preparation.
    pub fn admitted(&mut self) {
        if let Some(guard) = self.guard.as_mut() {
            guard.admitted();
        }
    }

    /// Complete an edge-generated empty reply and release the record.
    pub fn replied(
        self,
        status: StatusCode,
    ) {
        if let Some(guard) = self.guard {
            guard.replied(status);
        }
    }

    /// Transfer the record to the response stream without collecting it.
    pub fn finish(
        self,
        response: RouteResponse,
    ) -> RouteResponse {
        match self.guard {
            Some(guard) => guard.wrap(response),
            None => response,
        }
    }
}

/// Access-record actions prepared after the process's logging capability boots.
///
/// `off` and an absent producer capability return `None`. A named/default channel
/// is resolved at request start so logging reloads still affect existing
/// connections. The layer retains no HostConfig or application generation.
/// Install after identity and outside response shaping/body guards. Call the
/// state's admitted method only when execution actually acquires capacity.
#[derive(Clone)]
pub struct AccessLogLayer {
    target: AccessLogTarget,
    canonical: HostKey,
}

impl AccessLogLayer {
    pub fn new(
        canonical: HostKey,
        channel: Option<&str>,
    ) -> Option<Self> {
        Some(Self { target: AccessLogTarget::prepare(channel)?, canonical })
    }

    /// Native dispatch can borrow its request without splitting or boxing it.
    pub fn begin<B>(
        &self,
        request: &Request<B>,
        connection: &Arc<ConnCtx>,
    ) -> AccessLogState {
        self.start(
            request.method(),
            request.uri().path(),
            request.version(),
            connection,
        )
    }

    fn start(
        &self,
        method: &Method,
        path: &str,
        version: Version,
        connection: &Arc<ConnCtx>,
    ) -> AccessLogState {
        let Some(channel) = self.target.channel() else {
            return AccessLogState::default();
        };
        AccessLogState {
            guard: Some(self.target.begin(
                channel,
                &self.canonical.0.to_cow_str(),
                method,
                path,
                version,
                connection,
            )),
        }
    }
}

impl<S> HostLayer<S> for AccessLogLayer
where
    S: AsRef<IdentityState> + AsMut<AccessLogState>,
{
    fn prepare(
        self,
        layer: &mut HostLayerBuilder<S>,
    ) -> crate::Result<()> {
        layer.before(move |head, state| {
            let record = state
                .as_ref()
                .connection()
                .map(|connection| {
                    self.start(
                        &head.method,
                        head.uri.path(),
                        head.version,
                        connection,
                    )
                })
                .unwrap_or_default();
            *state.as_mut() = record;
            ControlFlow::Continue(())
        });
        layer.after(|head, state| {
            if let Some(guard) = state.as_mut().guard.as_mut() {
                guard.record_head(head.status, &head.headers);
            }
        });
        layer.body(|body, state| match state.as_mut().guard.take() {
            Some(guard) => guard.wrap_body(body),
            None => body,
        });
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../../tests/pipeline/host/access_log.rs"]
mod tests;
