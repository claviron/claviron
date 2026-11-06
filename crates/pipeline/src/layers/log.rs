//! Prepared access-record selection and request/body lifetime ownership.

use std::sync::Arc;

use claviron_types::Host;

use super::*;
use crate::log::{AccessGuard, AccessLogTarget};

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

/// Access-record actions prepared after the process's logging capability boots.
///
/// `off`, an absent producer or an unselected channel returns `None`.
/// Active channel membership remains live across logging reloads. Accepted
/// facts are borrowed; no identity-injection layer or application is retained.
/// Prepare before request checks whose replies must be recorded and outside
/// response shaping/body guards.
#[derive(Clone)]
pub struct AccessLogLayer {
    target: AccessLogTarget,
    canonical: Arc<str>,
}

impl AccessLogLayer {
    pub fn new(
        canonical: Option<&Host>,
        channel: Option<&str>,
    ) -> Option<Self> {
        Some(Self {
            target: AccessLogTarget::prepare(channel)?,
            // A site served without a host table has no name to record.
            canonical: canonical.map_or_else(
                || Arc::from("-"),
                |canonical| canonical.to_cow_str().into(),
            ),
        })
    }
}

impl<S> HostLayer<S> for AccessLogLayer
where
    S: AsMut<AccessLogState>,
{
    fn prepare(
        self,
        layer: &mut HostLayerBuilder<S>,
    ) -> crate::Result<()> {
        layer.before_context(move |head, state, context| {
            state.as_mut().guard = context.and_then(|context| {
                let channel = self.target.channel()?;
                Some(self.target.begin(
                    channel,
                    self.canonical.clone(),
                    head,
                    context.connection,
                ))
            });
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
