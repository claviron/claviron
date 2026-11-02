//! Prepared rate admission over the selected application's existing stores.

use std::sync::Arc;

use claviron_http::ext::PathPrefix;
use claviron_http::rate_limit::{RequestHead, Snapshot};

use super::*;
use crate::service::{HostResponse, RequestRejection};

mod prepare;

pub use prepare::HostQuotas;
pub(crate) use prepare::QuotaRegistrations;
use prepare::RegisteredQuota;

/// The committed admission snapshot, retained without a second store read.
#[derive(Default)]
pub struct QuotaState {
    snapshot: Option<Snapshot>,
}

impl QuotaState {
    pub fn snapshot(&self) -> Option<Snapshot> {
        self.snapshot
    }
}

impl AsMut<QuotaState> for QuotaState {
    fn as_mut(&mut self) -> &mut QuotaState {
        self
    }
}

/// A cold-prepared layer sharing the host's existing quota authority.
///
/// Install after identity and access checks. Key providers see only the parsed
/// head and its typed extensions. Preparing another layer shares debt; it does
/// not open or reset a store. Unconfigured hosts have no quota plan.
pub struct QuotaPolicy {
    rules: Arc<[(PathPrefix, RegisteredQuota)]>,
}

impl QuotaPolicy {
    pub fn new(quotas: &HostQuotas) -> Self {
        Self::from_rules(quotas.rules.clone())
    }

    pub(crate) fn from_rules(
        rules: Arc<[(PathPrefix, RegisteredQuota)]>
    ) -> Self {
        Self { rules }
    }

    fn register<S: AsMut<QuotaState>, R: From<HostResponse>>(
        self,
        layer: &mut HostLayerBuilder<S, R>,
    ) {
        if self.rules.is_empty() {
            return;
        }
        // Choose the uniform path once, rather than testing it per request.
        if self.rules.len() == 1 && self.rules[0].0.as_str() == "/" {
            let quota = self.rules[0].1.clone();
            layer.before(move |head, state| {
                record(quota.check(&request_head(head)), state.as_mut())
            });
        } else {
            layer.before(move |head, state| {
                record(self.check_scoped(head), state.as_mut())
            });
        }
    }

    fn check_scoped(
        &self,
        head: &RequestParts,
    ) -> Result<Option<Snapshot>, HostResponse> {
        let paths = super::acl::comparison_paths(head.uri.path())?;
        let selected = self.select(&paths.separated);
        if let Some(literal) = &paths.literal
            && selected.map(|rule| rule as *const _)
                != self.select(literal).map(|rule| rule as *const _)
        {
            return Err(RequestRejection::AmbiguousPath.into());
        }
        match selected {
            Some((_, quota)) => quota.check(&request_head(head)),
            None => Ok(None),
        }
    }

    fn select(
        &self,
        path: &str,
    ) -> Option<&(PathPrefix, RegisteredQuota)> {
        self.rules.iter().find(|(prefix, _)| prefix.has_path_segment(path))
    }
}

fn request_head(head: &RequestParts) -> RequestHead<'_> {
    RequestHead {
        method: &head.method,
        uri: &head.uri,
        headers: &head.headers,
        extensions: &head.extensions,
    }
}

fn record<R: From<HostResponse>>(
    result: Result<Option<Snapshot>, HostResponse>,
    state: &mut QuotaState,
) -> ControlFlow<R> {
    match result {
        Ok(snapshot) => {
            state.snapshot = snapshot;
            ControlFlow::Continue(())
        }
        Err(reply) => ControlFlow::Break(reply.into()),
    }
}

impl<S: AsMut<QuotaState>, R: From<HostResponse>> HostLayer<S, R> for QuotaPolicy {
    fn prepare(
        self,
        layer: &mut HostLayerBuilder<S, R>,
    ) -> crate::Result<()> {
        self.register(layer);
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../../tests/pipeline/host/quota.rs"]
mod tests;

