//! Declared CORS policy: early response context and post-quota admission.

use std::sync::Arc;

use super::*;
use claviron_http::config::cors::{CorsConfig, CorsPolicy};
use http::HeaderValue;

mod rules;

#[derive(Default)]
pub struct CorsState {
    origin: Option<HeaderValue>,
    denied: bool,
}

impl CorsState {
    pub fn accepted_origin(&self) -> Option<&HeaderValue> {
        self.origin.as_ref()
    }
}

impl AsMut<CorsState> for CorsState {
    fn as_mut(&mut self) -> &mut Self {
        self
    }
}

struct PreparedCors {
    config: CorsConfig,
    patterns: regex::RegexSet,
}

impl PreparedCors {
    fn allows(
        &self,
        origin: &HeaderValue,
    ) -> bool {
        let Ok(origin) = origin.to_str() else {
            return false;
        };
        match self.config.policy {
            CorsPolicy::Disabled => false,
            CorsPolicy::Permissive => true,
            CorsPolicy::Allowlist => {
                self.config.origins.iter().any(|allowed| allowed == origin)
                    || self.patterns.is_match(origin)
            }
        }
    }
}

/// Boot-compiled CORS policy. An absent declaration creates no layer or state.
/// Explicit `disabled` retains its Origin-refusal policy.
pub struct CorsLayer {
    policy: Arc<PreparedCors>,
}

impl CorsLayer {
    pub fn new(config: CorsConfig) -> crate::Result<Self> {
        let patterns = if config.policy == CorsPolicy::Allowlist {
            regex::RegexSet::new(
                config.origins.iter().filter(|origin| origin.starts_with('^')),
            )
            .map_err(|error| {
                crate::Error::Invalid(format!(
                    "invalid CORS origin pattern: {error}"
                ))
            })?
        } else {
            regex::RegexSet::empty()
        };
        Ok(Self { policy: Arc::new(PreparedCors { config, patterns }) })
    }

    /// Register response ownership before defenses, but decide preflight and
    /// refusals after quota. Both actions share one request-local CorsState.
    pub(crate) fn split<S: AsMut<CorsState>>(
        self
    ) -> (impl HostLayer<S>, impl HostLayer<S>) {
        (CorsCapture(self.policy), CorsAdmission)
    }
}

struct CorsCapture(Arc<PreparedCors>);
struct CorsAdmission;

impl<S: AsMut<CorsState>> HostLayer<S> for CorsCapture {
    fn prepare(
        self,
        layer: &mut HostLayerBuilder<S>,
    ) -> crate::Result<()> {
        let policy = self.0.clone();
        layer.before(move |head, state| {
            let state = state.as_mut();
            state.origin = None;
            state.denied = false;
            if let Some(origin) = head.headers.get(http::header::ORIGIN) {
                if policy.allows(origin) {
                    state.origin = Some(origin.clone());
                } else {
                    state.denied = true;
                }
            }
            ControlFlow::Continue(())
        });
        layer.after(move |head, state| {
            rules::finish(
                &self.0.config,
                state.as_mut().origin.as_ref(),
                &mut head.headers,
            );
        });
        Ok(())
    }
}

impl<S: AsMut<CorsState>> HostLayer<S> for CorsAdmission {
    fn prepare(
        self,
        layer: &mut HostLayerBuilder<S>,
    ) -> crate::Result<()> {
        layer.before(|head, state| {
            match rules::check(&head.method, &head.headers, state.as_mut()) {
                Some(reply) => ControlFlow::Break(reply),
                None => ControlFlow::Continue(()),
            }
        });
        Ok(())
    }
}

impl<S: AsMut<CorsState>> HostLayer<S> for CorsLayer {
    fn prepare(
        self,
        layer: &mut HostLayerBuilder<S>,
    ) -> crate::Result<()> {
        let (capture, admission) = self.split();
        capture.prepare(layer)?;
        admission.prepare(layer)
    }
}

#[cfg(test)]
#[path = "../../../tests/pipeline/layers/cors.rs"]
mod tests;
