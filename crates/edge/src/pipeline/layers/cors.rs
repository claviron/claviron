//! CORS admission and response shaping, installed only for a declared policy.

use claviron_http::config::cors::{CorsConfig, CorsPolicy};
use http::HeaderValue;
use super::*;

mod rules;

#[derive(Default)]
pub struct CorsState {
    origin: Option<HeaderValue>,
}

impl CorsState {
    pub fn accepted_origin(&self) -> Option<&HeaderValue> { self.origin.as_ref() }
}

impl AsMut<CorsState> for CorsState {
    fn as_mut(&mut self) -> &mut Self { self }
}

pub struct CorsLayer {
    config: CorsConfig,
}

impl CorsLayer {
    pub fn new(config: CorsConfig) -> Self { Self { config } }
}

impl<S: AsMut<CorsState>> HostLayer<S> for CorsLayer {
    fn prepare(self, layer: &mut HostLayerBuilder<S>) -> crate::Result<()> {
        if self.config.policy == CorsPolicy::Disabled {
            layer.before(|head, _| {
                if head.headers.get(http::header::ORIGIN).and_then(|value| value.to_str().ok()).is_some() {
                    ControlFlow::Break(claviron_http::err_status(http::StatusCode::FORBIDDEN))
                } else { ControlFlow::Continue(()) }
            });
            return Ok(());
        }
        let config = self.config;
        match &config.policy {
            CorsPolicy::Permissive => register_check(layer, |_| true),
            CorsPolicy::Allowlist => {
                let matcher = regex::RegexSet::new(&config.origins)
                    .map_err(|error| crate::Error::Invalid(format!("invalid CORS origin pattern: {error}")))?;
                let origins = config.origins.clone();
                register_check(layer, move |origin| {
                    origins.iter().any(|allowed| allowed == origin) || matcher.is_match(origin)
                });
            }
            CorsPolicy::Disabled => unreachable!("disabled policy was prepared separately"),
        }
        layer.after(move |head, state| {
            rules::finish(&config, state.as_mut().origin.as_ref(), &mut head.headers);
        });
        Ok(())
    }
}

fn register_check<S: AsMut<CorsState>>(
    layer: &mut HostLayerBuilder<S>,
    allows: impl Fn(&str) -> bool + Send + Sync + 'static,
) {
    layer.before(move |head, state| {
        let (reply, origin) = rules::check(&head.method, &head.headers, &allows);
        state.as_mut().origin = origin;
        match reply {
            Some(reply) => ControlFlow::Break(reply),
            None => ControlFlow::Continue(()),
        }
    });
}
