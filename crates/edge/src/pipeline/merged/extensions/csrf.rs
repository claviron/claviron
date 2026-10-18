//! Optional CSRF route layer. Session/key ownership remains with the host.

use std::sync::Arc;

use axum::{Router, extract::Request, middleware::Next, response::Response};
use serde::Deserialize;

use super::{RouteContext, RouteEffect, RouteExtension, RouteLayer, directive};

/// Session-aware verifier prepared by the application's auth provider.
///
/// Implementations validate the credential carrier and session scope and return
/// the rejection response directly. They must not treat AJAX as authentication.
pub trait CsrfVerifier: Send + Sync {
    fn verify(
        &self,
        request: &Request,
    ) -> Result<(), Response>;
}

/// Registers `csrf: bool`; enabled routes validate unsafe methods before any
/// Rust contribution or configured downstream handler executes.
pub struct Csrf {
    verifier: Arc<dyn CsrfVerifier>,
}

impl Csrf {
    pub fn new(verifier: Arc<dyn CsrfVerifier>) -> Self {
        Self { verifier }
    }
}

#[derive(Deserialize)]
#[serde(transparent)]
struct Config(bool);

struct Layer {
    enabled: bool,
    verifier: Arc<dyn CsrfVerifier>,
}

impl RouteLayer for Layer {
    fn apply(
        &self,
        router: Router,
    ) -> Router {
        if !self.enabled {
            return router;
        }
        let verifier = self.verifier.clone();
        router.layer(axum::middleware::from_fn(
            move |request: Request, next: Next| {
                let verifier = verifier.clone();
                async move {
                    if !matches!(
                        request.method().as_str(),
                        "GET" | "HEAD" | "OPTIONS"
                    ) && let Err(response) = verifier.verify(&request)
                    {
                        return response;
                    }
                    next.run(request).await
                }
            },
        ))
    }
}

impl RouteExtension<crate::config::HostConfig> for Csrf {
    fn name(&self) -> &'static str {
        "csrf"
    }

    fn resolve(
        &self,
        context: &RouteContext<'_>,
        source: &serde_yaml::Value,
    ) -> claviron_runtime::registry::Result<RouteEffect> {
        let verifier = self.verifier.clone();
        directive::<Config, _>(self.name(), move |_, config| {
            Ok(RouteEffect::Layer(Arc::new(Layer {
                enabled: config.0,
                verifier: verifier.clone(),
            })))
        })
        .resolve(context, source)
    }
}
