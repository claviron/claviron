//! Explicit prefix redirects; templates are parsed only during preparation.

use claviron_http::ext::PathPrefix;
use http::{HeaderValue, StatusCode, Uri};
use crate::config::RedirectConfig;
use crate::service::{HostResponse, RequestRejection};
use super::*;

pub struct RedirectLayer {
    path: PathPrefix,
    status: StatusCode,
    target: Target,
}

impl RedirectLayer {
    pub fn new(config: &RedirectConfig) -> Self {
        Self {
            path: config.path.clone(),
            status: config.code.status(),
            target: Target::compile(config.to.as_str()),
        }
    }
}

enum Target {
    Static(HeaderValue),
    Template(Box<[Part]>),
}

enum Part {
    Literal(Box<str>),
    Path,
    Query,
}

impl Target {
    fn compile(template: &str) -> Self {
        let mut parts = Vec::new();
        let mut remaining = template;
        let mut literal_start = 0;
        // Only declared tokens are substituted. Request data is appended once,
        // never fed through a second replacement pass as template syntax.
        while let Some(offset) = remaining[literal_start..].find('$') {
            let index = literal_start + offset;
            let rest = &remaining[index..];
            let token = if rest.starts_with("$uri") {
                Some((4, Part::Path))
            } else if rest.starts_with("$args") {
                Some((5, Part::Query))
            } else if rest.starts_with("$query") {
                Some((6, Part::Query))
            } else {
                None
            };
            if let Some((len, part)) = token {
                if index > 0 {
                    parts.push(Part::Literal(remaining[..index].into()));
                }
                parts.push(part);
                remaining = &remaining[index + len..];
                literal_start = 0;
            } else {
                literal_start = index + 1;
            }
        }
        if parts.is_empty() {
            return Self::Static(HeaderValue::from_str(template).expect(
                "LocationTemplate validated the header at construction",
            ));
        }
        if !remaining.is_empty() {
            parts.push(Part::Literal(remaining.into()));
        }
        Self::Template(parts.into_boxed_slice())
    }

    fn render(
        &self,
        uri: &Uri,
    ) -> Result<HeaderValue, http::header::InvalidHeaderValue> {
        match self {
            Self::Static(location) => Ok(location.clone()),
            Self::Template(parts) => {
                let value: String = parts
                    .iter()
                    .map(|part| match part {
                        Part::Literal(text) => text.as_ref(),
                        Part::Path => uri.path(),
                        Part::Query => uri.query().unwrap_or_default(),
                    })
                    .collect();
                HeaderValue::from_str(&value)
            }
        }
    }
}

impl<S, R: From<HostResponse>> HostLayer<S, R> for RedirectLayer {
    fn prepare(self, layer: &mut HostLayerBuilder<S, R>) -> crate::Result<()> {
        layer.before(move |head, _| {
            if !self.path.has_path_segment(head.uri.path()) {
                return ControlFlow::Continue(());
            }
            let reply = match self.target.render(&head.uri) {
                Ok(location) => HostResponse::Redirect { status: self.status, location },
                Err(_) => RequestRejection::InvalidRedirect.into(),
            };
            ControlFlow::Break(reply.into())
        });
        Ok(())
    }
}

/// HTTPS authority is prepared once for the selected host/listener binding.
pub struct HttpsRedirectLayer {
    prefix: Box<str>,
}

impl HttpsRedirectLayer {
    pub fn new(authority: &http::uri::Authority, port: std::num::NonZeroU16) -> Self {
        let host = authority.host();
        let prefix = if port.get() == 443 {
            format!("https://{host}")
        } else {
            format!("https://{host}:{port}")
        };
        Self { prefix: prefix.into_boxed_str() }
    }
}

impl<S, R: From<HostResponse>> HostLayer<S, R> for HttpsRedirectLayer {
    fn prepare(self, layer: &mut HostLayerBuilder<S, R>) -> crate::Result<()> {
        layer.before(move |head, _| {
            let path = head.uri.path_and_query().map(|path| path.as_str()).unwrap_or("/");
            let mut location = String::with_capacity(self.prefix.len() + path.len());
            location.push_str(&self.prefix);
            location.push_str(path);
            let reply = match HeaderValue::from_maybe_shared(bytes::Bytes::from(location)) {
                Ok(location) => HostResponse::Redirect { status: StatusCode::PERMANENT_REDIRECT, location },
                Err(_) => RequestRejection::InvalidRedirect.into(),
            };
            ControlFlow::Break(reply.into())
        });
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../../tests/pipeline/layers/redirect.rs"]
mod tests;
