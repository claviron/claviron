//! Redirects for the selected scope; templates are parsed during preparation.

use super::*;
use crate::response::{HostResponse, RequestRejection};
use claviron_http::config::response::RedirectConfig;
use http::{HeaderValue, StatusCode, Uri};

pub struct RedirectLayer {
    status: StatusCode,
    target: Target,
}

impl RedirectLayer {
    pub fn new(config: &RedirectConfig) -> Self {
        Self {
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
    fn prepare(
        self,
        layer: &mut HostLayerBuilder<S, R>,
    ) -> crate::Result<()> {
        layer.before(move |head, _| {
            let reply = match self.target.render(&head.uri) {
                Ok(location) => {
                    HostResponse::Redirect { status: self.status, location }
                }
                Err(_) => RequestRejection::InvalidRedirect.into(),
            };
            ControlFlow::Break(reply.into())
        });
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../tests/layers/redirect.rs"]
mod tests;
