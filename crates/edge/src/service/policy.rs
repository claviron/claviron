//! One immutable, boot-compiled host request policy shared by all aliases.

use std::net::IpAddr;
use std::sync::Arc;

use claviron_http::ext::PathPrefix;
use http::{HeaderValue, StatusCode, Uri};

use super::acl::CompiledAcl;
use super::{HostResponse, RequestRejection};
use crate::config::{HostConfig, RedirectConfig};

pub(crate) struct RequestPolicy {
    acl: Option<Arc<CompiledAcl>>,
    route_acl: Option<Arc<CompiledAcl>>,
    redirects: Box<[RedirectRule]>,
}

impl RequestPolicy {
    pub(crate) fn compile(config: &HostConfig) -> Option<Arc<Self>> {
        let policy = Self {
            acl: config.acl().and_then(CompiledAcl::compile),
            route_acl: CompiledAcl::from_entries(
                config.routes.iter().filter_map(|(prefix, route)| {
                    route
                        .route_acl
                        .as_ref()
                        .map(|acl| (prefix.clone(), acl.clone()))
                }),
            ),
            redirects: config
                .redirect
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(RedirectRule::compile)
                .collect(),
        };
        if policy.acl.is_none()
            && policy.route_acl.is_none()
            && policy.redirects.is_empty()
        {
            None
        } else {
            Some(Arc::new(policy))
        }
    }

    pub(crate) fn check(
        &self,
        uri: &Uri,
        ip: IpAddr,
    ) -> Result<(), HostResponse> {
        if let Some(acl) = &self.acl {
            acl.check(uri.path(), ip)?;
        }
        if let Some(acl) = &self.route_acl {
            acl.check(uri.path(), ip)?;
        }
        for rule in &self.redirects {
            if rule.path.has_path_segment(uri.path()) {
                let location = rule
                    .target
                    .render(uri)
                    .map_err(|_| RequestRejection::InvalidRedirect)?;
                return Err(HostResponse::Redirect {
                    status: rule.status,
                    location,
                });
            }
        }
        Ok(())
    }
}

/// No canonical host must not be confused with an open host. The open path
/// keeps the existing opt-in cost: no policy Arc clone or rule traversal.
pub(crate) enum PolicySnapshot {
    Removed,
    Open,
    Rules(Arc<RequestPolicy>),
}

impl PolicySnapshot {
    pub(crate) fn check(
        &self,
        uri: &Uri,
        ip: IpAddr,
    ) -> Result<(), HostResponse> {
        match self {
            Self::Removed => Err(RequestRejection::HostRemoved.into()),
            Self::Open => Ok(()),
            Self::Rules(policy) => policy.check(uri, ip),
        }
    }
}

struct RedirectRule {
    path: PathPrefix,
    status: StatusCode,
    target: Target,
}

impl RedirectRule {
    fn compile(config: &RedirectConfig) -> Self {
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

#[cfg(test)]
#[path = "../../tests/service/policy.rs"]
mod tests;
