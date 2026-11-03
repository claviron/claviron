//! Configured ACL rules. Accepted identity is borrowed, not injected into a map.

use super::*;
use crate::config::acl::{AccessLimiter, AccessPolicy};
use crate::service::RequestRejection;
use claviron_http::ext::{ConnCtx, PathPrefix, RealIpExt};
use std::sync::Arc;

mod path;
use path::CompiledAcl;
#[cfg(feature = "rate-limit")]
pub(super) use path::comparison_paths;

#[derive(Clone)]
pub struct AclLayer {
    rules: Arc<CompiledAcl>,
}

impl AclLayer {
    pub fn new(rule: &AccessLimiter) -> Self {
        Self {
            rules: CompiledAcl::from_entries([(
                PathPrefix::root(),
                rule.clone(),
            )])
            .expect("one prepared access rule"),
        }
    }

    pub fn from_entries(
        entries: impl IntoIterator<Item = (PathPrefix, AccessLimiter)>
    ) -> Option<Self> {
        CompiledAcl::from_entries(entries).map(|rules| Self { rules })
    }

    pub(crate) fn for_host(config: &crate::HostConfig) -> Option<Self> {
        if !config.scopes().any(|(_, scope)| {
            scope.acl().is_some_and(|rule| {
                matches!(rule.policy, AccessPolicy::Deny)
                    || !rule.denied.is_empty()
            })
        }) {
            return None;
        }
        Self::from_entries(config.scopes().filter_map(|(prefix, scope)| {
            scope.acl().map(|rule| {
                (prefix.cloned().unwrap_or_else(PathPrefix::root), rule.clone())
            })
        }))
    }
}

impl<S, R: From<RequestRejection>> HostLayer<S, R> for AclLayer {
    fn prepare(
        self,
        layer: &mut HostLayerBuilder<S, R>,
    ) -> crate::Result<()> {
        layer.before_context(move |head, _, context| {
            let ip = head.extensions.get::<RealIpExt>().map(|ip| ip.0).or_else(
                || {
                    context
                        .map(|context| context.connection.as_ref())
                        .or_else(|| {
                            head.extensions
                                .get::<Arc<ConnCtx>>()
                                .map(Arc::as_ref)
                        })
                        .map(|connection| {
                            connection
                                .real_ip
                                .unwrap_or_else(|| connection.remote.ip())
                        })
                },
            );
            let Some(ip) = ip else {
                return ControlFlow::Break(
                    RequestRejection::MissingIdentity.into(),
                );
            };
            match self.rules.check(head.uri.path(), ip) {
                Ok(()) => ControlFlow::Continue(()),
                Err(reason) => ControlFlow::Break(reason.into()),
            }
        });
        Ok(())
    }
}
