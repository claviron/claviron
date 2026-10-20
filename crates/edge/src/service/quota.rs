//! Host/prefix bindings compiled before serving. Config uses IP keys; Rust
//! bindings retain their own typed KeyProvider. Neither path is middleware.

use std::collections::HashMap;
use std::sync::Arc;

use claviron_http::ext::PathPrefix;
use claviron_http::rate_limit::{
    Error as LimitError, IpKey, KeyProvider, RateLimiter, Rejection,
    RequestHead,
};
use claviron_orbit::quota::{Decision, Quota, QuotaConfig, QuotaResource};
use claviron_runtime::registry::{Error, Result};
use http::Request;

use super::{HostResponse, RequestRejection};
use crate::Host;
use crate::config::{HostConfig, quota::RateLimitConfig};
use crate::loader::table::VhostTable;

trait Admission: Send + Sync {
    fn check(
        &self,
        request: &RequestHead<'_>,
    ) -> std::result::Result<Decision, LimitError>;
    fn config(&self) -> &QuotaConfig;
}

impl<K: KeyProvider> Admission for RateLimiter<K> {
    fn check(
        &self,
        request: &RequestHead<'_>,
    ) -> std::result::Result<Decision, LimitError> {
        self.check(request)
    }
    fn config(&self) -> &QuotaConfig {
        self.store().config()
    }
}

#[derive(Clone)]
pub(crate) struct RegisteredQuota(Arc<dyn Admission>);

#[derive(Clone, Default)]
pub(crate) struct QuotaRegistrations {
    sealed: bool,
    entries: HashMap<Host, Vec<(PathPrefix, RegisteredQuota)>>,
}

impl QuotaRegistrations {
    pub(crate) fn register<K: KeyProvider + 'static>(
        &mut self,
        host: Host,
        prefix: PathPrefix,
        limiter: RateLimiter<K>,
    ) -> Result<()> {
        if self.sealed {
            return Err(Error::msg(
                "quota registration is closed after host preparation",
            ));
        }
        validate_prefix(&prefix)?;
        let entries = self.entries.entry(host).or_default();
        if entries.iter().any(|(existing, _)| existing == &prefix) {
            return Err(Error::msg(format!(
                "quota already registered at '{prefix}'"
            )));
        }
        entries.push((prefix, RegisteredQuota(Arc::new(limiter))));
        Ok(())
    }

    /// Validate all resource identities before opening any configured store.
    /// Quota resources use explicitly dedicated namespaces, never core kinds.
    pub(crate) fn seal(
        &mut self,
        runtime_fleet: &str,
        table: &VhostTable,
    ) -> Result<()> {
        let mut used = HashMap::<QuotaResource, String>::new();
        for host in self.entries.keys() {
            if table.canonical_config(host).is_none() {
                return Err(Error::msg(format!(
                    "quota host '{host}' has no canonical declaration"
                )));
            }
        }
        for (host, record) in table.canonical_entries() {
            let declarations = declarations(&record.config);
            let registered =
                self.entries.get(host).map(Vec::as_slice).unwrap_or_default();
            for (prefix, definition) in &declarations {
                validate_prefix(prefix)?;
                definition.validate().map_err(Error::msg)?;
                if registered.iter().any(|(bound, _)| bound == prefix) {
                    return Err(Error::msg(format!(
                        "host '{host}' quota at '{prefix}' is declared in both config and Rust"
                    )));
                }
            }
            for (prefix, config) in declarations
                .iter()
                .map(|(p, c)| (p, &c.quota))
                .chain(registered.iter().map(|(p, q)| (p, q.0.config())))
            {
                if let Some(resource) = config.resource() {
                    if resource.fleet.as_str() == runtime_fleet {
                        return Err(Error::msg(
                            "quota SHM requires a dedicated namespace, separate from the runtime fleet",
                        ));
                    }
                    let owner = format!("{host}{prefix}");
                    if let Some(previous) = used.insert(resource, owner.clone())
                    {
                        return Err(Error::msg(format!(
                            "quota SHM resource is reused by '{previous}' and '{owner}'; each limit definition needs its own resource"
                        )));
                    }
                }
            }
        }
        self.sealed = true;
        Ok(())
    }

    pub(crate) fn prepare(
        &self,
        config_check: bool,
        host: &Host,
        config: &HostConfig,
        previous: Option<&Option<Arc<HostQuotas>>>,
    ) -> Result<Option<Arc<HostQuotas>>> {
        let declarations = declarations(config);
        let registered = self.entries.get(host).cloned().unwrap_or_default();
        if let Some(previous) = previous {
            let unchanged = match previous {
                Some(old) => {
                    old.declarations == declarations
                        && old.registered.len() == registered.len()
                        && old.registered.iter().zip(&registered).all(
                            |((a, x), (b, y))| {
                                a == b && Arc::ptr_eq(&x.0, &y.0)
                            },
                        )
                }
                None => declarations.is_empty() && registered.is_empty(),
            };
            if !unchanged {
                return Err(Error::msg(format!(
                    "host '{host}': quota changes require a process restart; live quota reload is not supported"
                )));
            }
            return Ok(previous.clone());
        }
        if declarations.is_empty() && registered.is_empty() {
            return Ok(None);
        }
        // A config check must not claim SHM owner rows beside a serving fleet.
        // Structural validation ran in seal(); no request will be served here.
        if config_check {
            return Ok(None);
        }
        let mut rules = registered.clone();
        for (prefix, definition) in &declarations {
            let store =
                Quota::new(definition.quota.clone()).map_err(|error| {
                    Error::msg(format!(
                        "host '{host}' quota '{prefix}': {error}"
                    ))
                })?;
            let limiter = RateLimiter::new(
                store,
                IpKey,
                definition.namespace.as_bytes(),
                definition.cost,
            )
            .map_err(|error| Error::msg(error.to_string()))?;
            rules.push((prefix.clone(), RegisteredQuota(Arc::new(limiter))));
        }
        rules.sort_unstable_by(|(a, _), (b, _)| {
            b.len().cmp(&a.len()).then_with(|| a.as_str().cmp(b.as_str()))
        });
        Ok(Some(Arc::new(HostQuotas {
            declarations,
            registered,
            rules: rules.into_boxed_slice(),
        })))
    }
}

fn declarations(config: &HostConfig) -> Vec<(PathPrefix, RateLimitConfig)> {
    let mut definitions = Vec::new();
    if let Some(definition) = &config.rate_limit {
        definitions.push((PathPrefix::root(), definition.clone()));
    }
    for (prefix, route) in config.routes.iter() {
        if prefix.as_str() != "/"
            && let Some(definition) = &route.route_rate_limit
        {
            definitions.push((prefix.clone(), definition.clone()));
        }
    }
    definitions
}

fn validate_prefix(prefix: &PathPrefix) -> Result<()> {
    if prefix.contains(['%', '?', '#', '\\'])
        || prefix.contains("//")
        || prefix.split('/').any(|part| matches!(part, "." | ".."))
    {
        return Err(Error::msg(format!("ambiguous quota prefix '{prefix}'")));
    }
    Ok(())
}

/// A host's prepared rate admission, shared by aliases. Longest declared prefix
/// selects one limiter; parent budgets are not charged a second time.
pub struct HostQuotas {
    declarations: Vec<(PathPrefix, RateLimitConfig)>,
    registered: Vec<(PathPrefix, RegisteredQuota)>,
    rules: Box<[(PathPrefix, RegisteredQuota)]>,
}

impl HostQuotas {
    pub(crate) fn check<B>(
        &self,
        request: &Request<B>,
    ) -> std::result::Result<(), HostResponse> {
        // A uniform host policy needs no path normalization or prefix search.
        let selected =
            if self.rules.len() == 1 && self.rules[0].0.as_str() == "/" {
                Some(&self.rules[0])
            } else {
                let paths = super::acl::comparison_paths(request.uri().path())?;
                let selected = self.select(&paths.separated);
                if let Some(literal) = &paths.literal
                    && selected.map(|rule| rule as *const _)
                        != self.select(literal).map(|rule| rule as *const _)
                {
                    return Err(RequestRejection::AmbiguousPath.into());
                }
                selected
            };
        if let Some((_, quota)) = selected {
            Rejection::from_result(quota.0.check(&RequestHead::new(request)))
                .map_err(HostResponse::Quota)?;
        }
        Ok(())
    }

    fn select(
        &self,
        path: &str,
    ) -> Option<&(PathPrefix, RegisteredQuota)> {
        self.rules.iter().find(|(prefix, _)| prefix.has_path_segment(path))
    }
}

#[cfg(test)]
#[path = "../../tests/service/quota.rs"]
mod tests;
