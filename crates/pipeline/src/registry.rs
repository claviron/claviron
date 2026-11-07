//! What a site's directives and owners are prepared by, independent of hosts.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use claviron_http::dispatch::RouteCondition;
use claviron_runtime::registry::{Error, Result};

use crate::extension::{RouteContext, RouteEffect, RouteExtension, RouteLayer};
use crate::factory::BackendFactory;
use crate::{HostHandler, Site};

/// Route extensions brought by something other than the registry, for one
/// scope's resolution. `inactive` names those that exist but do not apply to
/// the protocol being prepared: their directives are skipped, not unresolved.
#[derive(Default)]
pub struct ContributedExtensions {
    pub active: Vec<Arc<dyn RouteExtension>>,
    pub inactive: HashSet<&'static str>,
}

#[derive(Default)]
pub(crate) struct ResolvedRouteExtensions {
    pub handler: Option<Arc<dyn HostHandler>>,
    pub conditions: Vec<Arc<dyn RouteCondition>>,
    pub layers: Vec<Arc<dyn RouteLayer>>,
}

/// The backend factories and route extensions a [`Site`]'s directives resolve
/// against. Each directive has one owner.
#[derive(Default)]
pub struct SiteRegistry {
    route_extensions: RwLock<Vec<Arc<dyn RouteExtension>>>,
    backend_factories: RwLock<Vec<Arc<dyn BackendFactory>>>,
}

impl SiteRegistry {
    /// Register a host-independent directive compiler before snapshot build.
    pub fn register_route_extension(
        &self,
        extension: Arc<dyn RouteExtension>,
    ) -> Result<()> {
        if self
            .backend_factories
            .read()
            .expect("backend factories lock poisoned")
            .iter()
            .any(|factory| factory.name() == extension.name())
        {
            return Err(Error::msg(format!(
                "directive '{}' already belongs to a backend",
                extension.name()
            )));
        }
        let mut extensions = self
            .route_extensions
            .write()
            .expect("route extensions lock poisoned");
        if extensions.iter().any(|existing| existing.name() == extension.name())
        {
            return Err(Error::msg(format!(
                "route extension '{}' is already registered",
                extension.name()
            )));
        }
        extensions.push(extension);
        Ok(())
    }

    /// Register an execution owner: from code with
    /// [`backend`](crate::factory::backend), or from a site's directive with
    /// [`factory`](crate::factory::factory). Consulted only while preparing
    /// scopes.
    pub fn register_backend(
        &self,
        factory: Arc<dyn BackendFactory>,
    ) -> Result<()> {
        if self
            .route_extensions
            .read()
            .expect("route extensions lock poisoned")
            .iter()
            .any(|extension| extension.name() == factory.name())
        {
            return Err(Error::msg(format!(
                "directive '{}' already belongs to a route extension",
                factory.name()
            )));
        }
        let mut factories = self
            .backend_factories
            .write()
            .expect("backend factories lock poisoned");
        if factories.iter().any(|existing| existing.name() == factory.name()) {
            return Err(Error::msg(format!(
                "backend '{}' is already registered",
                factory.name()
            )));
        }
        factories.push(factory);
        Ok(())
    }

    /// The registered backends whose directive this scope declares, in
    /// registration order. A null value declares nothing.
    pub(crate) fn declared_backends(
        &self,
        scope: &Site,
    ) -> Vec<&'static str> {
        self.backend_factories
            .read()
            .expect("backend factories lock poisoned")
            .iter()
            .map(|factory| factory.name())
            .filter(|name| {
                scope.directive(name).is_some_and(|value| !value.is_null())
            })
            .collect()
    }

    /// What is registered, for one compilation to prepare.
    pub(crate) fn backends(&self) -> Vec<Arc<dyn BackendFactory>> {
        self.backend_factories
            .read()
            .expect("backend factories lock poisoned")
            .clone()
    }

    pub(crate) fn resolve_extensions(
        &self,
        context: &RouteContext<'_>,
        inherited_only: bool,
        contributed: &ContributedExtensions,
    ) -> Result<ResolvedRouteExtensions> {
        let mut codecs = HashMap::new();
        for codec in self
            .route_extensions
            .read()
            .expect("route extensions lock poisoned")
            .iter()
        {
            codecs.insert(codec.name(), codec.clone());
        }
        let inactive = &contributed.inactive;
        for codec in &contributed.active {
            let name = codec.name();
            // A site decodes its own directives before any extension could
            // see them.
            if crate::site::is_builtin_directive(name) {
                return Err(Error::msg(format!(
                    "route extension '{name}' names a directive a site owns itself"
                )));
            }
            if codecs.insert(name, codec.clone()).is_some() {
                return Err(Error::msg(format!(
                    "route extension '{name}' has multiple owners"
                )));
            }
        }
        let backend_names: HashSet<_> = self
            .backend_factories
            .read()
            .expect("backend factories lock poisoned")
            .iter()
            .map(|factory| factory.name())
            .collect();
        if let Some(name) =
            codecs.keys().find(|name| backend_names.contains(**name))
        {
            return Err(Error::msg(format!(
                "directive '{name}' has both a route extension and backend owner"
            )));
        }
        let config = context.route;
        let mut handler = None;
        let mut conditions = Vec::new();
        let mut layers = Vec::new();
        let mut directives = std::collections::BTreeMap::new();
        for (name, value) in &context.config.extensions {
            if codecs
                .get(name.as_str())
                .is_some_and(|codec| codec.inherit_from_host())
            {
                directives.insert(name, value);
            }
        }
        if !inherited_only {
            directives.extend(config.extensions.iter());
        }
        let (inherited, local): (Vec<_>, Vec<_>) =
            directives.into_iter().partition(|(name, _)| {
                codecs
                    .get(name.as_str())
                    .is_some_and(|codec| codec.inherit_from_host())
            });
        for (name, value) in inherited.into_iter().chain(local) {
            if backend_names.contains(name.as_str()) {
                continue;
            }
            if !codecs.contains_key(name.as_str())
                && inactive.contains(name.as_str())
            {
                continue;
            }
            let codec = codecs.get(name.as_str()).ok_or_else(|| {
                Error::msg(format!(
                    "unresolved route directive '{name}' at '{}'",
                    context.prefix
                ))
            })?;
            let effect = codec.resolve(context, value)?;
            if codec.inherit_from_host()
                && !matches!(&effect, RouteEffect::Layer(_))
            {
                return Err(Error::msg(format!(
                    "inherited route directive '{name}' must resolve to a layer"
                )));
            }
            match effect {
                RouteEffect::Handler(application) => {
                    if handler.is_some() {
                        return Err(Error::msg(format!(
                            "route '{}' has multiple Rust handler directives",
                            context.prefix
                        )));
                    }
                    handler = Some(application);
                }
                RouteEffect::Condition(condition) => conditions.push(condition),
                RouteEffect::Layer(layer) => layers.push(layer),
            }
        }
        Ok(ResolvedRouteExtensions { handler, conditions, layers })
    }
}
