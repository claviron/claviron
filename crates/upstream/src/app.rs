//! Owns the runtime app matcher and profile-to-service handles.

use std::collections::BTreeMap;
use std::sync::Arc;

use matcher::{CompiledUpstreamRoute, RouteSelector};
pub(crate) mod current;
pub(crate) mod matcher;
use crate::proxy::service::UpstreamService;

#[derive(Clone, Debug)]
pub struct UpstreamApp {
    inner: Arc<AppInner>,
}

#[derive(Debug)]
struct AppInner {
    name: String,
    services: BTreeMap<String, ServiceHandle>,
    matcher: CompiledUpstreamApp,
}

#[derive(Clone, Debug)]
enum ServiceHandle {
    Fixed(UpstreamService),
    Live(Arc<current::CurrentService>),
}
impl ServiceHandle {
    fn load(&self) -> UpstreamService {
        match self {
            Self::Fixed(service) => service.clone(),
            Self::Live(current) => current.load(),
        }
    }
}

impl UpstreamApp {
    pub fn new(
        name: impl Into<String>,
        matcher: CompiledUpstreamApp,
        services: BTreeMap<String, UpstreamService>,
    ) -> Self {
        let services = services
            .into_iter()
            .map(|(name, service)| (name, ServiceHandle::Fixed(service)))
            .collect();
        Self {
            inner: Arc::new(AppInner { name: name.into(), services, matcher }),
        }
    }

    /// Provider-bound apps follow process-local publication. Route matching is
    /// frozen; only the selected profile's immutable service handle advances.
    pub(crate) fn bound(
        name: impl Into<String>,
        matcher: CompiledUpstreamApp,
        current: BTreeMap<String, Arc<current::CurrentService>>,
    ) -> Self {
        let services = current
            .into_iter()
            .map(|(name, current)| (name, ServiceHandle::Live(current)))
            .collect();
        Self {
            inner: Arc::new(AppInner { name: name.into(), services, matcher }),
        }
    }

    pub fn name(&self) -> &str {
        &self.inner.name
    }

    pub fn select_explicit(
        &self,
        path: &str,
    ) -> Option<UpstreamService> {
        self.inner
            .matcher
            .select_explicit_profile(path)
            .and_then(|profile| self.inner.services.get(profile))
            .map(ServiceHandle::load)
    }

    pub fn select_root(
        &self,
        path: &str,
    ) -> Option<UpstreamService> {
        self.inner
            .matcher
            .select_root_profile(path)
            .and_then(|profile| self.inner.services.get(profile))
            .map(ServiceHandle::load)
    }
}

#[derive(Clone, Debug)]
pub struct CompiledUpstreamApp {
    pub name: String,
    pub(crate) exact: Vec<CompiledUpstreamRoute>,
    pub(crate) prefixes: Vec<CompiledUpstreamRoute>,
    pub(crate) regexes: Vec<CompiledUpstreamRoute>,
    pub(crate) root_fallbacks: Vec<CompiledUpstreamRoute>,
}

impl CompiledUpstreamApp {
    pub fn root_profile(profile: impl Into<String>) -> Self {
        let profile = profile.into();
        Self {
            name: profile.clone(),
            exact: Vec::new(),
            prefixes: Vec::new(),
            regexes: Vec::new(),
            root_fallbacks: vec![CompiledUpstreamRoute {
                profile,
                selector: RouteSelector::RootFallback,
                excludes: Vec::new(),
            }],
        }
    }

    pub fn select_explicit_profile(
        &self,
        path: &str,
    ) -> Option<&str> {
        self.exact
            .iter()
            .chain(self.prefixes.iter())
            .chain(self.regexes.iter())
            .find(|route| route.matches(path))
            .map(|route| route.profile.as_str())
    }

    pub fn select_root_profile(
        &self,
        path: &str,
    ) -> Option<&str> {
        self.root_fallbacks
            .iter()
            .find(|route| route.matches(path))
            .map(|route| route.profile.as_str())
    }

    pub fn profiles(&self) -> impl Iterator<Item = &str> {
        self.exact
            .iter()
            .chain(self.prefixes.iter())
            .chain(self.regexes.iter())
            .chain(self.root_fallbacks.iter())
            .map(|route| route.profile.as_str())
    }
}
