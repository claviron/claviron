//! File-only scope selection for the automatic Axum-free upstream binding.

use std::path::PathBuf;

use claviron_files::FileSite;
use claviron_http::config::{
    mapping::RouteMapping, routes::RouteTable, spa::SpaConfig,
    statics::StaticConfig,
};

use super::*;

struct FileScope {
    steps: Vec<Arc<dyn DispatchStep>>,
    fallback: FilesConfig,
}

impl FileScope {
    fn new(config: &HostConfig) -> Self {
        Self { steps: scope_steps(config), fallback: fallback_config(config) }
    }
}

struct PreparedFileScopes {
    root: FileScope,
    routes: RouteTable<FileScope>,
}

impl FileSite for PreparedFileScopes {
    fn root(&self) -> PathBuf { self.root.fallback.root() }
    fn mapping(&self) -> Option<&RouteMapping> { self.root.fallback.mapping() }
    fn route_for(&self, path: &str) -> Option<&dyn FileSite> {
        self.routes.find(path).map(|scope| &scope.fallback as &dyn FileSite)
    }
    fn indexes(&self) -> &[String] { self.root.fallback.indexes() }
    fn try_files(&self) -> bool { false }
    fn spa(&self) -> Option<&str> { self.root.fallback.spa() }
    fn spa_config(&self) -> Option<&SpaConfig> { self.root.fallback.spa_config() }
    fn statics(&self) -> Option<&[StaticConfig]> { None }
    fn has_static_index(&self) -> bool { self.root.fallback.has_static_index() }
}

struct ScopedFiles(Arc<PreparedFileScopes>);

impl DispatchStep for ScopedFiles {
    fn call(&self, request: Request<RouteBody>) -> DispatchFuture {
        let plan = self.0.clone();
        DispatchFuture::new(async move {
            // One selected scope, including empty scopes which deliberately
            // suppress root file steps. No HostConfig survives this boundary.
            let scope = plan.routes.find(request.uri().path())
                .unwrap_or(&plan.root);
            let mut request = request;
            for step in &scope.steps {
                request = match step.call(request).await {
                    ServiceResult::Handled(response) => {
                        return ServiceResult::Handled(response);
                    }
                    ServiceResult::Unhandled(request) => request,
                };
            }
            ServiceResult::Unhandled(request)
        })
    }
}

/// Share one cold route mapping between pre-backend steps and the residual
/// directory-index/SPA fallback. Public UpstreamBypass composition is unchanged.
pub(crate) fn prepare_upstream_files(
    config: &HostConfig,
) -> (Arc<dyn FileSite>, Vec<Arc<dyn DispatchStep>>) {
    let root = FileScope::new(config.fallback().map_or(config, Arc::as_ref));
    if config.routes.is_empty() {
        return (Arc::new(root.fallback), root.steps);
    }
    let routes = RouteTable::new(config.routes.iter().map(|(prefix, config)| {
        (prefix.clone(), FileScope::new(config))
    }));
    let has_steps = !root.steps.is_empty()
        || routes.iter().any(|(_, scope)| !scope.steps.is_empty());
    let plan = Arc::new(PreparedFileScopes { root, routes });
    let steps: Vec<Arc<dyn DispatchStep>> = if has_steps {
        vec![Arc::new(ScopedFiles(plan.clone()))]
    } else {
        Vec::new()
    };
    (plan, steps)
}
