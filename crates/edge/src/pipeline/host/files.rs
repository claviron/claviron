//! Cold file-step preparation. Configuration spelling does not survive dispatch.
//!
//! A selected scope runs its Rust applications, explicit Static steps, then
//! its optional TryFiles step, before handing the unread body to Embed/backend.
//! Configuration is compiled before dispatch; these steps never enter Axum.

use std::sync::Arc;

use claviron_files::StaticMount;
use claviron_http::{RequestContext, RouteBody, ServiceResult};
use http::Request;
use claviron_http::config::files::FilesConfig;
use claviron_http::ext::RealIpExt;

use super::{DispatchStep, DispatchFuture};
use crate::HostConfig;

/// One explicit, prepared static mount. The file service owns mapping/security.
#[derive(Clone)]
pub struct StaticStage {
    mount: Arc<StaticMount>,
}

impl StaticStage {
    pub fn new(root: std::path::PathBuf, mount: claviron_http::config::statics::StaticConfig) -> Self {
        Self { mount: Arc::new(StaticMount::new(root, mount)) }
    }
}

impl DispatchStep for StaticStage {
    fn call(&self, request: Request<RouteBody>) -> DispatchFuture {
        let mount = self.mount.clone();
        DispatchFuture::new(async move {
            let context = RequestContext {
                uri: request.uri(), headers: request.headers(),
                real_ip: request.extensions().get::<RealIpExt>().map(|ip| ip.0),
            };
            match mount.try_handle(context).await {
                ServiceResult::Handled(response) => ServiceResult::Handled(response),
                ServiceResult::Unhandled(_) => ServiceResult::Unhandled(request),
            }
        })
    }
}

/// An explicitly enabled filesystem probe before the Embed/backend owner.
#[derive(Clone)]
pub struct TryFilesStage {
    site: Arc<FilesConfig>,
}

impl TryFilesStage {
    /// Disabled declarations contribute no request step.
    pub fn new(site: Arc<FilesConfig>) -> Option<Self> {
        site.try_files.then_some(Self { site })
    }
}

impl DispatchStep for TryFilesStage {
    fn call(&self, request: Request<RouteBody>) -> DispatchFuture {
        let site = self.site.clone();
        DispatchFuture::new(async move {
            match claviron_files::try_files_for_site(site.as_ref(), request.uri(), request.headers()).await {
                ServiceResult::Handled(response) => ServiceResult::Handled(response),
                ServiceResult::Unhandled(_) => ServiceResult::Unhandled(request),
            }
        })
    }
}

pub(super) fn scope_steps(config: &HostConfig) -> Vec<Arc<dyn DispatchStep>> {
    let mut files = config.files.clone();
    files.root = config.root();
    let mut steps: Vec<Arc<dyn DispatchStep>> = Vec::new();
    for mount in files.statics.iter().flatten() {
        steps.push(Arc::new(StaticStage::new(files.root.clone(), mount.clone())));
    }
    if let Some(probe) = TryFilesStage::new(Arc::new(files)) {
        steps.push(Arc::new(probe));
    }
    steps
}

/// General fallback after these steps must not repeat Static or TryFiles.
pub(crate) fn fallback_config(config: &HostConfig) -> FilesConfig {
    let mut files = config.files.clone();
    files.root = config.root();
    files.statics = None;
    files.try_files = false;
    files
}

#[cfg(test)]
#[path = "../../../tests/pipeline/host/files.rs"]
mod tests;
