//! Static files in front of the owner, served by Claviron's file service.
//!
//! Behind an edge the edge serves them and this is not needed. Without one —
//! a runner's own listener in development, or a deployment that runs no edge —
//! it answers explicit mounts in Rust, before the owner is reached. `assets`
//! is shorthand for mounts under public; `try_files` opts into general probing.
//! Only GET and HEAD look at files; every other method goes on whatever its path,
//! and so does a dynamic file, whose source is never served. A missing file is
//! the mount's 404 unless the mount says `none_found: true`.
//!
//! ```yaml
//! files:
//!   root: public
//!   statics:
//!     - { path: /static, dir: static }
//! ```

use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::middleware::{Next, from_fn_with_state};
use axum::response::Response;
use claviron_files::FileSite;
use claviron_http::config::statics::StaticConfig;
use http::{Method, StatusCode};
use serde::Deserialize;

/// The `files:` section, as written.
///
/// Carried to workers as it was written rather than as parsed: Claviron's
/// mount rules read durations as text and keep seconds, so they do not write
/// back to what they read. Each process parses it; the master first, when the
/// launch is resolved, so a bad block stops the launch there.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct FilesSource(serde_yaml::Value);

impl FilesSource {
    pub fn parse(&self) -> Result<Files, String> {
        let files: Files = serde_yaml::from_value(self.0.clone())
            .map_err(|error| format!("files: {error}"))?;
        files.validate()?;
        Ok(files)
    }
}

/// File policy for one application. `root` is relative to the
/// application's root, and each mount's `dir` to `root`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Files {
    root: PathBuf,
    statics: Vec<StaticConfig>,
    #[serde(default)]
    try_files: bool,
}

impl Files {
    pub(crate) fn public() -> Self {
        Self { root: "public".into(), statics: Vec::new(), try_files: false }
    }

    /// Expand shorthand directories into the same static mount policy.
    /// An explicit mount at that prefix retains its rules.
    pub(crate) fn with_assets(
        mut self,
        assets: &[PathBuf],
        try_files: bool,
    ) -> Result<Self, String> {
        self.try_files |= try_files;
        for asset in assets {
            let text = asset.to_str().ok_or("assets must be UTF-8 paths")?;
            if text.is_empty()
                || text.contains(['\\', '%', '?', '#'])
                || asset
                    .components()
                    .any(|part| !matches!(part, Component::Normal(_)))
                || text
                    .split('/')
                    .any(|part| part.is_empty() || part.starts_with('.'))
            {
                return Err(format!(
                    "assets: '{text}' must be a directory relative to the public root"
                ));
            }
            let path = serde_yaml::from_value(serde_yaml::Value::String(
                format!("/{text}"),
            ))
            .map_err(|error| format!("assets: {error}"))?;
            if !self.statics.iter().any(|mount| mount.path == path) {
                self.statics.push(StaticConfig {
                    path,
                    dir: asset.clone(),
                    none_found: false,
                    rules: None,
                });
            }
        }
        self.validate()?;
        Ok(self)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.root.as_os_str().is_empty() {
            return Err("files.root must not be empty".to_owned());
        }
        for mount in &self.statics {
            if mount.path.contains(['?', '#']) {
                return Err(format!(
                    "files.statics: '{}' is not an absolute URL path",
                    mount.path
                ));
            }
            if mount.dir.as_os_str().is_empty() {
                return Err(format!(
                    "files.statics: '{}' has no dir",
                    mount.path
                ));
            }
        }
        Ok(())
    }

    /// Anchored at the application's root.
    pub fn under(
        mut self,
        application_root: &Path,
    ) -> Self {
        if self.root.is_relative() {
            self.root = application_root.join(&self.root);
        }
        self
    }
}

impl FileSite for Files {
    fn root(&self) -> PathBuf {
        self.root.clone()
    }

    fn indexes(&self) -> &[String] {
        &[]
    }

    fn try_files(&self) -> bool {
        self.try_files
    }

    fn spa(&self) -> Option<&str> {
        None
    }

    fn statics(&self) -> Option<&[StaticConfig]> {
        Some(&self.statics)
    }

    fn has_static_index(&self) -> bool {
        false
    }
}

/// `router` with explicit mounts and optional public-file probing in front.
pub fn in_front(
    files: Arc<Files>,
    router: Router,
) -> Router {
    // Explicit mounts stay authoritative. General probing only runs after
    // an unmatched mount, and only when requested at launch.
    let router = if files.try_files {
        router.layer(from_fn_with_state(Arc::clone(&files), probe))
    } else {
        router
    };
    let site: Arc<dyn FileSite> = files;
    router.layer(from_fn_with_state(site, serve))
}

async fn probe(
    State(files): State<Arc<Files>>,
    request: Request,
    next: Next,
) -> Response {
    if request.method() == Method::GET || request.method() == Method::HEAD {
        if let claviron_http::ServiceResult::Handled(response) =
            claviron_files::try_files_probe(
                &files.root,
                request.uri(),
                request.headers(),
            )
            .await
        {
            return claviron_http::into_axum_response(response);
        }
    }
    next.run(request).await
}

async fn serve(
    site: State<Arc<dyn FileSite>>,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    // The file service takes no method: a POST to an asset's path is the
    // owner's, not a file read.
    let head = request.method() == Method::HEAD;
    if !head && request.method() != Method::GET {
        return Ok(next.run(request).await);
    }
    let mut response =
        claviron_files::axum_static_mount_handler(site, request, next).await?;
    if head {
        *response.body_mut() = Body::empty();
    }
    Ok(response)
}

#[cfg(test)]
#[path = "../tests/files.rs"]
mod tests;
