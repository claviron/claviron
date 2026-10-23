//! What the file service knows about the site it serves.
//!
//! The file service does not know hosts. Whoever owns the site — the edge,
//! for a virtual host — hands it this view when the service is built, and the
//! service reads nothing else about where it runs.

use std::path::PathBuf;

use claviron_http::config::mapping::RouteMapping;
use claviron_http::config::spa::SpaConfig;
use claviron_http::config::statics::StaticConfig;

pub trait FileSite: Send + Sync {
    /// Directory that request paths resolve under.
    fn root(&self) -> PathBuf;
    /// Prepared disk directory and URL prefix for this selected route.
    fn mapping(&self) -> Option<&RouteMapping> {
        None
    }
    /// Host-wide consumers may select a prepared route without Axum. A caller
    /// already holding a selected route returns no additional selection.
    fn route_for(
        &self,
        _path: &str,
    ) -> Option<&dyn FileSite> {
        None
    }
    /// Index file names tried for a directory request, in order.
    fn indexes(&self) -> &[String];
    /// Whether a request path is looked up on disk before falling through.
    fn try_files(&self) -> bool;
    /// File served for any path the disk does not answer, if the site is a
    /// single-page application.
    fn spa(&self) -> Option<&str>;
    /// Typed policy, when supplied by the site owner. Legacy implementors
    /// keep the existing shell/cache behavior through `spa()`.
    fn spa_config(&self) -> Option<&SpaConfig> {
        None
    }
    /// Explicit static mounts, served ahead of any application.
    fn statics(&self) -> Option<&[StaticConfig]>;
    /// Whether some index file can be served statically rather than by an
    /// application.
    fn has_static_index(&self) -> bool;
}

/// HTTP data can be supplied directly, without an Edge host wrapper. Existing
/// application-owned FileSite implementations remain valid.
impl FileSite for claviron_http::config::files::FilesConfig {
    fn root(&self) -> PathBuf {
        self.mapping.as_ref().map_or_else(
            || self.root.clone(),
            |mapping| mapping.directory().to_path_buf(),
        )
    }
    fn mapping(&self) -> Option<&RouteMapping> {
        self.mapping.as_ref()
    }
    fn indexes(&self) -> &[String] {
        &self.index
    }
    fn try_files(&self) -> bool {
        self.try_files
    }
    fn spa(&self) -> Option<&str> {
        self.spa.as_ref().and_then(|spa| spa.entry.to_str())
    }
    fn spa_config(&self) -> Option<&SpaConfig> {
        self.spa.as_ref()
    }
    fn statics(&self) -> Option<&[StaticConfig]> {
        self.statics.as_deref()
    }
    fn has_static_index(&self) -> bool {
        self.index.iter().any(|index| {
            !claviron_http::config::statics::is_dynamic_file(index)
        })
    }
}
