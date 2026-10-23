//! A prepared filesystem directory paired with its original URL prefix.

use crate::ext::PathPrefix;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub struct RouteMapping {
    prefix: PathPrefix,
    directory: PathBuf,
}

impl RouteMapping {
    pub fn new(
        prefix: PathPrefix,
        directory: PathBuf,
    ) -> Self {
        Self { prefix, directory }
    }

    pub fn prefix(&self) -> &PathPrefix {
        &self.prefix
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    /// Return only the name below this mapping. Decoding and traversal checks
    /// remain with the file or script resolver; the HTTP URI is never rewritten.
    pub fn relative_path<'a>(
        &self,
        path: &'a str,
    ) -> Option<&'a str> {
        if !self.prefix.has_path_segment(path) {
            return None;
        }
        path.strip_prefix(self.prefix.as_str().trim_end_matches('/'))
            .map(|relative| relative.trim_start_matches('/'))
    }
}
