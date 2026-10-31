//! Static directories a host serves and which files are never served as
//! static content.

use serde::Deserialize;
use std::path::PathBuf;

use crate::ext::PathPrefix;

use super::caching::RulesConfig;

/// File extensions that require an application handler rather than static delivery.
pub const DYNAMIC_FILE_EXTS: &[&str] = &["php", "phtml", "phar", "cgi", "fcgi"];

#[derive(Clone, Debug, Deserialize)]
pub struct StaticConfig {
    /// URL mount prefix, validated and normalized during deserialization.
    pub path: PathPrefix,
    /// Filesystem directory, relative to the site's root unless absolute.
    pub dir: PathBuf,
    /// (default false), when true, if file does not exist, server does NOT
    /// answer 404 while trying files; otherwise return the request to the next step.
    /// Must be `true` when dynamic files and assets are in same directory.
    /// `statics` attributes (with paths) are handled before catch-all dynamic/entry handlers.
    #[serde(default)]
    pub none_found: bool,
    pub rules: Option<Vec<RulesConfig>>,
}

#[inline]
pub fn is_dynamic_file(base_name: &str) -> bool {
    if let Some((_, ext)) = base_name.rsplit_once('.') {
        DYNAMIC_FILE_EXTS.iter().any(|d| ext.eq_ignore_ascii_case(d))
    } else {
        false
    }
}
