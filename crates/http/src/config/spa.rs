//! Single-page application shell and its fallback policy.

use super::caching::CachingConfig;
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Clone, Debug, Deserialize)]
#[serde(from = "SpaDeclaration")]
pub struct SpaConfig {
    pub entry: PathBuf,
    pub reject_ajax: bool,
    pub caching: Option<CachingConfig>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum SpaDeclaration {
    Entry(PathBuf),
    Options(SpaOptions),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SpaOptions {
    entry: PathBuf,
    #[serde(default)]
    reject_ajax: bool,
    caching: Option<CachingConfig>,
}

impl From<SpaDeclaration> for SpaConfig {
    fn from(value: SpaDeclaration) -> Self {
        match value {
            SpaDeclaration::Entry(entry) => {
                Self { entry, reject_ajax: false, caching: None }
            }
            SpaDeclaration::Options(options) => Self {
                entry: options.entry,
                reject_ajax: options.reject_ajax,
                caching: options.caching,
            },
        }
    }
}
