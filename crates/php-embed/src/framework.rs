//! Cold-path Composer inspection. Explicit application metadata always wins.

use std::collections::BTreeSet;
use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};

use serde_json::Value;

/// Composer package names for one application project, without loading PHP.
/// Installed packages take precedence over the lockfile and root requirements.
#[derive(Debug, Default)]
pub struct ComposerPackages {
    names: BTreeSet<String>,
}

impl ComposerPackages {
    /// Read a project's own Composer records. Missing records mean no packages;
    /// malformed or unreadable records are reported to the caller.
    pub fn read(root: &Path) -> io::Result<Self> {
        for relative in
            ["vendor/composer/installed.json", "composer.lock", "composer.json"]
        {
            let path = root.join(relative);
            let bytes = match std::fs::read(&path) {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            };
            let document: Value =
                serde_json::from_slice(&bytes).map_err(|error| {
                    io::Error::new(ErrorKind::InvalidData, error)
                })?;
            let names = if relative == "composer.json" {
                document
                    .get("require")
                    .and_then(Value::as_object)
                    .into_iter()
                    .flat_map(|packages| packages.keys().cloned())
                    .collect()
            } else {
                // Composer 1 writes an array; Composer 2 wraps it in packages.
                document
                    .get("packages")
                    .unwrap_or(&document)
                    .as_array()
                    .ok_or_else(|| {
                        io::Error::new(
                            ErrorKind::InvalidData,
                            format!(
                                "{}: expected Composer packages",
                                path.display()
                            ),
                        )
                    })?
                    .iter()
                    .filter_map(|package| package.get("name")?.as_str())
                    .map(str::to_owned)
                    .collect()
            };
            return Ok(Self { names });
        }
        Ok(Self::default())
    }

    pub fn contains(
        &self,
        package: &str,
    ) -> bool {
        self.names.contains(package)
    }

    /// Recognize full frameworks, not shared components or optional adapters.
    /// In particular, Laravel's Symfony dependencies do not imply Symfony.
    pub fn frameworks(&self) -> Vec<String> {
        [
            ("laravel/framework", "Laravel"),
            ("symfony/framework-bundle", "Symfony"),
        ]
        .into_iter()
        .filter(|(package, _)| self.contains(package))
        .map(|(_, label)| label.to_owned())
        .collect()
    }
}

/// Find the nearest Composer project enclosing the entry script. Relative
/// entries are resolved against cwd, never against the host's config directory.
pub fn project_root(entry: &Path) -> io::Result<Option<PathBuf>> {
    let entry = if entry.is_absolute() {
        entry.to_owned()
    } else {
        std::env::current_dir()?.join(entry)
    };
    for directory in entry.parent().into_iter().flat_map(Path::ancestors) {
        for relative in
            ["composer.json", "composer.lock", "vendor/composer/installed.json"]
        {
            if directory.join(relative).try_exists()? {
                return Ok(Some(directory.to_owned()));
            }
        }
    }
    Ok(None)
}

pub(crate) fn detect(entry: &Path) -> Vec<String> {
    let discovered = project_root(entry).and_then(|root| {
        root.map(|root| ComposerPackages::read(&root)).transpose()
    });
    match discovered {
        Ok(packages) => {
            packages.map(|packages| packages.frameworks()).unwrap_or_default()
        }
        Err(error) => {
            tracing::warn!(
                entry = %entry.display(),
                %error,
                "PHP framework metadata unavailable; execution is unchanged"
            );
            Vec::new()
        }
    }
}
