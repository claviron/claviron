//! Standalone Laravel console entry. Laravel owns command registration and boot.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use claviron_php_embed::console::PhpConsole;
use tokio_util::sync::CancellationToken;

/// Run the application's own `artisan` entry in its working directory.
/// No HTTP server, runtime provider registration or implicit live attachment.
pub fn run(
    base_path: impl AsRef<Path>,
    args: Vec<String>,
    environment: BTreeMap<String, String>,
    cancellation: CancellationToken,
) -> Result<i32, String> {
    validate_project(base_path.as_ref())?;
    command(base_path.as_ref().to_owned(), args, environment, cancellation)
        .run()
}

fn validate_project(base_path: &Path) -> Result<(), String> {
    let missing = "No Laravel application found here.\n  Run Artisan from your Laravel project's root directory.";
    match std::fs::metadata(base_path.join("artisan")) {
        Ok(metadata) if metadata.is_file() => Ok(()),
        Ok(_) => Err(missing.into()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Err(missing.into())
        }
        Err(error) => Err(format!("Cannot inspect artisan: {error}")),
    }
}

fn command(
    base_path: PathBuf,
    args: Vec<String>,
    mut environment: BTreeMap<String, String>,
    cancellation: CancellationToken,
) -> PhpConsole {
    // The shared SAPI is named claviron, not cli. Declare Laravel's existing
    // console marker before the framework decides which providers to load.
    environment.insert("APP_RUNNING_IN_CONSOLE".into(), "true".into());
    PhpConsole {
        script: PathBuf::from("artisan"),
        args,
        cwd: base_path,
        environment,
        cancellation,
    }
}

#[cfg(test)]
#[path = "../tests/artisan.rs"]
mod tests;
