use claviron_runtime::{AppState, Register, registry};

use super::LaravelConfigParser;

/// Registers the Laravel PHP parser before configuration preparation.
///
/// The working directory is the application root. Registration only declares
/// the parser: it does not evaluate PHP, boot Laravel or start a runnable.
pub struct LaravelConfigProvider;

impl Register<AppState> for LaravelConfigProvider {
    fn register(state: &AppState) -> registry::Result<()> {
        let root = std::env::current_dir()
            .map_err(|error| registry::Error::msg(error.to_string()))?;
        let parser = LaravelConfigParser::new(root, default_max_depth())
            .map_err(|error| registry::Error::msg(error.to_string()))?;
        state.config_reader().register(parser)?;
        Ok(())
    }
}

/// Preserve the existing host registration's returned-array nesting bound.
/// Explicit parser construction remains available for a different bound.
fn default_max_depth() -> usize {
    64
}
