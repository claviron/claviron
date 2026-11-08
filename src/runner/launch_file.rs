//! A launch file as every runner reads it: `.env` and the selected
//! environment's overlay first, then `${NAME}` expanded, then the schema.

use claviron_runner::RunnerOptions;
use claviron_runtime::congif::ConfigParser;
use claviron_runtime::env::{ConfigEnvironment, LOCAL, PRODUCTION};

/// The environment a launch file reads: `.env`, then `.env.<environment>`
/// for the environment the options select (`local` unless stated).
pub fn launch_environment(
    options: &RunnerOptions
) -> Result<ConfigEnvironment, String> {
    let environment = if options.production {
        PRODUCTION
    } else {
        options.environment.as_deref().unwrap_or(LOCAL)
    };
    ConfigEnvironment::load(
        &claviron_sys::short_hostname(),
        environment,
        std::path::Path::new(".env"),
    )
    .map_err(|error| error.to_string())
}

/// A YAML launch file's text with `${NAME}` expanded from
/// [`launch_environment`], ready for the runner's own schema.
pub fn expand_launch(
    text: &str,
    options: &RunnerOptions,
) -> Result<String, String> {
    let environment = launch_environment(options)?;
    claviron_runtime::congif::parsers::YamlParser
        .expand_variables(text, environment.snapshot().values())
        .map_err(|error| error.to_string())
}
