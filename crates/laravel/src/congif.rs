//! Laravel-authored PHP documents consumed by Claviron's Congif reader.
//! Laravel's own config loading/repository remains independent of this adapter.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use claviron_php_embed::evaluate::{
    PhpArrayKey, PhpBootstrap, PhpEvaluator, PhpValue,
};
use congif::{
    BoxError, ConfigParser, ConfigSource, ConfigValue, Variables, async_trait,
};

mod provider;

pub use provider::LaravelConfigProvider;

/// Executes one returned PHP value using Composer's autoloaded Laravel helpers.
/// No application providers, Laravel config files or HTTP/Artisan loop are booted.
/// Register after declaring PHP's process INI/extensions, before the reader's
/// configuration phase. Does not require PhpEmbedRuntime::boot or AppState.
#[derive(Clone, Debug)]
pub struct LaravelConfigParser {
    evaluator: PhpEvaluator,
}

impl LaravelConfigParser {
    /// The Composer runner ships in `claviron/laravel`; applications need no
    /// copied config runner. `max_depth` bounds the returned PHP array tree.
    pub fn new(
        base_path: impl AsRef<Path>,
        max_depth: usize,
    ) -> Result<Self, std::io::Error> {
        let base_path = std::path::absolute(base_path)?;
        let runner =
            base_path.join("vendor/claviron/laravel/runners/config.php");
        Ok(Self::with_runner(base_path, runner, max_depth))
    }

    /// Explicit runner location for Composer vendor-dir overrides or embedding.
    pub fn with_runner(
        base_path: PathBuf,
        runner: PathBuf,
        max_depth: usize,
    ) -> Self {
        Self {
            evaluator: PhpEvaluator::new(max_depth).with_bootstrap(
                PhpBootstrap {
                    path: runner,
                    argument: base_path.to_string_lossy().into_owned(),
                },
            ),
        }
    }

    /// Use an already composed independent evaluator (bootstrap/INI/cancellation).
    pub fn with_evaluator(evaluator: PhpEvaluator) -> Self {
        Self { evaluator }
    }
}

#[async_trait]
impl ConfigParser for LaravelConfigParser {
    fn extensions(&self) -> &[&str] {
        &["php"]
    }

    // PHP owns its syntax. An env() or getenv() call receives the pinned
    // environment; `${...}` in executable PHP is not a Congif text template.
    fn expand_variables(
        &self,
        text: &str,
        _variables: &Variables,
    ) -> congif::Result<String> {
        Ok(text.to_owned())
    }

    async fn parse(
        &self,
        source: ConfigSource<'_>,
    ) -> Result<ConfigValue, BoxError> {
        self.parse_with_variables(source, None).await
    }

    async fn parse_with_variables(
        &self,
        source: ConfigSource<'_>,
        variables: Option<&Variables>,
    ) -> Result<ConfigValue, BoxError> {
        let evaluator = self.evaluator.clone();
        let path = source.path.to_owned();
        let bytes = source.bytes.to_owned();
        let environment: BTreeMap<_, _> = variables
            .into_iter()
            .flat_map(|values| values.iter())
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        let value = tokio::task::spawn_blocking(move || {
            evaluator.evaluate_source(&path, &bytes, &environment)
        })
        .await?
        .map_err(std::io::Error::other)?;
        Ok(config_value(value))
    }
}

// No JSON round-trip: preserve integers, nulls and PHP's integer map keys.
fn config_value(value: PhpValue) -> ConfigValue {
    match value {
        PhpValue::Null => ConfigValue::Unit,
        PhpValue::Bool(value) => ConfigValue::Bool(value),
        PhpValue::Int(value) => ConfigValue::I64(value),
        PhpValue::Float(value) => ConfigValue::F64(value),
        PhpValue::String(value) => ConfigValue::String(value),
        PhpValue::Bytes(value) => ConfigValue::Bytes(value),
        PhpValue::Array(entries) => {
            if entries.iter().enumerate().all(|(index, (key, _))| {
                matches!(key, PhpArrayKey::Int(value) if usize::try_from(*value) == Ok(index))
            }) {
                ConfigValue::Seq(entries.into_iter().map(|(_, value)| config_value(value)).collect())
            } else {
                ConfigValue::Map(entries.into_iter().map(|(key, value)| {
                    let key = match key {
                        PhpArrayKey::Int(value) => ConfigValue::I64(value),
                        PhpArrayKey::String(value) => ConfigValue::String(value),
                    };
                    (key, config_value(value))
                }).collect())
            }
        }
    }
}

#[cfg(test)]
#[path = "../tests/congif.rs"]
mod tests;
