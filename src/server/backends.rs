//! Cold composition of selected PHP engines; Edge knows no execution engine.

use claviron_edge::{
    Site,
    pipeline::{
        BackendHandler, HostAppRegistry,
        backend::{BackendContext, BackendFactory, BackendPlacement},
    },
};
use claviron_php::{PhpConfig, PhpRunner, PhpRuntime};
use claviron_runtime::{
    AppState,
    registry::{Error, Result},
};
use std::sync::Arc;

pub(super) fn register(state: &AppState) -> Result<()> {
    let registry =
        state.registry_ref().resolve::<HostAppRegistry>().ok_or_else(|| {
            Error::msg("PHP backends require the Edge host registry")
        })?;
    registry.register_backend_factory(Arc::new(PhpFactory))
}

struct PhpFactory;
impl BackendFactory for PhpFactory {
    fn name(&self) -> &'static str {
        "php"
    }
    fn placement(&self) -> BackendPlacement {
        BackendPlacement::AfterFiles
    }
    fn prepare(
        &self,
        context: &BackendContext<'_>,
    ) -> Result<Vec<Arc<dyn BackendHandler>>> {
        let declared = declarations(context.route).map_err(|error| {
            Error::msg(format!(
                "backend 'php' at '{}': {error}",
                context.prefix
            ))
        })?;
        // Every declaration in this scope shares its resolved prefix. Retain
        // last-declared precedence without another runtime mount dispatcher.
        prepare_runtimes(context.state, context.route, &declared)
            .into_iter()
            .rev()
            .map(|runtime| match runtime.php_runner {
                #[cfg(feature = "php_embed")]
                PhpRunner::Script => {
                    claviron_php_embed::backend::prepare_script(
                        context.state,
                        runtime,
                    )
                }
                #[cfg(feature = "php_embed")]
                PhpRunner::Application => {
                    Ok(claviron_php_embed::backend::prepare_application(
                        context.state,
                        runtime,
                    ))
                }
                #[cfg(feature = "php_fcgi")]
                PhpRunner::Fcgi => {
                    claviron_php_fcgi::backend::prepare(context.state, runtime)
                }
            })
            .collect()
    }
}

/// The route's `php:` directive as declared: one mapping or a list of them.
/// The edge keeps it as written; this is where it is decoded and validated.
/// An absent or null directive declares nothing.
fn declarations(
    route: &Site
) -> std::result::Result<Vec<PhpConfig>, serde_yaml::Error> {
    match route.directive("php") {
        None | Some(serde_yaml::Value::Null) => Ok(Vec::new()),
        Some(serde_yaml::Value::Sequence(items)) => {
            items.iter().cloned().map(serde_yaml::from_value).collect()
        }
        Some(one) => Ok(vec![serde_yaml::from_value(one.clone())?]),
    }
}

// Resolve route mappings and PHP defaults before selecting runner owners.
fn prepare_runtimes(
    state: &AppState,
    config: &Site,
    declared: &[PhpConfig],
) -> Vec<PhpRuntime> {
    declared
        .iter()
        .map(|template| {
            let mut runtime = PhpRuntime::from_config(
                state,
                template,
                config.files.mapping.as_ref().map_or_else(
                    || config.root(),
                    |mapping| mapping.directory().to_path_buf(),
                ),
                config.indexes(),
            );
            runtime.path = config
                .files
                .mapping
                .as_ref()
                .map(|mapping| mapping.prefix().clone());
            if let Some(mapping) = &config.files.mapping {
                runtime.mapping =
                    Some(claviron_http::config::mapping::RouteMapping::new(
                        mapping.prefix().clone(),
                        runtime.dir.clone(),
                    ));
            }
            runtime
        })
        .collect()
}

#[cfg(test)]
#[path = "../../tests/server/backends.rs"]
mod tests;
