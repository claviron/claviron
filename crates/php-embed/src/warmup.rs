//! Background OPcache priming through the process's existing PHP engine.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use claviron_runtime::registry::{self, Component, Runnable};
use claviron_runtime::{AppState, RunContext};
use tokio_util::sync::CancellationToken;

use crate::evaluate::{PhpEvaluator, PhpValue};

pub(crate) fn validate(folders: &[PathBuf]) -> Result<(), String> {
    if folders.iter().any(|path| {
        path.as_os_str().is_empty()
            || path.as_os_str().as_encoded_bytes().contains(&0)
            || path.to_str().is_none()
    }) {
        return Err(
            "PHP engine.warmup folders must be nonempty UTF-8 paths without NUL bytes".into(),
        );
    }
    Ok(())
}

pub(crate) fn run(
    folders: &[PathBuf],
    cancellation: CancellationToken,
) -> Result<usize, String> {
    if folders.is_empty() {
        return Ok(0);
    }
    let started = Instant::now();
    let environment = BTreeMap::from([(
        "CLAVIRON_PHP_WARMUP".to_owned(),
        serde_json::to_string(folders).map_err(|error| error.to_string())?,
    )]);
    // The embedded script returns only an integer, so arrays are not an allowed
    // return shape and require no recursive value-decoding budget.
    let value =
        PhpEvaluator::new(0).with_cancellation(cancellation).evaluate_source(
            Path::new("claviron-opcache-warmup.php"),
            include_str!("../php/embeded/warmup.php").as_bytes(),
            &environment,
        )?;
    let count = match value {
        PhpValue::Null => {
            tracing::info!(
                "PHP OPcache warmup skipped: OPcache is unavailable or disabled"
            );
            return Ok(0);
        }
        PhpValue::Int(count) => count,
        _ => {
            return Err("PHP OPcache warmup did not return a file count".into());
        }
    };
    let files = usize::try_from(count)
        .map_err(|_| "PHP OPcache warmup returned an invalid file count")?;
    tracing::info!(
        folders = folders.len(),
        files,
        elapsed_ms = started.elapsed().as_millis(),
        "PHP OPcache warmup completed"
    );
    Ok(files)
}

pub(crate) fn register(
    state: &AppState,
    runtime: Arc<crate::PhpEmbedRuntime>,
) -> registry::Result<()> {
    #[cfg(feature = "tasks")]
    register_task(state, Arc::clone(&runtime))?;
    if !state.is_master() {
        state.registry_ref().register(Arc::new(PhpWarmupService))?;
    }
    let _ = runtime;
    Ok(())
}

/// One process-local background occurrence. All providers are booted before
/// Runnable submission, so this does not depend on Server's ProcessStarted
/// event or on any particular Edge, Runner, or PHP framework integration.
struct PhpWarmupService;

impl Component<AppState> for PhpWarmupService {
    type Deps = Arc<crate::PhpEmbedRuntime>;

    fn name(&self) -> &'static str {
        "PhpWarmupService"
    }

    fn as_runnable(
        self: Arc<Self>
    ) -> Option<Arc<dyn Runnable<AppState, Self::Deps>>> {
        Some(self)
    }
}

#[async_trait::async_trait]
impl Runnable<AppState, Arc<crate::PhpEmbedRuntime>> for PhpWarmupService {
    async fn run(
        self: Arc<Self>,
        runtime: Arc<crate::PhpEmbedRuntime>,
        _state: AppState,
        context: RunContext,
    ) -> registry::Result<()> {
        let result = runtime.warmup(context.cancellation_token().clone()).await;
        if context.is_cancelled() {
            return Ok(());
        }
        if let Err(error) = &result {
            tracing::error!(%error, "PHP OPcache warmup failed");
        }
        result.map(|_| ()).map_err(registry::Error::msg)
    }
}

#[cfg(feature = "tasks")]
fn register_task(
    state: &AppState,
    runtime: Arc<crate::PhpEmbedRuntime>,
) -> registry::Result<()> {
    use claviron_tasks::{TaskConfig, service::TaskService};

    let Some(tasks) = state.registry_ref().resolve::<TaskService>() else {
        return Ok(());
    };
    // Manual timing uses TaskService's declared execution budget. No periodic
    // trigger or retry cadence is invented for this one-shot startup operation.
    let defaults = TaskConfig::from_yaml(
        "scope: workers\ndispatch: each\nconcurrency: 1\nattempts: 1\n",
    )
    .map_err(registry::Error::msg)?;
    tasks.register_builtin(
        "php-warmup",
        defaults,
        move |_state: AppState, cancellation: CancellationToken| {
            let runtime = Arc::clone(&runtime);
            async move {
                runtime
                    .warmup(cancellation)
                    .await
                    .map(|_| ())
                    .map_err(claviron_tasks::Error::other)
            }
        },
    );
    Ok(())
}

#[cfg(test)]
#[path = "../tests/warmup.rs"]
mod tests;
