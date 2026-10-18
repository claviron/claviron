//! Execution-owned environments, not application management or configuration.
//!
//! One Dotenw store shares native watching. Owners select `changed()` inside
//! their existing runnable; request-scoped execution takes a snapshot instead.

use std::collections::BTreeMap;
use std::future::{Future, poll_fn};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use claviron_runtime::AppState;
use claviron_runtime::congif::{self, dotenw};
use futures_util::{FutureExt, StreamExt, stream::FuturesUnordered};

/// Preparation capability that does not keep the application registry alive.
#[derive(Clone)]
pub struct EnvironmentContext {
    store: dotenw::EnvStore,
    base: dotenw::Environment,
}

impl EnvironmentContext {
    pub fn new(state: &AppState) -> congif::Result<Self> {
        Ok(Self {
            store: state.config_reader().environments().clone(),
            base: state.program_recipe(Vec::<PathBuf>::new(), [])?,
        })
    }

    /// Files and their deployment variants in order, followed by inline values.
    pub fn observe(
        &self,
        files: &[PathBuf],
        values: impl IntoIterator<Item = (String, String)>,
    ) -> congif::Result<ExecutionEnvironment> {
        Ok(ExecutionEnvironment(Mutex::new(
            self.store.observe(self.recipe(files, values)?)?,
        )))
    }

    fn recipe(
        &self,
        files: &[PathBuf],
        values: impl IntoIterator<Item = (String, String)>,
    ) -> congif::Result<dotenw::Environment> {
        let mut recipe = self.base.clone();
        for (index, path) in files.iter().enumerate() {
            if index == 0 && !path.is_file() {
                return Err(congif::Error::msg(format!(
                    "env_file {} does not exist",
                    path.display()
                )));
            }
            recipe =
                recipe.with_environment_file(path, dotenw::MergeMode::Override);
        }
        let values: dotenw::Values = values.into_iter().collect();
        if !values.is_empty() {
            recipe = recipe.with_values(values, dotenw::MergeMode::Override);
        }
        Ok(recipe)
    }
}

#[cfg(test)]
#[path = "../tests/environment.rs"]
mod tests;

/// Dropping the last owner unregisters its environment, not its peers'.
pub struct ExecutionEnvironment(Mutex<dotenw::EnvNotification>);

impl ExecutionEnvironment {
    /// Last valid values. No I/O and no process-environment mutation.
    pub fn snapshot(&self) -> congif::Result<dotenw::Snapshot> {
        let notification = self.0.lock().map_err(|_| {
            congif::Error::msg("execution environment lock poisoned")
        })?;
        Ok(notification.snapshot()?)
    }

    /// Cancel-safe wait; no lock is held across an await and no task is spawned.
    pub async fn changed(&self) -> congif::Result<dotenw::Snapshot> {
        poll_fn(|cx| {
            let mut notification = match self.0.lock() {
                Ok(notification) => notification,
                Err(_) => {
                    return std::task::Poll::Ready(Err(congif::Error::msg(
                        "execution environment lock poisoned",
                    )));
                }
            };
            let next = notification.notified();
            futures_util::pin_mut!(next);
            next.poll(cx).map(|result| result.map_err(congif::Error::from))
        })
        .await
    }

    /// Request owners have no renewal loop. Report pending preparation errors
    /// here, then pin one immutable environment for the whole request.
    pub fn request_snapshot(&self) -> congif::Result<dotenw::Snapshot> {
        if let Some(Err(error)) = self.changed().now_or_never() {
            tracing::error!(%error, "request environment replacement rejected; previous snapshot retained");
        }
        self.snapshot()
    }
}

/// Multiplex environments inside one existing application-owner future.
#[derive(Default)]
pub struct ApplicationEnvironments(BTreeMap<String, Arc<ExecutionEnvironment>>);

impl ApplicationEnvironments {
    /// Identify the subscription that produced a change. Dynamic catalogs
    /// can discard a notification from a recipe they have since replaced.
    pub fn get(
        &self,
        name: &str,
    ) -> Option<&Arc<ExecutionEnvironment>> {
        self.0.get(name)
    }

    pub fn insert(
        &mut self,
        name: String,
        environment: ExecutionEnvironment,
    ) {
        self.0.insert(name, Arc::new(environment));
    }

    pub fn insert_shared(
        &mut self,
        name: String,
        environment: Arc<ExecutionEnvironment>,
    ) {
        self.0.insert(name, environment);
    }

    pub fn snapshot(
        &self,
        name: &str,
    ) -> congif::Result<dotenw::Snapshot> {
        self.0
            .get(name)
            .ok_or_else(|| {
                congif::Error::msg("application environment not prepared")
            })?
            .snapshot()
    }

    pub async fn changed(&self) -> (String, congif::Result<dotenw::Snapshot>) {
        let waits: FuturesUnordered<_> = self
            .0
            .iter()
            .map(|(name, environment)| async move {
                (name.clone(), environment.changed().await)
            })
            .collect();
        futures_util::pin_mut!(waits);
        match waits.next().await {
            Some(change) => change,
            None => std::future::pending().await,
        }
    }
}
