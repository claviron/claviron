//! A service of this application's own, run and managed by Claviron.
//!
//! If you come from Python: this is what a FastAPI `lifespan` or a background
//! task started on startup would be, except that the process does not own it
//! alone. Claviron starts it, stops it, restarts it and reloads it — from a
//! signal, or from Claviron's management API and the GUI on it — and
//! this file only says what each of those means for this service.
//!
//! - `boot()` runs once, before anything serves.
//! - `run()` ([`Runnable`]) is the service's life. One call is one
//!   *generation*; it ends when its [`RunContext`] is cancelled — at
//!   shutdown, or when the service alone is stopped or restarted. A restart
//!   is a new generation: a new call to `run()` with a new context.
//! - `reload()` ([`Reloadable`]) refreshes the service in place, without
//!   stopping it: `SIGHUP` to the master reloads every service in every
//!   worker.
//! - `finalize()` ([`Finalizable`]) runs once at the end, after every `run()`
//!   has returned: the place to leave behind what the next start needs.
//!
//! A counter needs none of that: this is an example, written to show the
//! shape a real service takes — one that opens a pool, drains a queue, or
//! holds a connection. The count itself is Orbit's keyed counter `"hits"`,
//! one value for every worker, and Python reaches the same one with no
//! service at all:
//!
//! ```python
//! import claviron
//! claviron.counter("hits").increment()
//! ```
//!
//! What the service adds is its lifecycle. The master (or a single process)
//! keeps the count in `storage/counter` under the application root:
//! `finalize()` writes it once every worker has stopped, and the next
//! `boot()` puts it back before any worker starts. A reload starts the count
//! again from zero.

use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use claviron::orbit::{Counter, CounterProvider};
use claviron::registry::Error;
use claviron::{
    AppState, Bootable, Component, Finalizable, Reloadable, RunContext,
    Runnable,
};

/// Where the count is kept between runs, under the application root.
const COUNTER_FILE: &str = "storage/counter";

/// The key in the fleet's counter table; `claviron.counter("hits")` in Python.
const KEY: &str = "hits";

#[derive(Default)]
pub(crate) struct CounterService {
    /// The fleet's table, resolved at boot.
    table: OnceLock<Arc<Counter>>,
}

impl CounterService {
    fn table(&self) -> claviron::registry::Result<&Counter> {
        self.table
            .get()
            .map(Arc::as_ref)
            .ok_or_else(|| Error::msg("the counter table is not open"))
    }

    pub(crate) fn hit(&self) -> claviron::registry::Result<i64> {
        self.table()?
            .increment(KEY, 1)
            .map_err(|error| Error::msg(error.to_string()))
    }

    pub(crate) fn count(&self) -> claviron::registry::Result<i64> {
        let hits = self
            .table()?
            .get(KEY)
            .map_err(|error| Error::msg(error.to_string()))?;
        Ok(hits.unwrap_or(0))
    }

    /// The count's file; `None` in a worker, which keeps none.
    fn file(state: &AppState) -> Option<PathBuf> {
        (!state.is_worker()).then(|| state.config_dir().join(COUNTER_FILE))
    }

    pub(crate) fn register(state: &AppState) -> claviron::registry::Result<()> {
        state.registry_ref().register(Arc::new(Self::default()))?;
        Ok(())
    }
}

#[async_trait]
impl Component<AppState> for CounterService {
    type Deps = (Arc<CounterProvider>,);

    fn name(&self) -> &'static str {
        "CounterService"
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<AppState, Self::Deps>> {
        Some(self)
    }

    /// The same for `reload()`: without it a reload passes this service by.
    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        Some(self)
    }

    /// And for `finalize()`.
    fn as_finalizable(&self) -> Option<&dyn Finalizable<AppState>> {
        Some(self)
    }

    /// Without this the runtime never spawns `run()`: implementing `Runnable`
    /// declares the capability, this hook hands it over.
    fn as_runnable(
        self: Arc<Self>
    ) -> Option<Arc<dyn Runnable<AppState, Self::Deps>>> {
        Some(self)
    }
}

#[async_trait]
impl Runnable<AppState, <Self as Component<AppState>>::Deps>
    for CounterService
{
    async fn run(
        self: Arc<Self>,
        _deps: <Self as Component<AppState>>::Deps,
        _state: AppState,
        context: RunContext,
    ) -> claviron::registry::Result<()> {
        // A generation lasts until it is cancelled. There is no interval here
        // on purpose: nothing needs waking, so nothing wakes. A service with
        // work of its own selects on `cancelled()` beside it, or hands
        // `context.cancellation_token()` to a server's graceful shutdown.
        context.cancelled().await;
        tracing::info!(hits = self.count()?, "counter generation ended");
        Ok(())
    }
}

#[async_trait]
impl Reloadable<AppState> for CounterService {
    /// A reload keeps the generation running and starts the count again. A
    /// real service rereads its settings here and swaps them in; in-flight
    /// work keeps what it started with.
    ///
    /// The count is the fleet's, so one process resets it: the master, or a
    /// single process. A worker reloads with nothing to do.
    async fn reload(
        &self,
        state: &AppState,
    ) -> claviron::registry::Result<()> {
        if state.is_worker() {
            return Ok(());
        }
        let hits = self.count()?;
        self.table()?
            .reset(KEY)
            .map_err(|error| Error::msg(error.to_string()))?;
        tracing::info!(hits, "counter reset by reload");
        Ok(())
    }
}

#[async_trait]
impl Finalizable<AppState> for CounterService {
    /// Leave the count for the next `boot()`. Written beside the file and
    /// renamed over it, so a stop halfway never leaves half a number.
    async fn finalize(
        &self,
        state: &AppState,
    ) -> claviron::registry::Result<()> {
        let Some(file) = Self::file(state) else {
            return Ok(());
        };
        let hits = self.count()?;
        let written = async {
            if let Some(directory) = file.parent() {
                tokio::fs::create_dir_all(directory).await?;
            }
            let partial = file.with_extension("partial");
            tokio::fs::write(&partial, hits.to_string()).await?;
            tokio::fs::rename(&partial, &file).await
        };
        written.await.map_err(|error| Error::Finalize {
            name: self.name(),
            source: error.into(),
        })?;
        tracing::info!(hits, file = %file.display(), "counter saved");
        Ok(())
    }
}

#[async_trait]
impl Bootable<AppState, <Self as Component<AppState>>::Deps>
    for CounterService
{
    /// Open the table, and in the master pick up the count the last run
    /// left. No file is a first run; a file that is not a count is an error,
    /// not a zero.
    async fn boot(
        &self,
        (provider,): <Self as Component<AppState>>::Deps,
        state: &AppState,
    ) -> claviron::registry::Result<()> {
        let table = provider
            .handle()
            .ok_or_else(|| Error::msg("CounterProvider is not registered"))?;
        let table = self.table.get_or_init(|| table);
        let Some(file) = Self::file(state) else {
            return Ok(());
        };
        let text = match tokio::fs::read_to_string(&file).await {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(());
            }
            Err(error) => {
                return Err(Error::Boot {
                    name: self.name(),
                    source: error.into(),
                });
            }
        };
        let hits: i64 = text.trim().parse().map_err(|error| Error::Boot {
            name: self.name(),
            source: Box::new(error),
        })?;
        let restored =
            table.reset(KEY).and_then(|()| table.increment(KEY, hits));
        restored.map_err(|error| Error::Boot {
            name: self.name(),
            source: Box::new(error),
        })?;
        tracing::info!(hits, file = %file.display(), "counter restored");
        Ok(())
    }
}
