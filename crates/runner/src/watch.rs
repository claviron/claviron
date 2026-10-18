//! Development's reload: act when the application's source changes.
//!
//! What counts as source and what a change does are the owner's — a Python
//! runner rolls its workers over, since an interpreter is not reloaded in
//! place; a PHP runner restarts its application in the process. This is the
//! watching: native notifications only (FSEvents, inotify, kqueue), one
//! pending wake however many files a save touches, and a platform that would
//! fall back to polling refused, because a poll is a guess at how often to
//! look.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use claviron_runtime::{AppState, Component, RunContext, Runnable, registry};
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::Notify;

/// What a change does, said by the owner: `Ok` with what was asked for, for
/// the log, or why nothing could be. It is awaited before the next change is
/// taken, so a burst of saves during a restart is one more restart, not many.
pub type Reaction = Arc<
    dyn Fn(
            AppState,
        )
            -> Pin<Box<dyn Future<Output = Result<String, String>> + Send>>
        + Send
        + Sync,
>;

/// What the owner calls source.
#[derive(Clone, Debug)]
pub struct Source {
    /// File extensions whose change matters (`py`; `php`).
    pub extensions: &'static [&'static str],
    /// Names under the root that are never source (`.venv`, `vendor`).
    pub skip: &'static [&'static str],
    /// What is watched when the launch names nothing: these, relative to the
    /// root, or — empty — every directory of the root but the skipped ones,
    /// and the root's own files.
    pub defaults: &'static [&'static str],
}

pub struct SourceWatcher {
    root: PathBuf,
    paths: Vec<PathBuf>,
    source: Source,
    reaction: Reaction,
}

impl SourceWatcher {
    /// `paths` are what the launch named, relative to `root` or absolute;
    /// empty takes the owner's defaults.
    pub fn new(
        root: PathBuf,
        paths: Vec<PathBuf>,
        source: Source,
        reaction: Reaction,
    ) -> Self {
        Self { root, paths, source, reaction }
    }

    fn resolve(&self) -> std::io::Result<Vec<(PathBuf, RecursiveMode)>> {
        let named: Vec<PathBuf> = if !self.paths.is_empty() {
            self.paths.clone()
        } else if !self.source.defaults.is_empty() {
            self.source.defaults.iter().map(PathBuf::from).collect()
        } else {
            return self.every_directory();
        };
        Ok(named
            .into_iter()
            .map(
                |path| {
                    if path.is_absolute() { path } else { self.root.join(path) }
                }
            )
            // A default the application does not have is not an error.
            .filter(|path| !self.paths.is_empty() || path.exists())
            .map(|path| {
                let mode = if path.is_dir() {
                    RecursiveMode::Recursive
                } else {
                    RecursiveMode::NonRecursive
                };
                (path, mode)
            })
            .collect())
    }

    /// The root without recursion, for a directory added later, and each of
    /// its directories but the skipped and hidden ones, recursively.
    fn every_directory(
        &self
    ) -> std::io::Result<Vec<(PathBuf, RecursiveMode)>> {
        let mut paths = vec![(self.root.clone(), RecursiveMode::NonRecursive)];
        for entry in std::fs::read_dir(&self.root)? {
            let entry = entry?;
            let name = entry.file_name();
            if self.source.skip.iter().any(|skip| name == *skip)
                || name.to_string_lossy().starts_with('.')
            {
                continue;
            }
            if entry.file_type()?.is_dir() {
                paths.push((entry.path(), RecursiveMode::Recursive));
            }
        }
        Ok(paths)
    }

    fn start(
        &self
    ) -> registry::Result<(RecommendedWatcher, Arc<Changes>, Vec<PathBuf>)>
    {
        if matches!(
            RecommendedWatcher::kind(),
            notify::WatcherKind::PollWatcher
        ) {
            return Err(failure(
                "watch needs native filesystem notifications on this platform",
            ));
        }
        let paths = self.resolve().map_err(|error| {
            failure(format!("watch: read {}: {error}", self.root.display()))
        })?;
        let changes = Arc::new(Changes::default());
        let handler = Handler {
            changes: Arc::clone(&changes),
            source: self.source.clone(),
        };
        let mut watcher =
            RecommendedWatcher::new(handler, notify::Config::default())
                .map_err(failure)?;
        for (path, mode) in &paths {
            watcher.watch(path, *mode).map_err(|error| {
                failure(format!("watch {}: {error}", path.display()))
            })?;
        }
        Ok((
            watcher,
            changes,
            paths.into_iter().map(|(path, _)| path).collect(),
        ))
    }
}

/// A change worth acting on: source, or a directory appearing, disappearing
/// or moving, which can carry source without an event per file.
fn relevant(
    event: &Event,
    source: &Source,
) -> bool {
    if matches!(event.kind, EventKind::Access(_)) {
        return false;
    }
    let structural = matches!(
        event.kind,
        EventKind::Create(notify::event::CreateKind::Folder)
            | EventKind::Remove(notify::event::RemoveKind::Folder)
            | EventKind::Modify(notify::event::ModifyKind::Name(_))
    );
    event.need_rescan()
        || event.paths.iter().any(|path| {
            let skipped = path.components().any(|part| {
                let part = part.as_os_str();
                source.skip.iter().any(|skip| part == *skip)
            });
            !skipped && (structural || is_source(path, source))
        })
}

fn is_source(
    path: &Path,
    source: &Source,
) -> bool {
    path.extension().is_some_and(|extension| {
        source.extensions.iter().any(|wanted| extension == *wanted)
    })
}

#[derive(Default)]
struct Changes {
    wake: Notify,
    error: Mutex<Option<String>>,
}

/// The watcher's side of [`Changes`], called on its own thread.
struct Handler {
    changes: Arc<Changes>,
    source: Source,
}

impl notify::EventHandler for Handler {
    fn handle_event(
        &mut self,
        event: notify::Result<Event>,
    ) {
        match event {
            Ok(event) if relevant(&event, &self.source) => {
                self.changes.wake.notify_one()
            }
            Ok(_) => {}
            Err(error) => {
                self.changes
                    .error
                    .lock()
                    .unwrap()
                    .get_or_insert_with(|| error.to_string());
                self.changes.wake.notify_one();
            }
        }
    }
}

impl Component<AppState> for SourceWatcher {
    type Deps = ();

    fn name(&self) -> &'static str {
        "SourceWatcher"
    }

    fn as_runnable(
        self: Arc<Self>
    ) -> Option<Arc<dyn Runnable<AppState, Self::Deps>>> {
        Some(self)
    }
}

#[async_trait]
impl Runnable<AppState> for SourceWatcher {
    async fn run(
        self: Arc<Self>,
        _deps: (),
        state: AppState,
        context: RunContext,
    ) -> registry::Result<()> {
        let (watcher, changes, paths) = self.start()?;
        tracing::info!(
            ?paths,
            "watching the application's source; a change restarts it"
        );
        loop {
            tokio::select! {
                biased;
                () = context.cancelled() => break,
                () = changes.wake.notified() => {}
            }
            if let Some(error) = changes.error.lock().unwrap().take() {
                drop(watcher);
                return Err(failure(format!("source watch failed: {error}")));
            }
            // A burst of saves is one wake: the permit is held until taken.
            match (self.reaction)(state.clone()).await {
                Ok(asked) => {
                    tracing::info!(%asked, "source changed; restarting")
                }
                Err(error) => {
                    tracing::warn!(%error, "source changed; restart not requested")
                }
            }
        }
        drop(watcher);
        Ok(())
    }
}

fn failure(error: impl std::fmt::Display) -> registry::Error {
    registry::Error::msg(error.to_string())
}
