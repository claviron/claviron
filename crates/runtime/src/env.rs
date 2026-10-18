//! Worker-level process env hygiene.
//!
//! Two pieces:
//!
//! 1. [`EnvSnapshot`] — frozen snapshot of the libc env, captured at
//!    `State::initialize`. Stored on `State` (not the registry); every
//!    provider that resolves `${ENV_KEY}` placeholders reads through
//!    `state.env_snapshot()` so the same code path serves first boot
//!    AND SIGHUP reload (the snapshot survives the scrub).
//!
//! 2. [`scrub_process_env`] — runs once per traffic-serving process
//!    (standalone or worker, NOT master) right after `boot_all()`
//!    completes. From that point libc `environ` carries only OS /
//!    runtime essentials; claviron secrets cannot leak to embedded
//!    runtimes (libphp.so, libpq, vendor SDKs) via libc `getenv`.
//!
//! Master is intentionally excluded from scrub: worker commands inherit its
//! full process environment. Scrubbing on master would leave workers without
//! their boot-time secrets.
//!
//! libc semantics: `unsetenv` does not cache; subsequent `getenv`
//! walks the live `environ` and returns NULL for missing keys.
//! Scrub-then-use is therefore safe — provided every consumer reads
//! at boot (or via the captured snapshot), which is the convention
//! claviron enforces.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

/// The environment a production process runs under. It is only ever chosen
/// explicitly (`--production`), never by name, and nothing infers it.
pub const PRODUCTION: &str = "production";

/// The environment a process runs under when none is selected.
pub const LOCAL: &str = "local";

/// Startup environment recipe and its first evaluated snapshot.
///
/// Server bootstrap prepares this before logging and state construction. Congif
/// owns subsequent refreshes through the retained Dotenw store and handle;
/// loading files never writes their values into the process environment.
pub struct ConfigEnvironment {
    pub(crate) snapshot: EnvSnapshot,
    pub(crate) store: dotenw::EnvStore,
    pub(crate) handle: dotenw::EnvHandle,
}

impl ConfigEnvironment {
    /// Validate a deployment name without reading environment files.
    pub fn validate_environment(environment: &str) -> dotenw::Result<()> {
        dotenw::EnvStore::new().with_environment(environment).map(|_| ())
    }

    /// Capture inherited process values and select `.env` plus its environment
    /// variant from the application working directory.
    pub fn load(
        hostname: &str,
        environment: &str,
        env_file: &Path,
    ) -> dotenw::Result<Self> {
        let inherited = EnvSnapshot::capture(hostname);
        Self::from_snapshot(
            dotenw::Snapshot::new(inherited.values().clone()),
            environment,
            Some(env_file),
        )
    }

    /// Prepare from a resolved base. `None` preserves configless embedders;
    /// `Some(path)` selects existing `path` and `path.<environment>` files.
    /// Missing optional files are skipped; invalid existing files fail startup.
    pub fn from_snapshot(
        base: dotenw::Snapshot,
        environment: &str,
        env_file: Option<&Path>,
    ) -> dotenw::Result<Self> {
        let store =
            dotenw::EnvStore::from_base(base)?.with_environment(environment)?;
        let mut recipe = dotenw::Environment::new();
        if let Some(path) = env_file {
            recipe =
                recipe.with_environment_file(path, dotenw::MergeMode::Override);
        }
        let handle = store.register(recipe)?;
        let snapshot =
            EnvSnapshot { map: Arc::new(handle.snapshot()?.as_ref().clone()) };
        Ok(Self { snapshot, store, handle })
    }

    /// Selected deployment name, independent of the machine hostname.
    pub fn environment(&self) -> &str {
        self.store.environment()
    }

    /// Startup values for consumers that need configuration before state exists.
    pub fn snapshot(&self) -> &EnvSnapshot {
        &self.snapshot
    }
}

/// Frozen snapshot of the process env, captured at `State::initialize`
/// — before any provider runs and before the worker scrub. Single
/// source of truth for `${ENV_KEY}` resolution: `DbService`,
/// `VhostResolver`, `AuthService`, … resolve placeholders through
/// `state.env_snapshot()` instead of `std::env::var`, so the same
/// code path works on first boot AND on SIGHUP reload (the worker
/// scrub erases the libc copy but leaves this snapshot intact).
///
/// This legacy snapshot is **frozen for the process lifetime**. Congif bindings
/// use the live Dotenw handle from [`ConfigEnvironment`] instead; refreshing its
/// files does not change this already captured view or the process environment.
#[derive(Debug, Clone)]
pub struct EnvSnapshot {
    map: Arc<HashMap<String, String>>,
}

impl EnvSnapshot {
    /// Capture the live libc env into a snapshot. Called by
    /// [`State::initialize`] at construction (before any provider
    /// touches env) and by [`scrub_process_env`] before the wipe.
    pub(crate) fn capture(hostname: &str) -> Self {
        // Supervisor bootstrap keys are an internal parent/child protocol,
        // not application configuration. Keep them out of `${ENV_KEY}`
        // interpolation even though they are present in the worker's libc
        // environment during early boot.
        let mut map: HashMap<String, String> = std::env::vars()
            .filter(|(key, _)| !key.starts_with("CLAVIRON_INTERNAL_"))
            .collect();
        // The machine's own name, so a config can name a per-host file
        // (`env/app.${CLAVIRON_HOSTNAME}.env`) without the launcher having to
        // pass it in — systemd, the script and a bare run all behave alike.
        // An explicit value in the environment still wins.
        map.entry("CLAVIRON_HOSTNAME".to_owned())
            .or_insert_with(|| hostname.to_owned());
        Self { map: Arc::new(map) }
    }

    /// Look up a key from the snapshot. Mirrors `std::env::var`'s
    /// semantics minus the unicode-error case (we always store
    /// `String`).
    pub fn get(
        &self,
        key: &str,
    ) -> Option<String> {
        self.map.get(key).cloned()
    }

    /// Borrow the frozen variables for configuration parsing without reading
    /// or repopulating the scrubbed process environment.
    pub fn values(&self) -> &HashMap<String, String> {
        &self.map
    }

    /// Number of captured keys.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// True when nothing was captured. Should never happen in
    /// practice — `State::initialize` always runs in a process with
    /// at least the OS-supplied env baseline.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

/// Keys we leave behind after scrub. Strictly OS / language-runtime
/// essentials — not application secrets.
pub const RUNTIME_ESSENTIALS: &[&str] = &[
    // PHP / runtime essentials
    "PATH",
    "HOME",
    "USER",
    "LOGNAME",
    "SHELL",
    "TMPDIR",
    "TMP",
    // Dynamic linker search paths. ld.so caches these at process
    // startup, but glibc/musl `dlopen()` re-reads them on every
    // call — so PHP extensions / libpq / libcurl that lazy-dlopen
    // backend plugins (Kerberos, TLS providers, ICU data) need
    // these alive past worker scrub. Not secrets; deploy-path
    // config that users may pin.
    "LD_LIBRARY_PATH",
    "DYLD_LIBRARY_PATH",
    "DYLD_FALLBACK_LIBRARY_PATH",
    // Locale family — date/time/intl, libc, libpq error messages
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "LC_MESSAGES",
    "LC_COLLATE",
    "LC_TIME",
    "LC_NUMERIC",
    "LC_MONETARY",
    "TZ",
    // PHP-specific knobs claviron itself sets
    "PHPRC",
    "PHP_INI_SCAN_DIR",
    // libpq client encoding (not a secret; users may pin this)
    "PGCLIENTENCODING",
];

/// Wipe libc of everything not in `allow`. The pre-scrub view is
/// already preserved on `State::env_snapshot()` (captured at
/// construction), so this function only performs the wipe.
///
/// # Safety
///
/// `std::env::remove_var` is `unsafe` since Rust 1.85 because mutating
/// `environ` while another thread reads it is UB. The caller must
/// guarantee no concurrent env reads — typically by invoking this
/// before any non-claviron runtime (libphp.so, libpq, etc.) is alive
/// and before any background thread that depends on env values is
/// spawned.
pub fn scrub_process_env(allow: &[&str]) {
    let allow: HashSet<&str> = allow.iter().copied().collect();
    let keys: Vec<String> = std::env::vars().map(|(k, _)| k).collect();
    for key in keys {
        if !allow.contains(key.as_str()) {
            // SAFETY: documented invariant above — caller ensures
            // single-threaded read access to libc `environ` for the
            // duration of this loop.
            unsafe { std::env::remove_var(&key) };
        }
    }
}

#[cfg(test)]
#[path = "../tests/environment.rs"]
mod tests;
