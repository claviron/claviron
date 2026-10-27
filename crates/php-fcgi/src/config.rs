use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use claviron_helpers::de::{
    deserialize_duration, deserialize_optional_socket_addr,
};
use serde::Deserialize;

pub(crate) type FpmTable = HashMap<String, Arc<FpmClient>>;

/// Cadence for the periodic stale-connection sweep over every pooled
/// entry, run by the `fcgi_housekeep` task.
///
/// It lives in `fpm.yaml` rather than in `tasks.yaml` because it is this
/// crate's operating policy, and because the number it has to be checked
/// against is in this file: the rule is `interval <= idle_timeout / 2`
/// (ARCHITECTURE §3.3), so that claviron decides a connection is stale
/// before FPM's own `pm.process_idle_timeout` invalidates the worker on
/// the other side. Two files could not check that against each other.
///
/// One cadence covers every pooled entry. Per-entry cadences would mean
/// a task name each, and the sweep costs microseconds even when it runs
/// tighter than the slowest pool needs — so the interval is set by the
/// tightest `idle_timeout` in the file and the rest are swept early.
#[derive(Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct HousekeepConfig {
    #[serde(default = "default_housekeep_enabled")]
    pub(crate) enabled: bool,

    #[serde(
        default = "default_housekeep_interval",
        deserialize_with = "deserialize_duration"
    )]
    pub(crate) interval: Duration,
}

fn default_housekeep_enabled() -> bool {
    true
}

/// Half of the `idle_timeout` the sample configuration uses, which is
/// the rule stated as a number. An entry whose own `idle_timeout` is
/// tighter than twice this is warned about when the config loads.
fn default_housekeep_interval() -> Duration {
    Duration::from_secs(15)
}

impl Default for HousekeepConfig {
    fn default() -> Self {
        Self {
            enabled: default_housekeep_enabled(),
            interval: default_housekeep_interval(),
        }
    }
}

/// `fpm.yaml` whole: the FPM catalog, the sweep cadence and the metrics
/// policy. `PhpFcgiService` owns it through Congif; the metrics providers
/// and the housekeeping task take their part from the prepared document, so
/// the file is read once and every section is checked against the others.
/// An absent file is the empty document: no pool, metrics off.
#[derive(Default, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FpmDocument {
    #[serde(default)]
    pub(crate) fpm: FpmTable,
    /// Parsed with the catalog so the sweep cadence can be checked against
    /// the `idle_timeout` values it is bound to.
    #[serde(default)]
    pub(crate) housekeep: HousekeepConfig,
    #[cfg(feature = "metrics")]
    #[serde(default)]
    pub(crate) metrics: Option<crate::metrics::config::FpmMetricsConfig>,
    /// Accepted without the `metrics` feature so one file serves every build.
    #[cfg(not(feature = "metrics"))]
    #[serde(default)]
    metrics: Option<Ignored>,
}

/// A section another build reads: accepted whatever it holds.
#[cfg(not(feature = "metrics"))]
#[derive(Default, PartialEq)]
struct Ignored;

#[cfg(not(feature = "metrics"))]
impl<'de> serde::Deserialize<'de> for Ignored {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D
    ) -> Result<Self, D::Error> {
        serde::de::IgnoredAny::deserialize(deserializer).map(|_| Self)
    }
}

/// One `fpm.yaml` entry — an FPM endpoint plus the budgets that apply to
/// it however requests reach it.
///
/// `connect_timeout` and `exec_timeout` sit here, not under `pool:`, on
/// purpose: both apply to one-shot entries too, which have no `pool:`
/// block to put them in.
#[derive(Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FpmClient {
    pub(crate) unix: Option<PathBuf>,
    #[serde(default, deserialize_with = "deserialize_optional_socket_addr")]
    pub(crate) tcp: Option<SocketAddr>,

    /// TCP/Unix dial deadline. Applies to BOTH pooled (each fresh dial
    /// inside the pool) and one-shot (every request dials a new socket)
    /// variants. Independent of `acquire_timeout`. Default: 2s.
    #[serde(default, deserialize_with = "deserialize_duration")]
    pub(crate) connect_timeout: Option<Duration>,

    /// Total cap on the FCGI request/response cycle for the *buffered*
    /// path. Pooled+buffered uses it after acquire; one-shot+buffered
    /// uses it after dial. Streaming variants ignore this — long
    /// transfers shouldn't be wall-clock capped here. Default: 30s.
    #[serde(default, deserialize_with = "deserialize_duration")]
    pub(crate) exec_timeout: Option<Duration>,

    /// Cap on the wait for FPM's response *headers*, separately from the
    /// body that follows them.
    ///
    /// It exists for the streaming path, which deliberately puts no
    /// wall-clock cap on a transfer — an SSE stream or a large download
    /// is meant to run long — and so had no bound at all on the wait
    /// before it. An FPM that accepts the request and then says nothing
    /// held the caller and the connection for ever.
    ///
    /// The buffered path is already covered by `exec_timeout`, which
    /// bounds the whole cycle. Defaults to `exec_timeout` for the same
    /// reason: the same budget the buffered path gives everything,
    /// applied to just the head.
    #[serde(default, deserialize_with = "deserialize_duration")]
    pub(crate) head_timeout: Option<Duration>,

    #[cfg(feature = "metrics")]
    /// Optional FPM `pm.status_path` for metrics collection. Applies to the
    /// FPM entry regardless of whether request traffic uses keep-alive pooling
    /// or one-shot dials.
    #[serde(default)]
    pub(crate) status_path: Option<String>,

    /// Placeholder so an operator config that configures metrics still
    /// parses in a build without the `metrics` feature. Keeps the record
    /// strict without making the schema shape feature-dependent.
    #[cfg(not(feature = "metrics"))]
    #[allow(dead_code)]
    #[serde(default, rename = "status_path")]
    pub(crate) status_path_ignored: serde::de::IgnoredAny,

    /// `pool:` block presence selects the connection mode:
    /// * `Some` → keep-alive `FcgiPool`, `execute()` / `execute_stream()`.
    /// * `None` → one-shot dial per request, `execute_once()` /
    ///   `execute_once_stream()`.
    pub(crate) pool: Option<PoolConfig>,
}

impl FpmClient {
    pub fn connect_timeout(&self) -> Duration {
        self.connect_timeout.unwrap_or(Duration::from_secs(2))
    }

    pub fn exec_timeout(&self) -> Duration {
        self.exec_timeout.unwrap_or(Duration::from_secs(30))
    }

    pub fn head_timeout(&self) -> Duration {
        self.head_timeout.unwrap_or_else(|| self.exec_timeout())
    }
}

/// Claviron's client-side connection pool tuning. These control how claviron
/// talks to PHP-FPM; they are NOT FPM server settings (don't try to mirror
/// `pm.start_servers` etc. — different semantics, different layer). Sizing
/// rule of thumb: keep `max_live` under `FPM.max_children / server.workers`
/// so total claviron-side connections stay within FPM's worker capacity.
///
/// Three-tier sizing, mirroring FPM's process-manager vocabulary:
///   `min_live` — warm minimum (FPM `pm.start_servers` / `pm.min_spare_servers`):
///                replenisher floor; `live < min_live` triggers refill. It
///                is a floor on `live`, not on the idle queue.
///   `max_idle` — idle queue cap (FPM `pm.max_spare_servers`): on lease drop
///                a conn returns to idle only while `idle_count < max_idle`,
///                otherwise it closes and frees the live slot.
///   `max_live` — hard ceiling (FPM `pm.max_children`): `acquire()` queues
///                callers up to `acquire_timeout` once reached.
/// Bursty conns (SSE, streaming, chunked uploads) naturally land in the
/// `max_idle..max_live` zone and drain back without bloating the warm pool.
#[derive(Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct PoolConfig {
    /// Warm minimum: the replenisher maintains `live` at this floor.
    /// Reached on first touch, not at boot — the pool is instantiated
    /// lazily and the first acquire dials, with the replenisher filling
    /// the rest behind it. (FPM analog: `pm.start_servers`.)
    ///
    /// The floor is on `live`, not on the idle queue: a leased connection
    /// satisfies it, so under sustained load the queue can be empty while
    /// the floor is met. That is deliberate — it is what keeps an acquire
    /// from churning a dial per request — and `min_idle`, the name this
    /// field carried until the behaviour was written down, said the
    /// opposite. The old spelling is still accepted because operator
    /// config is shared between hosts and may not be updated in the same
    /// step as the binary; `claviron-upstream` still spells it that way
    /// and means the same thing.
    #[serde(alias = "min_idle")]
    pub min_live: usize,

    /// Idle queue cap: on lease drop, conns past this count close
    /// instead of returning to idle. (FPM analog: `pm.max_spare_servers`.)
    pub max_idle: usize,

    /// Total cap on live connections (warm + burst).
    /// (FPM analog: `pm.max_children`.)
    pub max_live: usize,

    /// Optional FPM `ping.path` (e.g. `/ping`). When set, every
    /// freshly-opened connection (warm-up + replenisher) is validated
    /// end-to-end with a single FCGI ping before it is published to
    /// the idle queue. A successful `pong` proves the FPM worker on
    /// the other side is live and the keep-alive socket can complete
    /// a request cycle. When unset, only TCP/Unix connect is performed
    /// — the first real request becomes the canary; useful as an
    /// operational fallback (e.g. emergency lazy mode with
    /// `min_live: 0`, no ping, opportunistic keep-alive only).
    #[serde(default)]
    pub ping_path: Option<String>,

    /// Proactive repair retry policy. Null disables the replenisher; ordinary
    /// request acquisition still works. Omission preserves the existing policy.
    #[serde(default = "default_replenish")]
    pub replenish: Option<ReplenishConfig>,

    /// Deprecated: prefer top-level `fpm.<name>.status_path` so metrics can
    /// apply to both pooled and one-shot FPM entries. Read only as a
    /// fallback, and only when `metrics` is compiled in.
    #[cfg(feature = "metrics")]
    #[serde(default)]
    pub status_path: Option<String>,

    /// Placeholder keeping the deprecated nested form parseable in a
    /// build without `metrics`, so a strict record does not reject an
    /// operator config that merely predates the move.
    #[cfg(not(feature = "metrics"))]
    #[allow(dead_code)]
    #[serde(default, rename = "status_path")]
    pub status_path_ignored: serde::de::IgnoredAny,

    /// Rotate a client connection after this many requests. Set slightly below
    /// FPM's `pm.max_requests` so claviron recycles before the paired PHP
    /// child hits its own limit (avoids broken-pipe bursts).
    #[serde(default)]
    pub max_requests: Option<u32>,

    /// Max wait for a leasable pool slot when the pool is at `max_live`,
    /// and the total an acquire can cost: if the wait ends in a fresh
    /// dial, that dial spends what is left of this rather than starting a
    /// second budget. Set it generously — under burst load clients queue
    /// here rather than receiving false-positive 5xx. `FpmClient.
    /// connect_timeout` still caps the dial on its own, so a hung FPM
    /// fails fast wherever it is dialled. Default: 5s.
    #[serde(default, deserialize_with = "deserialize_duration")]
    pub acquire_timeout: Option<Duration>,

    /// Cap a client connection's wall-clock lifetime (since first connect).
    #[serde(default, deserialize_with = "deserialize_duration")]
    pub max_lifetime: Option<Duration>,

    /// Drop idle client connections that haven't been used for this long.
    #[serde(default, deserialize_with = "deserialize_duration")]
    pub idle_timeout: Option<Duration>,
}

impl PoolConfig {
    pub fn acquire_timeout(&self) -> Duration {
        self.acquire_timeout.unwrap_or(Duration::from_secs(5))
    }
}

/// Retry policy for this pool's background repair, independent of request deadlines.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplenishConfig {
    initial_backoff: Duration,
    max_backoff: Duration,
}

impl Default for ReplenishConfig {
    fn default() -> Self {
        Self {
            initial_backoff: default_replenish_initial(),
            max_backoff: default_replenish_max(),
        }
    }
}
impl ReplenishConfig {
    pub fn initial_backoff(&self) -> Duration {
        self.initial_backoff
    }
    pub fn next_backoff(
        &self,
        current: Duration,
    ) -> Duration {
        current.saturating_mul(2).min(self.max_backoff)
    }
}
fn default_replenish() -> Option<ReplenishConfig> {
    Some(ReplenishConfig::default())
}
fn default_replenish_initial() -> Duration {
    Duration::from_millis(100)
}
fn default_replenish_max() -> Duration {
    Duration::from_secs(5)
}

impl<'de> Deserialize<'de> for ReplenishConfig {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D
    ) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Policy {
            #[serde(
                default = "default_replenish_initial",
                deserialize_with = "deserialize_duration"
            )]
            initial_backoff: Duration,
            #[serde(
                default = "default_replenish_max",
                deserialize_with = "deserialize_duration"
            )]
            max_backoff: Duration,
        }
        let policy = Policy::deserialize(deserializer)?;
        if policy.initial_backoff.is_zero()
            || policy.max_backoff < policy.initial_backoff
        {
            return Err(serde::de::Error::custom(
                "replenish requires 0 < initial_backoff <= max_backoff",
            ));
        }
        Ok(Self {
            initial_backoff: policy.initial_backoff,
            max_backoff: policy.max_backoff,
        })
    }
}

#[cfg(test)]
#[path = "../tests/replenish.rs"]
mod replenish_tests;
