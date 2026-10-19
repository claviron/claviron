use std::collections::HashMap;
use std::convert::Infallible;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use claviron_http::{RouteBody, boxed_body};
use claviron_runtime::AppState;
use claviron_runtime::registry::{Component, Reloadable};
use fastcgi_client::response::{Content, ResponseStream};
use fastcgi_client::{Client, ClientError, Params, Request, StreamExt};
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use http_body::Frame;
use http_body_util::StreamBody;
use tokio::io::AsyncRead;
use tokio::sync::{RwLock, mpsc, oneshot};
use tokio_util::compat::TokioAsyncReadCompatExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::balancer::pick_spacious_entry;
use claviron_runtime::congif::{
    self, ConfigFile, ConfigObserver, Configurable,
};
#[cfg(feature = "tasks")]
use std::sync::atomic::{AtomicBool, Ordering};

use crate::config::{FpmClient, FpmDocument, FpmTable, HousekeepConfig};
use crate::connection::FcgiEndpoint;
use crate::error::ExecuteError;
#[cfg(feature = "metrics")]
use crate::metrics::fcgi::{FCGI_REQUEST_METRICS, FcgiStreamGuard};
use crate::pool::{FcgiPool, PoolError, PoolOpts, StaleKeepalive};

/// Channel buffer for the stream pump → response body bridge. Tunes
/// back-pressure between FPM read and HTTP write. 32 chunks is enough to
/// absorb typical FPM frame bursts without holding much RAM.
const STREAM_BODY_CHANNEL: usize = 32;

/// Per-pool dispatch table entry. Determined at config load time:
/// `pool:` block present → `Pooled` (keep-alive `FcgiPool`),
/// absent → `OneShot` (fresh dial per request).
#[derive(Clone)]
pub(crate) enum FpmEntry {
    Pooled(PooledEntry),
    OneShot(OneShotEntry),
}

/// One process-local pooled FPM entry.
#[derive(Clone)]
pub(crate) struct PooledEntry {
    local: FcgiPool,
}

impl Deref for PooledEntry {
    type Target = FcgiPool;

    fn deref(&self) -> &Self::Target {
        &self.local
    }
}

#[derive(Clone)]
pub(crate) struct OneShotEntry {
    pub(crate) endpoint: FcgiEndpoint,
    pub(crate) connect_timeout: Duration,
    pub(crate) exec_timeout: Duration,
    pub(crate) head_timeout: Duration,
}

#[cfg(feature = "metrics")]
#[derive(Clone)]
pub(crate) struct FpmStatusTarget {
    pub(crate) name: String,
    pub(crate) endpoint: FcgiEndpoint,
    pub(crate) connect_timeout: Duration,
    pub(crate) status_path: String,
}

impl FpmEntry {
    pub(crate) fn endpoint(&self) -> &FcgiEndpoint {
        match self {
            FpmEntry::Pooled(p) => p.endpoint(),
            FpmEntry::OneShot(o) => &o.endpoint,
        }
    }
}

#[derive(Clone)]
pub struct PhpFcgiService {
    entries: Arc<RwLock<HashMap<String, FpmEntry>>>,
    fpm_table: Arc<ArcSwapOption<FpmTable>>,
    config_path: PathBuf,
    state: AppState,
    #[cfg(feature = "tasks")]
    housekeeping_registered: Arc<AtomicBool>,
}

/// Locate `fpm.yaml` inside the runtime config directory.
fn fpm_config_path(state: &AppState) -> PathBuf {
    state.config_dir().join("fpm.yaml")
}

impl PhpFcgiService {
    /// The process's shutdown, for the replies that end at it
    /// ([`claviron_http::body::EndsAtShutdown`]).
    pub(crate) fn shutdown_token(&self) -> CancellationToken {
        self.state.shutdown_token()
    }

    pub fn from_state(state: AppState) -> Self {
        let config_path = fpm_config_path(&state);
        Self {
            entries: Arc::new(RwLock::new(HashMap::new())),
            fpm_table: Arc::new(ArcSwapOption::empty()),
            config_path,
            state,
            #[cfg(feature = "tasks")]
            housekeeping_registered: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Apply the prepared `fpm.yaml`: store its `FpmTable` snapshot and
    /// converge the pools on it. The master serves no PHP and keeps no pool.
    async fn apply(
        &self,
        document: &FpmDocument,
    ) -> claviron_runtime::registry::Result<()> {
        warn_on_sweep_cadence(&document.housekeep, &document.fpm);
        if document.fpm.is_empty() {
            self.fpm_table.store(None);
            debug!(
                path = %self.config_path.display(),
                "fpm.yaml declares no pool; PhpService running empty"
            );
        } else {
            self.fpm_table.store(Some(Arc::new(document.fpm.clone())));
        }
        if self.state.is_master() {
            self.entries.write().await.clear();
        } else {
            self.reconcile_pools().await.map_err(|e| {
                claviron_runtime::registry::Error::msg(format!(
                    "php reconcile: {e}"
                ))
            })?;
        }
        Ok(())
    }

    /// Prepare `fpm.yaml` through the reader and apply it, as boot's
    /// `configure` and a reload batch would: for the tests, which drive the
    /// service without a runtime.
    #[cfg(test)]
    async fn rebuild(
        &self,
        state: &AppState,
    ) -> claviron_runtime::registry::Result<()> {
        let owner: &dyn congif::ConfigRegistration = self;
        state.config_reader().prepare_owners(&[owner]).await?;
        let document = state.config_reader().snapshot::<Self>()?;
        self.apply(&document).await
    }

    // Removed `config_snapshot()` accessor — relied on the old monolithic
    // `state.config_snapshot()` API. PhpService should load its own
    // config from `etc/claviron/fpm.yaml` in `boot()` once that wiring lands.

    /// Look up an FPM entry by name, creating it on first access from
    /// the cached `fpm_table` config. Pools that fpm.yaml declares but
    /// no vhost route ever touches stay un-instantiated forever — no
    /// idle FCGI sockets, no housekeep noise, zero per-worker overhead
    /// for unused pool definitions.
    ///
    /// Returns `None` only when the requested name isn't in the
    /// fpm.yaml catalog at all (genuine misconfiguration).
    pub(crate) async fn get_entry(
        &self,
        name: &str,
    ) -> Option<FpmEntry> {
        // Fast path — already instantiated.
        if let Some(entry) = self.entries.read().await.get(name).cloned() {
            return Some(entry);
        }

        // Slow path: catalog lookup + build under write lock. Re-check
        // under write to avoid double-build under concurrent first
        // requests for the same pool.
        let cfg = self.fpm_table.load_full()?.get(name)?.clone();
        let mut entries = self.entries.write().await;
        if let Some(entry) = entries.get(name).cloned() {
            return Some(entry);
        }

        let endpoint = match build_endpoint(&cfg) {
            Ok(e) => e,
            Err(e) => {
                warn!("php-pool `{name}` lazy build failed: {e}");
                return None;
            }
        };
        let entry = if cfg.pool.is_some() {
            let opts = build_opts(&cfg);
            let pool =
                FcgiPool::new(endpoint, opts, self.state.shutdown_token());
            debug!("php-pool `{name}` first-touch instantiated");
            FpmEntry::Pooled(PooledEntry { local: pool })
        } else {
            FpmEntry::OneShot(OneShotEntry {
                endpoint,
                connect_timeout: cfg.connect_timeout(),
                exec_timeout: cfg.exec_timeout(),
                head_timeout: cfg.head_timeout(),
            })
        };
        entries.insert(name.to_string(), entry.clone());
        Some(entry)
    }

    /// Snapshot of currently-registered pooled entries. Used by the
    /// periodic housekeeping task and the metrics provider; resolved
    /// fresh each tick so reloads that add/remove pools propagate
    /// without explicit signaling.
    #[cfg(any(feature = "tasks", feature = "metrics"))]
    pub(crate) async fn pooled_entries(&self) -> Vec<(String, FcgiPool)> {
        self.entries
            .read()
            .await
            .iter()
            .filter_map(|(name, entry)| match entry {
                FpmEntry::Pooled(pool) => {
                    Some((name.clone(), pool.local.clone()))
                }
                FpmEntry::OneShot(_) => None,
            })
            .collect()
    }

    /// Every pooled entry the catalog **declares**, with the endpoint
    /// that identifies it to the ledger and the ceiling the ledger bounds
    /// it by.
    ///
    /// `(pooled, oneshot)` entry counts. Cheap — single read lock.
    #[cfg(feature = "metrics")]
    pub(crate) async fn entry_counts(&self) -> (u64, u64) {
        let entries = self.entries.read().await;
        let mut pooled = 0u64;
        let mut oneshot = 0u64;
        for entry in entries.values() {
            match entry {
                FpmEntry::Pooled(_) => pooled += 1,
                FpmEntry::OneShot(_) => oneshot += 1,
            }
        }
        (pooled, oneshot)
    }

    /// Whether the first matching entry talks to a remote (TCP) FPM.
    /// Used to skip local-filesystem validations that are meaningless
    /// when PHP runs on a different machine.
    pub async fn is_remote(
        &self,
        names: &[String],
    ) -> bool {
        for n in names {
            if let Some(entry) = self.get_entry(n).await {
                return matches!(entry.endpoint(), FcgiEndpoint::Tcp(_));
            }
        }
        false
    }
    #[cfg(feature = "metrics")]
    pub(crate) async fn status_targets(&self) -> Vec<FpmStatusTarget> {
        let Some(fpm_table) = self.fpm_table.load_full() else {
            return Vec::new();
        };

        fpm_table
            .iter()
            .filter_map(|(name, cfg)| {
                let endpoint = match build_endpoint(cfg) {
                    Ok(endpoint) => endpoint,
                    Err(e) => {
                        warn!("FPM metrics skipped pool `{}`: {}", name, e);
                        return None;
                    }
                };

                let Some(status_path) = cfg.status_path.clone().or_else(|| {
                    cfg.pool.as_ref().and_then(|pool| pool.status_path.clone())
                }) else {
                    warn!(
                        "FPM metrics skipped pool `{}`: missing `status_path`",
                        name
                    );
                    return None;
                };

                Some(FpmStatusTarget {
                    name: name.clone(),
                    endpoint,
                    connect_timeout: cfg.connect_timeout(),
                    status_path,
                })
            })
            .collect()
    }

    /// Resolve the entry a route's `fpm:` list dispatches to.
    ///
    /// A single name is taken as written. A list goes through the
    /// balancer, which builds each candidate as it ranks it: `entries` is
    /// lazy, so asking it for capacity before anything has instantiated
    /// the entry finds nothing to rank and the whole list looks missing.
    pub(crate) async fn resolve_entry(
        &self,
        names: &[String],
    ) -> Result<(String, FpmEntry), ExecuteError> {
        match names {
            [] => Err(ExecuteError::Missing),
            [only] => {
                let entry =
                    self.get_entry(only).await.ok_or(ExecuteError::Missing)?;
                Ok((only.clone(), entry))
            }
            many => pick_spacious_entry(self, many).await,
        }
    }

    /// Run one request on FPM and hand its reply on as it arrives. Returns
    /// the status and headers once FPM has sent them, and a `RouteBody`
    /// whose chunks flow straight from FPM into the HTTP response. The pump
    /// task owns the connection (or pool lease) until the reply is drained.
    ///
    /// The only way a request reaches FPM: a stream can be made whole, a
    /// whole never a stream, and whether a reply is a page, SSE or a
    /// download shows only in its own head — too late to choose a method
    /// by it. Measured on bison, the stream costs nothing next to reading
    /// the reply whole (`docs/REVIEW.md` §3e).
    pub async fn fcgi_execute_stream(
        &self,
        names: &[String],
        params: Params<'static>,
        stdin: Stdin,
    ) -> Result<(StatusCode, HeaderMap, RouteBody), ExecuteError> {
        let (name, entry) = self.resolve_entry(names).await?;
        let entry_kind = match &entry {
            FpmEntry::Pooled(_) => "keep-alive",
            FpmEntry::OneShot(_) => "oneshot",
        };
        let params = params
            .custom("CLAVIRON_FCGI_CLIENT", entry_kind)
            .custom("CLAVIRON_FCGI_POOL", name.clone());
        match entry {
            FpmEntry::Pooled(pool) => {
                exec_pooled_stream(name, pool.local, params, stdin).await
            }
            FpmEntry::OneShot(one_shot) => {
                exec_one_shot_stream(one_shot, params, stdin).await
            }
        }
    }

    /// Sync the entries map with the current `fpm.yaml` snapshot.
    ///
    /// **Drop-only**: an entry is removed when its name left the catalog
    /// *or* when its configuration no longer describes it. New and
    /// never-touched names are not pre-built — `get_entry` instantiates
    /// lazily the first time a route dispatches to it, so pools declared
    /// in `fpm.yaml` and referenced by no live route cost nothing.
    ///
    /// Dropping is how a changed entry takes effect, because a pool's
    /// options are baked at construction: the only way a new `max_live`
    /// reaches one is a new pool. Until 2026-09-19 this reconciliation
    /// only removed *vanished* names, so a reload that changed a number
    /// logged success and changed nothing — an operator raising a ceiling
    /// under pressure got no relief and no error.
    ///
    /// Dropping is safe under load. A `PoolLease` holds the pool's inner
    /// state alive, so requests in flight finish on the old pool; the
    /// idle connections it still holds close when the last of them lets
    /// go, and each one's ledger row is released with it.
    ///
    /// Called from `boot()` and `reload()` after `rebuild()` stores the
    /// new snapshot.
    async fn reconcile_pools(&self) -> Result<(), ExecuteError> {
        let Some(fpm_table) = self.fpm_table.load_full() else {
            // No snapshot yet — drop everything; nothing to converge to.
            self.entries.write().await.clear();
            return Ok(());
        };

        let mut entries = self.entries.write().await;
        entries.retain(|name, entry| match fpm_table.get(name) {
            None => {
                debug!("php-pool `{name}` left the catalog; dropped");
                false
            }
            Some(cfg) if entry_matches_config(entry, cfg) => true,
            Some(_) => {
                info!(
                    "php-pool `{name}` reconfigured; rebuilt on next request"
                );
                false
            }
        });
        Ok(())
    }
}

/// Check the sweep cadence against what it has to sweep.
///
/// ARCHITECTURE §3.3: claviron must decide a connection is stale before
/// FPM's own `pm.process_idle_timeout` invalidates the worker behind it,
/// so the sweep interval has to be at most half the tightest
/// `idle_timeout` in the file. Both numbers live in `fpm.yaml`, which is
/// the reason the cadence moved there — two files could not check each
/// other.
///
/// A warning rather than a refusal: the consequence is a connection that
/// outlives its welcome, which `acquire` and lease drop still catch on
/// their own. It is an operator's mistake to see, not a boot to fail.
fn warn_on_sweep_cadence(
    housekeep: &HousekeepConfig,
    fpm: &FpmTable,
) {
    if !housekeep.enabled {
        return;
    }
    for (name, cfg) in fpm.iter() {
        let Some(idle_timeout) =
            cfg.pool.as_ref().and_then(|pool| pool.idle_timeout)
        else {
            continue;
        };
        if housekeep.interval * 2 > idle_timeout {
            warn!(
                "fpm `{name}`: housekeep.interval {:?} exceeds half of idle_timeout {:?}; \
                 a connection can sit unswept for up to {:?}",
                housekeep.interval,
                idle_timeout,
                housekeep.interval + idle_timeout
            );
        }
    }
}

/// Whether a live entry is still what its configuration describes.
///
/// Compared against the *reconciled* options, the same shape
/// `FcgiPool::new` settles on, so a clamp does not read as a change and
/// rebuild the pool on every reload.
fn entry_matches_config(
    entry: &FpmEntry,
    cfg: &FpmClient,
) -> bool {
    let Ok(endpoint) = build_endpoint(cfg) else {
        // Unbuildable now; drop it rather than keep serving from a
        // configuration that no longer parses.
        return false;
    };
    match entry {
        FpmEntry::Pooled(pool) => {
            let Some(_) = cfg.pool.as_ref() else { return false };
            pool.endpoint() == &endpoint
                && pool.opts() == &build_opts(cfg).reconciled()
        }
        FpmEntry::OneShot(one_shot) => {
            cfg.pool.is_none()
                && one_shot.endpoint == endpoint
                && one_shot.connect_timeout == cfg.connect_timeout()
                && one_shot.exec_timeout == cfg.exec_timeout()
                && one_shot.head_timeout == cfg.head_timeout()
        }
    }
}

fn build_endpoint(cfg: &FpmClient) -> Result<FcgiEndpoint, ExecuteError> {
    if let Some(path) = &cfg.unix {
        Ok(FcgiEndpoint::Unix(path.clone()))
    } else if let Some(addr) = cfg.tcp {
        Ok(FcgiEndpoint::Tcp(addr))
    } else {
        Err(ExecuteError::Missing)
    }
}

/// Build pool options for a Pooled entry. Caller must have verified
/// `cfg.pool.is_some()`.
///
/// Sizing is passed through as the operator wrote it. `FcgiPool::new`
/// reconciles the three tiers against each other, so there is one place
/// that decides what `min_live > max_live` means instead of two that
/// have to agree.
fn build_opts(cfg: &FpmClient) -> PoolOpts {
    let pool =
        cfg.pool.as_ref().expect("build_opts called without pool config");

    PoolOpts {
        max_live: pool.max_live,
        min_live: pool.min_live,
        max_idle: pool.max_idle,
        connect_timeout: cfg.connect_timeout(),
        acquire_timeout: pool.acquire_timeout(),
        exec_timeout: cfg.exec_timeout(),
        head_timeout: cfg.head_timeout(),
        max_lifetime: pool.max_lifetime,
        max_requests: pool.max_requests,
        idle_timeout: pool.idle_timeout,
        ping_path: pool.ping_path.clone(),
    }
}

/// A request body as it goes to FPM.
pub enum Stdin {
    /// In hand, so a stale keep-alive connection can be retried with it.
    Whole(Bytes),
    /// Arriving, handed on as it comes; read once, so never retried.
    Live(Box<dyn AsyncRead + Unpin + Send>),
}

impl Stdin {
    /// This attempt's reader, and the body kept for another one.
    fn into_attempt(
        self
    ) -> (Box<dyn AsyncRead + Unpin + Send>, Option<Bytes>) {
        match self {
            Stdin::Whole(bytes) => {
                (Box::new(std::io::Cursor::new(bytes.clone())), Some(bytes))
            }
            Stdin::Live(reader) => (reader, None),
        }
    }
}

// ---------------------------------------------------------------------------
// Dispatch — one per entry kind; both stream.
// ---------------------------------------------------------------------------

async fn exec_pooled_stream(
    name: String,
    pool: FcgiPool,
    params: Params<'static>,
    stdin: Stdin,
) -> Result<(StatusCode, HeaderMap, RouteBody), ExecuteError> {
    // Acquire the lease in the caller so the dial budget (acquire_timeout
    // inside the pool) bounds the head_rx wait below — we don't want to
    // spawn the pump before we know FPM is reachable.
    let lease = pool.acquire().await.map_err(map_pool_error)?;
    let head_timeout = pool.opts().head_timeout;

    let (head_tx, head_rx) = oneshot::channel::<HeadResult>();
    let (body_tx, body_rx) = mpsc::channel::<Bytes>(STREAM_BODY_CHANNEL);

    #[cfg(feature = "metrics")]
    // The pump task takes ownership of the lease so the connection stays
    // borrowed for the entire response stream. When the task ends (EOF /
    // error / receiver dropped), the lease drops and the
    // connection is either returned to the pool or discarded if marked
    // unrecoverable. The `_metric` guard pins `streams_in_flight` to the
    // pump task's lifetime — Drop fires only when the body is fully
    // drained / cancelled, so the gauge reflects "stream body still
    // flowing" semantics.
    let _metric = FcgiStreamGuard::enter();

    tokio::spawn(async move {
        #[cfg(feature = "metrics")]
        let _metric = _metric;
        let mut head_tx = Some(head_tx);
        let mut lease = lease;
        let mut stdin = Some(stdin);
        let mut replayed = false;
        loop {
            let Some(body) = stdin.take() else { return };
            let (reader, kept) = body.into_attempt();
            // `used + 1` = the index of THIS request on this keep-alive conn.
            // FPM doesn't surface a per-conn worker request counter via
            // $_SERVER, so we inject it here alongside the pool-scoped conn id.
            let conn_id = lease.connection().id();
            let req_no = lease.connection().used().saturating_add(1);
            let attempt = params
                .clone()
                .custom("CLAVIRON_FCGI_CONN_ID", conn_id.to_string())
                .custom("CLAVIRON_CONN_REQUEST", req_no.to_string());
            let from_idle = !lease.is_fresh();
            lease.connection().begin_request();

            let end = match lease
                .connection()
                .client_mut()
                .execute_stream(Request::new(attempt, reader.compat()))
                .await
            {
                Ok(stream) => {
                    drive_response_stream(
                        stream,
                        &mut head_tx,
                        &body_tx,
                        head_timeout,
                    )
                    .await
                }
                Err(e) if is_dead_on_arrival(&e) => {
                    StreamEnd::Unanswered(ExecuteError::Client(e))
                }
                Err(e) => {
                    if let Some(tx) = head_tx.take() {
                        let _ = tx.send(Err(ExecuteError::Client(e)));
                    }
                    StreamEnd::Abandoned
                }
            };

            let error = match end {
                StreamEnd::Completed => {
                    lease.connection().mark_used();
                    return;
                }
                StreamEnd::Abandoned => {
                    lease.mark_unrecoverable();
                    return;
                }
                StreamEnd::Unanswered(error) => error,
            };

            // Two independent questions, and the same failure answers them
            // differently. No fence is needed before reading the evidence:
            // the stream has ended, so nothing is still reading.
            let answered = lease.connection().saw_response_byte();
            lease.mark_unrecoverable();
            drop(lease);

            // Only a connection that came out of the idle queue says
            // anything about the queue. Purging it because a socket dialled
            // a moment ago broke would throw away healthy connections over a
            // failure they had no part in — and even then the queue is
            // dropped only once a second reused connection agrees, because
            // one FPM child reaching its own `pm.max_requests` breaks exactly
            // like an FPM that went away.
            if from_idle {
                match pool.report_stale_keepalive() {
                    StaleKeepalive::Suspected => warn!(
                        "fastcgi stale keepalive on pool `{name}`; retired the connection, \
                         the queue is not condemned on one break"
                    ),
                    StaleKeepalive::Confirmed { purged } => warn!(
                        "fastcgi stale keepalive on pool `{name}` confirmed by a second \
                         connection; purged {purged} idle"
                    ),
                }
            }

            // FPM had started answering, so the request ran. Sending it again
            // would execute it a second time — a second row, a second charge —
            // to save one error. A body read as it arrived is gone and cannot
            // be sent again either; only one held whole can.
            let Some(bytes) = kept.filter(|_| !answered && !replayed) else {
                if answered {
                    debug!(
                        "fastcgi broke after FPM answered on pool `{name}`; not replayed"
                    );
                }
                if let Some(tx) = head_tx.take() {
                    let _ = tx.send(Err(error));
                }
                return;
            };

            // Fresh on purpose: the queue this connection came out of is
            // under suspicion and has not been condemned, so the retry has to
            // step around it rather than draw the next one from it.
            lease = match pool.acquire_fresh().await {
                Ok(lease) => lease,
                Err(e) => {
                    if let Some(tx) = head_tx.take() {
                        let _ = tx.send(Err(map_pool_error(e)));
                    }
                    return;
                }
            };
            pool.count_replay();
            replayed = true;
            stdin = Some(Stdin::Whole(bytes));
        }
    });

    head_rx
        .await
        .map_err(|_| ExecuteError::Missing)?
        .map(|(status, headers)| (status, headers, body_from_receiver(body_rx)))
}

async fn exec_one_shot_stream(
    one_shot: OneShotEntry,
    params: Params<'static>,
    stdin: Stdin,
) -> Result<(StatusCode, HeaderMap, RouteBody), ExecuteError> {
    // Dial in the caller so connect failures surface synchronously
    // (pre-pump). After this point, ResponseStream owns the socket so
    // the spawned pump can move it freely. A fresh socket is never stale,
    // so there is nothing to retry and the body is not kept.
    let stream = one_shot.endpoint.open(one_shot.connect_timeout).await?;
    let client = Client::new_tokio(stream);
    let (reader, _) = stdin.into_attempt();

    let resp_stream = client
        .execute_once_stream(Request::new(params, reader.compat()))
        .await
        .map_err(ExecuteError::Client)?;

    let (head_tx, head_rx) = oneshot::channel::<HeadResult>();
    let (body_tx, body_rx) = mpsc::channel::<Bytes>(STREAM_BODY_CHANNEL);

    #[cfg(feature = "metrics")]
    let _metric = FcgiStreamGuard::enter();
    tokio::spawn(async move {
        #[cfg(feature = "metrics")]
        // Owned ResponseStream — no lease to manage, just drive to EOF
        // (or until receiver-drop). `_metric` keeps
        // `streams_in_flight` bumped while the body is flowing.
        let _metric = _metric;
        let mut head_tx = Some(head_tx);
        let end = drive_response_stream(
            resp_stream,
            &mut head_tx,
            &body_tx,
            one_shot.head_timeout,
        )
        .await;
        if let (StreamEnd::Unanswered(error), Some(tx)) = (end, head_tx.take())
        {
            let _ = tx.send(Err(error));
        }
    });

    head_rx
        .await
        .map_err(|_| ExecuteError::Missing)?
        .map(|(status, headers)| (status, headers, body_from_receiver(body_rx)))
}

// ---------------------------------------------------------------------------
// Streaming pump primitives
// ---------------------------------------------------------------------------

type HeadResult = Result<(StatusCode, HeaderMap), ExecuteError>;

/// How a streamed response stopped — the verdict a pooled connection is
/// returned under.
///
/// Only `Completed` may re-publish the connection. An FCGI keep-alive
/// socket carries one response's records until END_REQUEST; abandoning
/// the stream earlier leaves those records unread in the socket, and
/// `ResponseStream` does not drain them on drop. A connection put back
/// in the idle queue in that state hands the tail of this response to
/// whoever leases it next, as if it were their own. Client disconnect
/// mid-stream is the ordinary case here (an SSE tab closing), not an
/// exotic one.
#[derive(Debug)]
enum StreamEnd {
    /// The FCGI stream reached its natural end; the socket is clean.
    Completed,
    /// Abandoned before END_REQUEST — receiver gone, FPM
    /// error, or headers that never arrived; the caller has been told.
    Abandoned,
    /// The connection broke before FPM sent a single record: the shape of
    /// a keep-alive connection FPM had already closed. Nobody has been
    /// told yet, so the caller may retry or report it.
    Unanswered(ExecuteError),
}

/// Drive a `ResponseStream` chunk-by-chunk: header bytes accumulate into
/// a small buffer until a `\r\n\r\n` (or `\n\n`) separator appears, at
/// which point status + headers are sent over `head_tx`. From there on,
/// every Stdout chunk is forwarded directly to `body_tx` so the HTTP
/// response body sees bytes the moment FPM produces them. Stderr chunks
/// are logged. The returned `StreamEnd` is the verdict the caller
/// applies to the connection; `head_tx` is left in place only for
/// `Unanswered`.
async fn drive_response_stream<S>(
    mut stream: ResponseStream<S>,
    head_tx: &mut Option<oneshot::Sender<HeadResult>>,
    body_tx: &mpsc::Sender<Bytes>,
    head_timeout: Duration,
) -> StreamEnd
where
    S: futures_util::AsyncRead + Unpin,
{
    let mut header_buf = BytesMut::new();
    // Any record at all: once FPM has said something, the request ran.
    let mut received = false;
    // Bounds the head and nothing after it. The body is deliberately
    // uncapped — an SSE stream is supposed to run long — so the deadline
    // is armed only while the headers are still outstanding, and the
    // guard disarms it the moment they are sent.
    let head_deadline = tokio::time::Instant::now() + head_timeout;

    loop {
        let next = tokio::select! {
            biased;
            _ = tokio::time::sleep_until(head_deadline), if head_tx.is_some() => {
                // FPM took the request and said nothing. Without this the
                // caller waits for ever and the pump holds the lease with
                // it, so the slot never comes back either.
                if let Some(tx) = head_tx.take() {
                    let _ = tx.send(Err(ExecuteError::Timeout("response headers")));
                }
                return StreamEnd::Abandoned;
            }
            item = stream.next() => item,
        };

        let Some(item) = next else { break };

        match item {
            Ok(Content::Stdout(chunk)) => {
                received = true;
                if head_tx.is_some() {
                    header_buf.extend_from_slice(&chunk);
                    if let Some((status, headers, body_offset)) =
                        try_parse_headers(&header_buf)
                    {
                        if let Some(tx) = head_tx.take() {
                            let _ = tx.send(Ok((status, headers)));
                        }
                        let mut buf = std::mem::take(&mut header_buf);
                        if body_offset < buf.len() {
                            let body_part = buf.split_off(body_offset).freeze();
                            #[cfg(feature = "metrics")]
                            let n = body_part.len() as u64;
                            if body_tx.send(body_part).await.is_err() {
                                // Client gone before the first body chunk
                                // landed. The rest of this response is
                                // still queued on the socket.
                                return StreamEnd::Abandoned;
                            }
                            #[cfg(feature = "metrics")]
                            FCGI_REQUEST_METRICS.add_stream_bytes(n);
                        }
                    }
                } else {
                    #[cfg(feature = "metrics")]
                    let n = chunk.len() as u64;
                    if body_tx.send(chunk).await.is_err() {
                        // Receiver dropped — client gone mid-body.
                        return StreamEnd::Abandoned;
                    }
                    #[cfg(feature = "metrics")]
                    FCGI_REQUEST_METRICS.add_stream_bytes(n);
                }
            }
            Ok(Content::Stderr(chunk)) => {
                received = true;
                warn!(
                    fpm_stderr = %String::from_utf8_lossy(&chunk),
                    "fpm stderr"
                );
            }
            Err(e) if !received && is_dead_on_arrival(&e) => {
                return StreamEnd::Unanswered(ExecuteError::Client(e));
            }
            Err(e) => {
                if let Some(tx) = head_tx.take() {
                    let _ = tx.send(Err(ExecuteError::Client(e)));
                } else {
                    tracing::error!(error = ?e, "fpm stream error after headers sent");
                }
                return StreamEnd::Abandoned;
            }
        }
    }

    if !received {
        // Closed without a single record: FPM had let the connection go.
        return StreamEnd::Unanswered(ExecuteError::Missing);
    }
    if let Some(tx) = head_tx.take() {
        // Stream ended before any \r\n\r\n separator was seen.
        let _ = tx.send(Err(ExecuteError::Missing));
        return StreamEnd::Abandoned;
    }
    StreamEnd::Completed
}

/// Incremental FCGI/CGI header parser. Returns `(status, headers,
/// body_offset)` where `body_offset` is the index past the separator —
/// any bytes at `buf[body_offset..]` are the start of the response body.
/// Returns `None` while the buffer is incomplete.
fn try_parse_headers(buf: &[u8]) -> Option<(StatusCode, HeaderMap, usize)> {
    let (sep_pos, sep_len) =
        buf.windows(4).position(|w| w == b"\r\n\r\n").map(|p| (p, 4)).or_else(
            || buf.windows(2).position(|w| w == b"\n\n").map(|p| (p, 2)),
        )?;

    let mut status = StatusCode::OK;
    let mut headers = HeaderMap::new();

    for line in buf[..sep_pos].split(|&b| b == b'\n') {
        let line = if line.last() == Some(&b'\r') {
            &line[..line.len() - 1]
        } else {
            line
        };
        if line.is_empty() {
            continue;
        }
        let colon_pos = line.iter().position(|&b| b == b':')?;
        let name_bytes = &line[..colon_pos];
        let value_bytes = trim_ascii_start(&line[colon_pos + 1..]);

        if name_bytes.eq_ignore_ascii_case(b"Status") {
            let value_str = std::str::from_utf8(value_bytes).ok()?;
            let code = value_str.split_whitespace().next()?;
            let code = code.parse::<u16>().ok()?;
            status = StatusCode::from_u16(code).ok()?;
            continue;
        }

        let name = HeaderName::from_bytes(name_bytes).ok()?;
        let value = HeaderValue::from_bytes(value_bytes).ok()?;
        headers.append(name, value);
    }

    Some((status, headers, sep_pos + sep_len))
}

#[inline]
fn trim_ascii_start(bytes: &[u8]) -> &[u8] {
    let start = bytes
        .iter()
        .position(|&b| !b.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    &bytes[start..]
}

/// Wrap an `mpsc::Receiver<Bytes>` as a `RouteBody`. Each received chunk
/// becomes a single data frame; receiver close → end-of-body.
fn body_from_receiver(rx: mpsc::Receiver<Bytes>) -> RouteBody {
    use futures_util::stream;

    let s = stream::unfold(rx, |mut rx| async move {
        rx.recv()
            .await
            .map(|chunk| (Ok::<_, Infallible>(Frame::data(chunk)), rx))
    });
    boxed_body(StreamBody::new(s))
}

fn map_pool_error(e: PoolError) -> ExecuteError {
    match e {
        PoolError::Connect(ce) => ExecuteError::Connect(ce),
        PoolError::Timeout => ExecuteError::Timeout("acquire"),
        PoolError::Cancelled => ExecuteError::Timeout("cancelled"),
    }
}

fn is_dead_on_arrival(e: &ClientError) -> bool {
    use std::io::ErrorKind;
    match e {
        ClientError::Io(ioe) => {
            matches!(
                ioe.kind(),
                ErrorKind::UnexpectedEof
                    | ErrorKind::NotConnected
                    | ErrorKind::BrokenPipe
            ) || ioe.to_string().to_ascii_lowercase().contains("early eof")
        }
        _ => false,
    }
}

/// `fpm.yaml` is owned here; an absent file is the empty document.
#[async_trait]
impl Configurable for PhpFcgiService {
    type Config = FpmDocument;

    fn source(&self) -> ConfigFile {
        ConfigFile::yaml(&self.config_path)
    }

    fn absent_source(&self) -> Option<FpmDocument> {
        Some(FpmDocument::default())
    }

    async fn configure(
        &self,
        config: &FpmDocument,
    ) -> congif::Result<()> {
        // The sweep's task entry carries the prepared cadence; it installs
        // here, before `TaskService` boots and folds builtin entries in.
        #[cfg(feature = "tasks")]
        if !self.housekeeping_registered.swap(true, Ordering::AcqRel) {
            crate::tasks::register(&self.state, &config.housekeep)
                .map_err(congif::Error::msg)?;
        }
        self.apply(config).await.map_err(congif::Error::msg)?;
        if self.state.is_worker() {
            tracing::debug!(
                "📦 PhpFcgiService configured {}",
                self.config_path.display()
            );
        } else {
            info!(
                "📦 PhpFcgiService configured {}",
                self.config_path.display()
            );
        }
        Ok(())
    }

    fn as_config_observer(
        &self
    ) -> Option<&dyn ConfigObserver<Config = Self::Config>> {
        Some(self)
    }
}

#[async_trait]
impl ConfigObserver for PhpFcgiService {
    async fn config_changed(
        &self,
        config: &FpmDocument,
    ) -> congif::Result<()> {
        self.apply(config).await.map_err(congif::Error::msg)?;
        info!("🔁 PhpFcgiService reconfigured {}", self.config_path.display());
        Ok(())
    }
}

impl Component<AppState> for PhpFcgiService {
    type Deps = ();

    fn name(&self) -> &'static str {
        "PhpFcgiService"
    }

    fn as_configurable(&self) -> Option<&dyn congif::ConfigRegistration> {
        Some(self)
    }

    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        Some(self)
    }
}

#[async_trait]
impl Reloadable<AppState> for PhpFcgiService {
    async fn reload(
        &self,
        state: &AppState,
    ) -> claviron_runtime::registry::Result<()> {
        let document = state.config_reader().snapshot::<Self>()?;
        self.apply(&document).await?;
        info!("🔁 PhpFcgiService reloaded {}", self.config_path.display());
        Ok(())
    }
}

#[cfg(test)]
#[path = "../tests/service.rs"]
mod tests;
