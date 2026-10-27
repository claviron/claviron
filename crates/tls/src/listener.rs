//! Live cert / cert-alias updates from postgres `NOTIFY` / `LISTEN`.
//!
//! Mirrors the `claviron-edge::listener::VhostAliasListener` pattern but
//! for [`CertResolver`] state. Channel: `hosting_certificate_changed`.
//! Payload contract is documented in
//! `migrations/2026050501_certificate_notify_triggers.sql`.
//!
//! # Why every process runs this listener
//!
//! `CertResolver` is the system's single source of TLS truth (see the
//! crate `ARCHITECTURE.md` §1.3). It lives in **every** claviron process
//! — master, workers, standalone — because TLS-less roles still consult
//! it for sibling-cert lookups (e.g. redirect-only roles emit alt-svc
//! and need to know what the HTTPS endpoint advertises). Master skip
//! that other listeners do (e.g. vhost alias listener) does **not**
//! apply here.
//!
//! # Server-id filtering
//!
//! In a multi-server deployment, each `hosting.certificates` row is
//! linked to one or more servers via the `hosting.certificate_server`
//! pivot. A listener only acts on events whose cert is assigned to
//! **its** `server_id`. The check is one indexed `EXISTS` query per
//! event — cheap, race-free, idempotent for DELETE because
//! `CertResolver::remove_*` are no-ops when the host isn't installed.
//! Revocation refresh precedes this filter: client certificates need no
//! server assignment, and a revoked leaf must be rejected by every consumer.
//!
//! Embedding `server_ids` in the trigger payload was rejected because
//! on `certificates` INSERT the `certificate_server` pivot rows may
//! not yet exist (FK depends on the new id), making the array empty
//! exactly when we'd want it populated. See the migration's header
//! comment for the full rationale.
//!
//! # Failure modes
//!
//! A missing pool surfaces as `Error::Recoverable` so the worker keeps
//! serving traffic; the listener simply doesn't carry live cert events.
//! The connection comes from the pool through `claviron_db::ChannelListener`:
//! a dropped connection is replaced by sqlx, the gap is closed by reloading
//! every certificate, and a database that does not answer is waited for
//! (`PostgresConnected`), never retried on a clock. `Reloadable::reload`
//! starts again with the current pool so a refreshed `db.yaml` is picked up.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
#[cfg(feature = "postgres")]
use claviron_db::{ChannelListener, Notice};
#[cfg(feature = "postgres")]
use claviron_hosting::HostingStore;
use claviron_hosting::{HostingChanged, HostingService};
use claviron_runtime::registry::{
    Component, Error, Reloadable, Runnable, priority,
};
use claviron_runtime::{AppState, RunContext};
#[cfg(feature = "postgres")]
use serde::Deserialize;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::watch;
#[cfg(feature = "postgres")]
use tracing::{debug, error};
use tracing::{info, warn};

#[cfg(feature = "postgres")]
use crate::manager::CertificateManager;
use crate::provider::reload_resolver_certs;
use crate::resolver::CertResolver;

#[cfg(feature = "postgres")]
const CERTIFICATE_NOTIFY_CHANNEL: &str = "hosting_certificate_changed";

#[cfg(feature = "postgres")]
#[derive(Debug, Deserialize)]
struct CertChangeEvent {
    table: String,
    operation: String,
    old: Option<serde_json::Value>,
    new: Option<serde_json::Value>,
}

#[cfg(feature = "postgres")]
#[derive(Debug, Deserialize)]
struct CertRow {
    cn: String,
}

#[cfg(feature = "postgres")]
#[derive(Debug, Deserialize)]
struct AliasRow {
    hostname: String,
    cn: Option<String>,
}

pub struct CertificateListener {
    reload_tx: watch::Sender<u64>,
    reload_gen: AtomicU64,
}

impl Default for CertificateListener {
    fn default() -> Self {
        let (reload_tx, _reload_rx) = watch::channel(0u64);
        Self { reload_tx, reload_gen: AtomicU64::new(0) }
    }
}

impl CertificateListener {
    pub async fn start(
        self: Arc<Self>,
        (resolver, db, hosting): (
            Arc<CertResolver>,
            Arc<claviron_db::DbService>,
            Arc<HostingService>,
        ),
        state: AppState,
        context: RunContext,
    ) -> claviron_runtime::registry::Result<()> {
        let mut reload_rx = self.reload_tx.subscribe();

        // One pass per pool: a reload of this listener (after `db.yaml` or
        // the environment changed) starts again with the pool as it is now.
        loop {
            let _server_id = hosting.require_server_id().map_err(|e| {
                Error::recoverable(format!("server_id unavailable: {e}"))
            })?;

            // NOTIFY is PostgreSQL's. With hosting data elsewhere (SQLite)
            // the store that writes announces it as a fleet event instead;
            // a reload looks at the profile again.
            let profile = db.hosting_profile();
            let _pool = match db.profile(&profile) {
                #[cfg(feature = "postgres")]
                Ok(claviron_db::Pool::Postgres(postgres_pool)) => postgres_pool,
                #[allow(unreachable_patterns)] // a build with only PostgreSQL
                Ok(_) => {
                    if state.is_worker() {
                        tracing::debug!(
                            "📜 hosting data is in `{profile}`: listening for hosting events"
                        );
                    } else {
                        info!(
                            "📜 hosting data is in `{profile}`: listening for hosting events"
                        );
                    }
                    let mut events =
                        state.events().subscribe::<HostingChanged>();
                    // Subscribe before rereading: a change between boot and
                    // listener startup must not leave the blocklist stale.
                    if let Err(error) = crate::revocation::refresh(&state).await
                    {
                        warn!("certificate revocation refresh failed: {error}");
                    }
                    loop {
                        let received = tokio::select! {
                            _ = context.cancelled() => return Ok(()),
                            _ = reload_rx.changed() => break,
                            received = events.recv() => received
                        };
                        match received {
                            Ok(HostingChanged::Vhosts) => {}
                            // A missed event may have been a certificate's.
                            Ok(HostingChanged::Certificates)
                            | Err(RecvError::Lagged(_)) => {
                                match async {
                                    crate::revocation::refresh(&state).await?;
                                    reload_resolver_certs(&resolver, &state)
                                        .await
                                }
                                .await
                                {
                                    Ok(()) => {
                                        info!(
                                            "📜 certificates reloaded after a hosting change"
                                        )
                                    }
                                    Err(error) => warn!(
                                        "certificate reload failed: {error}"
                                    ),
                                }
                            }
                            Err(RecvError::Closed) => return Ok(()),
                        }
                    }
                    continue;
                }
                Err(error) => {
                    return Err(Error::recoverable(format!(
                        "hosting: {error}"
                    )));
                }
            };
            #[cfg(feature = "postgres")]
            {
                let pool = _pool;
                let server_id = _server_id;
                let store = HostingStore::new(pool.clone());

                let mut channel = tokio::select! {
                    _ = context.cancelled() => return Ok(()),
                    channel = ChannelListener::open(&state, &pool, &[CERTIFICATE_NOTIFY_CHANNEL]) => channel
                };
                if state.is_worker() {
                    tracing::debug!(
                        "📡 listening postgres channel '{}' (server_id={})",
                        CERTIFICATE_NOTIFY_CHANNEL,
                        server_id
                    );
                } else {
                    info!(
                        "📡 listening postgres channel '{}' (server_id={})",
                        CERTIFICATE_NOTIFY_CHANNEL, server_id
                    );
                }

                loop {
                    let notice = tokio::select! {
                        _ = context.cancelled() => {
                            debug!("🔸 cert listener is shutting down");
                            return Ok(());
                        }
                        _ = reload_rx.changed() => {
                            info!("♻️  cert listener reloading connection");
                            break;
                        }
                        notice = channel.next() => notice
                    };
                    let applied = match notice {
                        Notice::Changed(notifications) => {
                            // Client leaves are not assigned to an SNI server.
                            // Refresh before applying the server-assignment filter.
                            let mut applied = match crate::revocation::refresh(
                                &state,
                            )
                            .await
                            {
                                Ok(()) => true,
                                Err(error) => {
                                    error!(
                                        "certificate revocation refresh failed: {error}"
                                    );
                                    false
                                }
                            };
                            for notification in notifications {
                                if let Err(e) = handle_cert_notification(
                                    &resolver,
                                    &store,
                                    server_id,
                                    notification.payload(),
                                )
                                .await
                                {
                                    error!("cert event apply failed: {e}");
                                    applied = false;
                                }
                            }
                            applied
                        }
                        // Events may have been lost while the connection was
                        // down: read every certificate again.
                        Notice::Resync => {
                            match async {
                                crate::revocation::refresh(&state).await?;
                                reload_resolver_certs(&resolver, &state).await
                            }
                            .await
                            {
                                Ok(()) => {
                                    info!(
                                        "📜 certificates resynced after a listener gap"
                                    );
                                    true
                                }
                                Err(e) => {
                                    warn!("certificate resync failed: {e}");
                                    false
                                }
                            }
                        }
                    };
                    if !applied {
                        channel.missed();
                    }
                }
            }
        }
    }
}

/// Indexed `EXISTS` query: is this cert assigned to **this** server?
///
/// Used as the per-event filter so each claviron instance reacts only
/// to changes affecting its own certs. Lookup is one PK / index hit on
/// `hosting.certificate_server` joined with `hosting.certificates(cn)`.
#[cfg(feature = "postgres")]
async fn cert_assigned_to_server(
    store: &HostingStore,
    cn: &str,
    server_id: i32,
) -> bool {
    store.certificate_assigned_to_server(cn, server_id).await.unwrap_or_else(
        |e| {
            warn!("cert assignment lookup failed for cn={cn}: {e}");
            false
        },
    )
}

#[cfg(feature = "postgres")]
async fn handle_cert_notification(
    resolver: &CertResolver,
    store: &HostingStore,
    server_id: i32,
    payload: &str,
) -> claviron_runtime::registry::Result<()> {
    let event: CertChangeEvent = serde_json::from_str(payload)
        .map_err(|e| Error::msg(format!("cert event payload parse: {e}")))?;

    match event.table.as_str() {
        "certificates" => {
            handle_certificate_event(resolver, store, server_id, event).await
        }
        "cert_aliases" => {
            handle_alias_event(resolver, store, server_id, event).await
        }
        other => {
            warn!("unknown cert event table '{other}'; ignoring");
            Ok(())
        }
    }
}

#[cfg(feature = "postgres")]
async fn handle_certificate_event(
    resolver: &CertResolver,
    store: &HostingStore,
    server_id: i32,
    event: CertChangeEvent,
) -> claviron_runtime::registry::Result<()> {
    match event.operation.as_str() {
        "INSERT" | "UPDATE" => {
            let Some(new) = event.new else { return Ok(()) };
            let new: CertRow = serde_json::from_value(new).map_err(|e| {
                Error::msg(format!("certificates.new parse: {e}"))
            })?;

            if !cert_assigned_to_server(store, &new.cn, server_id).await {
                debug!(
                    "cert event op={} cn={} not assigned to server_id={}, skipping",
                    event.operation, new.cn, server_id
                );
                return Ok(());
            }

            let manager = CertificateManager::new(store, resolver);
            match manager.upsert_cn(&new.cn).await {
                Ok((certificate, aliases)) => {
                    info!(
                        enabled = certificate.enabled,
                        "✅ cert applied op={} cn={} aliases={}",
                        event.operation,
                        new.cn,
                        aliases.len()
                    );
                }
                Err(e) => warn!("cert upsert cn={} failed: {e}", new.cn),
            }
            Ok(())
        }
        "DELETE" => {
            let Some(old) = event.old else { return Ok(()) };
            let old: CertRow = serde_json::from_value(old).map_err(|e| {
                Error::msg(format!("certificates.old parse: {e}"))
            })?;

            // No assignment check: parent row is gone, the join would
            // return false anyway. Owned removal is idempotent (returns
            // 0 if the cn isn't installed) so this is safe to apply on
            // every listener regardless of original assignment.
            let removed = resolver.remove_managed_cert(&old.cn);
            if removed > 0 {
                info!("✅ cert removed cn={} entries={}", old.cn, removed);
            } else {
                debug!("cert delete cn={} not in resolver; skipping", old.cn);
            }
            Ok(())
        }
        other => {
            warn!("unknown certificates op '{other}'");
            Ok(())
        }
    }
}

#[cfg(feature = "postgres")]
async fn handle_alias_event(
    resolver: &CertResolver,
    store: &HostingStore,
    server_id: i32,
    event: CertChangeEvent,
) -> claviron_runtime::registry::Result<()> {
    match event.operation.as_str() {
        "INSERT" | "UPDATE" => {
            let Some(new) = event.new else { return Ok(()) };
            let new: AliasRow = serde_json::from_value(new).map_err(|e| {
                Error::msg(format!("cert_aliases.new parse: {e}"))
            })?;
            let Some(cn) = new.cn else {
                warn!(
                    "cert_aliases {} hostname={} cn missing; skipping",
                    event.operation, new.hostname
                );
                return Ok(());
            };

            // Same-owner rotation retires old aliases in the complete group
            // publication. A cross-certificate move retires only the old owner's
            // binding, never a name subsequently claimed by another certificate.
            if event.operation == "UPDATE"
                && let Some(old) = event.old.as_ref()
                && let Ok(old) = serde_json::from_value::<AliasRow>(old.clone())
                && let Some(old_cn) = old.cn
                && !old_cn.eq_ignore_ascii_case(&cn)
            {
                resolver.remove_alias(&old_cn, &old.hostname);
            }

            if !cert_assigned_to_server(store, &cn, server_id).await {
                debug!(
                    "alias event op={} hostname={} cn={} not on server_id={}, skipping",
                    event.operation, new.hostname, cn, server_id
                );
                return Ok(());
            }

            let manager = CertificateManager::new(store, resolver);
            match manager.upsert_cn(&cn).await {
                Ok((certificate, aliases)) => {
                    info!(
                        enabled = certificate.enabled,
                        "✅ alias applied op={} hostname={} cn={} re-stamped={} aliases",
                        event.operation,
                        new.hostname,
                        cn,
                        aliases.len()
                    );
                }
                Err(e) => warn!("alias upsert cn={cn} failed: {e}"),
            }
            Ok(())
        }
        "DELETE" => {
            let Some(old) = event.old else { return Ok(()) };
            let old: AliasRow = serde_json::from_value(old).map_err(|e| {
                Error::msg(format!("cert_aliases.old parse: {e}"))
            })?;

            // A cascade without a CN has no remaining owner to remove; the
            // parent certificate event retires its complete owned group.
            if let Some(cn) = old.cn.as_deref() {
                resolver.remove_alias(cn, &old.hostname);
            }
            debug!(
                "✅ alias removed hostname={} cn={:?}",
                old.hostname, old.cn
            );
            Ok(())
        }
        other => {
            warn!("unknown cert_aliases op '{other}'");
            Ok(())
        }
    }
}

impl Component<AppState> for CertificateListener {
    type Deps =
        (Arc<CertResolver>, Arc<claviron_db::DbService>, Arc<HostingService>);

    fn name(&self) -> &'static str {
        "CertificateListener"
    }

    fn run_priority(&self) -> Option<u8> {
        Some(priority::LATE)
    }

    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        Some(self)
    }

    fn as_runnable(
        self: std::sync::Arc<Self>
    ) -> Option<std::sync::Arc<dyn Runnable<AppState, Self::Deps>>> {
        Some(self)
    }
}

#[async_trait]
impl Reloadable<AppState> for CertificateListener {
    async fn reload(
        &self,
        _state: &AppState,
    ) -> claviron_runtime::registry::Result<()> {
        let epoch =
            self.reload_gen.fetch_add(1, Ordering::SeqCst).wrapping_add(1);
        let _ = self.reload_tx.send(epoch);
        info!("♻️  cert listener: signaled reload");
        Ok(())
    }
}

#[async_trait]
impl
    Runnable<
        AppState,
        (Arc<CertResolver>, Arc<claviron_db::DbService>, Arc<HostingService>),
    > for CertificateListener
{
    async fn run(
        self: Arc<Self>,
        deps: (
            Arc<CertResolver>,
            Arc<claviron_db::DbService>,
            Arc<HostingService>,
        ),
        state: AppState,
        context: RunContext,
    ) -> Result<(), Error> {
        self.start(deps, state, context).await
    }
}
