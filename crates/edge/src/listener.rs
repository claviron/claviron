use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
#[cfg(feature = "postgres")]
use claviron_db::{ChannelListener, Notice};
#[cfg(feature = "orbit")]
use claviron_orbit::placement::{Scope, Scoped};
use claviron_runtime::registry::{
    Component, Error, Reloadable, Runnable, priority,
};
use claviron_runtime::{AppState, RunContext};
#[cfg(feature = "postgres")]
use claviron_types::Host;
#[cfg(feature = "postgres")]
use serde::Deserialize;
use tokio::sync::watch;
#[cfg(feature = "postgres")]
use tracing::error;
use tracing::{info, warn};

use crate::VhostResolver;

#[cfg(feature = "postgres")]
const VHOST_ALIAS_NOTIFY_CHANNEL: &str = "hosting_vhost_alias_changed";

#[cfg(feature = "postgres")]
#[derive(Debug, Deserialize)]
struct VhostAliasChangeEvent {
    operation: String,
    old: Option<VhostAliasRow>,
    new: Option<VhostAliasRow>,
}

#[cfg(feature = "postgres")]
#[derive(Debug, Deserialize)]
struct VhostAliasRow {
    vhost_name: Option<String>,
    hostname: String,
}

pub struct VhostAliasListener {
    reload_tx: watch::Sender<u64>,
    reload_gen: AtomicU64,
}

impl Default for VhostAliasListener {
    fn default() -> Self {
        let (reload_tx, _reload_rx) = watch::channel(0u64);
        Self { reload_tx, reload_gen: AtomicU64::new(0) }
    }
}

impl VhostAliasListener {
    pub async fn start(
        self: Arc<Self>,
        (resolver, db, hosting): (
            Arc<VhostResolver>,
            Arc<claviron_db::DbService>,
            Arc<claviron_hosting::HostingService>,
        ),
        state: AppState,
        context: RunContext,
    ) -> claviron_runtime::registry::Result<()> {
        let mut reload_rx = self.reload_tx.subscribe();

        // One pass per pool: a reload of this listener starts again with the
        // pool as it is now.
        loop {
            let _server_id = hosting.require_server_id().map_err(|e| {
                Error::recoverable(format!("server_id unavailable: {e}"))
            })?;

            // Recoverable: vhost alias notify is best-effort. Without the
            // pool the worker still serves traffic; it just won't pick up
            // live alias changes. NOTIFY is PostgreSQL's. With hosting data
            // elsewhere (SQLite) the store that writes announces it as a
            // fleet event instead; a reload looks at the profile again.
            let profile = db.hosting_profile();
            let _pool = match db.profile(&profile) {
                #[cfg(feature = "postgres")]
                Ok(claviron_db::Pool::Postgres(pool)) => pool,
                #[allow(unreachable_patterns)] // a build with only PostgreSQL
                Ok(_) => {
                    if state.is_worker() {
                        tracing::debug!(
                            "hosting data is in `{profile}`: listening for hosting events"
                        );
                    } else {
                        info!(
                            "hosting data is in `{profile}`: listening for hosting events"
                        );
                    }
                    let mut events = state
                        .events()
                        .subscribe::<claviron_hosting::HostingChanged>(
                    );
                    loop {
                        let received = tokio::select! {
                            _ = context.cancelled() => return Ok(()),
                            _ = reload_rx.changed() => break,
                            received = events.recv() => received
                        };
                        match received {
                            Ok(claviron_hosting::HostingChanged::Certificates) => {}
                            // A missed event may have been a vhost's.
                            Ok(claviron_hosting::HostingChanged::Vhosts)
                            | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                // Alias by alias, as NOTIFY's changes are
                                // applied: replacing the table would fail
                                // every listener closed until a full reload.
                                match resolver.apply_stored_aliases(&state).await {
                                    Ok((set, removed)) => info!(
                                        set,
                                        removed, "aliases updated after a hosting change"
                                    ),
                                    Err(error) => warn!("alias update failed: {error}")
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(())
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
                let mut channel = tokio::select! {
                    _ = context.cancelled() => return Ok(()),
                    channel = ChannelListener::open(&state, &pool, &[VHOST_ALIAS_NOTIFY_CHANNEL]) => channel
                };
                if state.is_worker() {
                    tracing::debug!(
                        "📡 listening postgres channel '{}' (server_id={})",
                        VHOST_ALIAS_NOTIFY_CHANNEL,
                        server_id
                    );
                } else {
                    info!(
                        "📡 listening postgres channel '{}' (server_id={})",
                        VHOST_ALIAS_NOTIFY_CHANNEL, server_id
                    );
                }

                loop {
                    let notice = tokio::select! {
                        _ = context.cancelled() => return Ok(()),
                        _ = reload_rx.changed() => break,
                        notice = channel.next() => notice
                    };
                    let applied = match notice {
                        Notice::Changed(notifications) => {
                            let mut applied = true;
                            for notification in notifications {
                                if let Err(e) = handle_alias_notification(
                                    &resolver,
                                    &state,
                                    notification.payload(),
                                )
                                .await
                                {
                                    error!(
                                        "vhost alias event apply failed: {}",
                                        e
                                    );
                                    applied = false;
                                }
                            }
                            applied
                        }
                        // Events may have been lost while the connection was
                        // down: read every alias again.
                        Notice::Resync => {
                            match resolver.replace_all(&state).await {
                                Ok(_) => {
                                    info!(
                                        "🔁 vhost aliases resynced after a listener gap"
                                    );
                                    true
                                }
                                Err(e) => {
                                    warn!("vhost alias resync failed: {e}");
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

#[cfg(feature = "postgres")]
async fn handle_alias_notification(
    resolver: &VhostResolver,
    state: &AppState,
    payload: &str,
) -> claviron_runtime::registry::Result<()> {
    let event: VhostAliasChangeEvent = serde_json::from_str(payload)?;

    match event.operation.as_str() {
        "INSERT" => {
            let Some(new_row) = event.new else {
                return Ok(());
            };
            let Some(vhost_name) = new_row.vhost_name else {
                warn!(
                    "vhost alias INSERT missing vhost_name; rebuilding snapshot"
                );
                return resolver.replace_all(state).await;
            };

            let alias = new_row.hostname.parse::<Host>().map_err(Error::msg)?;
            let canonical = vhost_name.parse::<Host>().map_err(Error::msg)?;
            let inserted =
                resolver.upsert_alias(alias.clone(), canonical.clone())?;
            info!(
                "✅ vhost alias applied op=INSERT alias='{}' canonical='{}' inserted={}",
                alias, canonical, inserted
            );
        }
        "DELETE" => {
            let Some(old_row) = event.old else {
                return Ok(());
            };
            let alias = old_row.hostname.parse::<Host>().map_err(Error::msg)?;
            let removed = resolver.delete_alias(&alias);
            info!(
                "✅ vhost alias applied op=DELETE alias='{}' removed={}",
                alias, removed
            );
        }
        "UPDATE" => {
            let Some(new_row) = event.new else {
                warn!(
                    "vhost alias UPDATE missing new row; rebuilding snapshot"
                );
                return resolver.replace_all(state).await;
            };
            let Some(vhost_name) = new_row.vhost_name else {
                warn!(
                    "vhost alias UPDATE missing vhost_name; rebuilding snapshot"
                );
                return resolver.replace_all(state).await;
            };

            let old_alias = event.old
                .map(|row| row.hostname.parse::<Host>().map_err(Error::msg))
                .transpose()?;
            let alias = new_row.hostname.parse::<Host>().map_err(Error::msg)?;
            let canonical = vhost_name.parse::<Host>().map_err(Error::msg)?;
            let inserted =
                resolver.update_alias(old_alias.as_ref(), alias.clone(), canonical.clone())?;
            info!(
                "✅ vhost alias applied op=UPDATE alias='{}' canonical='{}' inserted={}",
                alias, canonical, inserted
            );
        }
        other => {
            warn!(
                "unknown vhost alias operation '{}'; rebuilding snapshot",
                other
            );
            resolver.replace_all(state).await?;
            info!(
                "✅ vhost alias listener rebuilt snapshot after unknown op='{}'",
                other
            );
        }
    }

    Ok(())
}

impl Component<AppState> for VhostAliasListener {
    type Deps = (
        Arc<VhostResolver>,
        Arc<claviron_db::DbService>,
        Arc<claviron_hosting::HostingService>,
    );

    fn name(&self) -> &'static str {
        "VhostAliasListener"
    }

    fn run_priority(&self) -> Option<u8> {
        Some(priority::LATE)
    }

    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        Some(self)
    }

    fn as_runnable(
        self: Arc<Self>
    ) -> Option<Arc<dyn Runnable<AppState, Self::Deps>>> {
        Some(self)
    }

    #[cfg(feature = "orbit")]
    fn as_scoped(&self) -> Option<&dyn Scoped> {
        Some(self)
    }
}

/// Alias changes matter where requests are answered: the listener runs in
/// workers (or a standalone process). The master's resolver takes them on
/// reload.
#[cfg(feature = "orbit")]
impl Scoped for VhostAliasListener {
    fn scope(&self) -> Scope {
        Scope::Worker
    }
}

#[async_trait]
impl Reloadable<AppState> for VhostAliasListener {
    async fn reload(
        &self,
        _state: &AppState,
    ) -> claviron_runtime::registry::Result<()> {
        let reload_epoch =
            self.reload_gen.fetch_add(1, Ordering::SeqCst).wrapping_add(1);
        let _ = self.reload_tx.send(reload_epoch);
        info!("♻️  vhost alias listener: signaled reload");
        Ok(())
    }
}

#[async_trait]
impl
    Runnable<
        AppState,
        (
            Arc<VhostResolver>,
            Arc<claviron_db::DbService>,
            Arc<claviron_hosting::HostingService>,
        ),
    > for VhostAliasListener
{
    async fn run(
        self: Arc<Self>,
        deps: (
            Arc<VhostResolver>,
            Arc<claviron_db::DbService>,
            Arc<claviron_hosting::HostingService>,
        ),
        state: AppState,
        context: RunContext,
    ) -> Result<(), Error> {
        self.start(deps, state, context).await
    }
}
