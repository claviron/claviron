use std::any::{Any, TypeId};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock, RwLock};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use claviron_runtime::congif::{
    self, ConfigFile, ConfigObserver, ConfigReader, Configurable,
    PartialConfigObserver,
};
use claviron_runtime::registry::{
    Bootable, Component, Reloadable, Resolve, priority,
};
use claviron_runtime::{AppState, Register};
use claviron_types::Host;
use tokio::sync::watch;
use tracing::info;

use crate::HttpConfigProvider;
use crate::config::HostConfig;
use crate::loader::config::{
    VhostDocument, partial_sources, read_document, read_partial_document,
};
use crate::loader::snapshot::VhostSnapshot;
use crate::loader::table::VhostTable;

/// What the vhost configuration file is called in the config directory.
pub const CONFIG_FILE: &str = "vhost.yaml";

/// The hosting aliases are hydrated from: the pool and the server id
/// `HostingService` resolves in its boot. Nothing without `db`.
#[cfg(feature = "db")]
type HostingDeps =
    (Arc<claviron_db::DbService>, Arc<claviron_hosting::HostingService>);
#[cfg(not(feature = "db"))]
type HostingDeps = ();

/// The declared log channels a host's `access_log` is checked against.
#[cfg(feature = "access-log")]
type ChannelDeps = Option<Arc<claviron_logging::pipeline::LogChannels>>;
#[cfg(not(feature = "access-log"))]
type ChannelDeps = ();

/// What a full table rebuild reads: listeners, hosting aliases and channels.
/// File notifications retain the live alias view instead of querying DB.
type VhostDeps = (Option<Arc<HttpConfigProvider>>, HostingDeps, ChannelDeps);

/// Cold preparation only: the caller supplies prepared config values.
type HostSource = dyn Fn(&AppState) -> crate::Result<Vec<(Host, Arc<HostConfig>)>>
    + Send
    + Sync;

type Preparation = dyn Fn(&VhostTable, &VhostTable) -> crate::Result<Arc<dyn Any + Send + Sync>>
    + Send
    + Sync;

pub struct VhostResolver {
    config_path: PathBuf,
    /// `vhost.yaml` and its includes as Congif last applied them: the
    /// declared hosts, before alias hydration. `replace_all` builds the
    /// published table from this, never from the file.
    document: ArcSwap<VhostDocument>,
    /// Retained registration-time sources, evaluated on table rebuild, never
    /// at request time. Consumer config files remain their owners' concern.
    sources: RwLock<Vec<Arc<HostSource>>>,
    preparations: RwLock<Vec<(TypeId, Arc<Preparation>)>>,
    publication: Mutex<()>,
    /// The process this resolver serves, once registered; a source change
    /// republishes through it. Unset in tests that publish tables by hand.
    state: OnceLock<AppState>,
    table: ArcSwap<VhostTable>,
    /// Advanced after every publication of `table`. It carries no data: a
    /// connection that holds a binding wakes on it and asks the new table
    /// whether its host still resolves where it did. Process-local by design;
    /// every worker already learns of hosting changes through Orbit and
    /// publishes its own table.
    publications: watch::Sender<u64>,
}

impl Default for VhostResolver {
    fn default() -> Self {
        Self {
            config_path: PathBuf::from(CONFIG_FILE),
            document: ArcSwap::from_pointee(VhostDocument::default()),
            sources: RwLock::new(Vec::new()),
            preparations: RwLock::new(Vec::new()),
            publication: Mutex::new(()),
            state: OnceLock::new(),
            table: ArcSwap::from_pointee(VhostTable::default()),
            publications: watch::channel(0).0,
        }
    }
}

/// Registers the resolver and binds `vhost.yaml` to the runtime's
/// configuration environment, in every process.
impl Register<AppState> for VhostResolver {
    fn register(state: &AppState) -> claviron_runtime::registry::Result<()> {
        state.bind_config_environment::<Self>()?;
        state.registry_ref().register(Arc::new(Self::new(state)))?;
        Ok(())
    }
}

impl VhostResolver {
    /// Prepare dependent routing/transport data before publishing a host view.
    /// Runs only during cold publication. Readers capture the table and its
    /// prepared data together; a failed preparation leaves the old view live.
    pub fn prepare_publications<T: Send + Sync + 'static>(
        &self,
        prepare: impl Fn(&VhostTable, Option<&T>) -> crate::Result<T>
        + Send
        + Sync
        + 'static,
    ) -> crate::Result<()> {
        let _publication = self.publication.lock().map_err(|_| {
            crate::Error::other("vhost publication lock poisoned")
        })?;
        let id = TypeId::of::<T>();
        let current = self.snapshot();
        let value: Arc<dyn Any + Send + Sync> =
            Arc::new(prepare(&current.declarations_only(), None)?);
        let preparation: Arc<Preparation> = Arc::new(move |next, previous| {
            let old = previous.prepared::<T>();
            Ok(Arc::new(prepare(next, old.as_deref())?))
        });
        let mut preparations = self.preparations.write().map_err(|_| {
            crate::Error::other("vhost preparation lock poisoned")
        })?;
        if let Some(entry) = preparations.iter_mut().find(|(key, _)| *key == id)
        {
            entry.1 = preparation;
        } else {
            preparations.push((id, preparation));
        }
        let mut next = current.as_ref().clone();
        next.set_prepared(id, value);
        self.publish_prepared(next);
        Ok(())
    }
    pub(crate) fn register_hosts(
        &self,
        source: impl Fn(&AppState) -> crate::Result<Vec<(Host, Arc<HostConfig>)>>
        + Send
        + Sync
        + 'static,
    ) -> crate::Result<()> {
        self.sources
            .write()
            .map_err(|_| {
                crate::Error::other("vhost source registration lock poisoned")
            })?
            .push(Arc::new(source));
        Ok(())
    }

    /// Invoke caller code without holding the registration lock. An invalid
    /// combined declaration fails before anything publishes a live table.
    fn declared_table(
        &self,
        state: &AppState,
    ) -> crate::Result<VhostTable> {
        let sources = self
            .sources
            .read()
            .map_err(|_| {
                crate::Error::other("vhost source registration lock poisoned")
            })?
            .clone();
        let mut hosts = Vec::new();
        for source in sources {
            hosts.extend(source(state)?);
        }
        self.document.load().table_with(hosts)
    }

    pub fn new(state: &AppState) -> Self {
        let resolver = Self {
            config_path: state.config_dir().join(CONFIG_FILE),
            ..Self::default()
        };
        let _ = resolver.state.set(state.clone());
        resolver
    }

    /// The only way a table is published: store it, then wake every binding.
    fn publish(
        &self,
        table: VhostTable,
    ) -> crate::Result<()> {
        let _publication = self.publication.lock().map_err(|_| {
            crate::Error::other("vhost publication lock poisoned")
        })?;
        self.publish_locked(table)
    }

    fn publish_locked(
        &self,
        mut table: VhostTable,
    ) -> crate::Result<()> {
        let previous = self.snapshot();
        table.retain_unchanged(&previous);
        let declarations = table.declarations_only();
        let preparations = self
            .preparations
            .read()
            .map_err(|_| {
                crate::Error::other("vhost preparation lock poisoned")
            })?
            .clone();
        for (id, prepare) in preparations {
            table.set_prepared(id, prepare(&declarations, &previous)?);
        }
        self.publish_prepared(table);
        Ok(())
    }

    fn publish_prepared(
        &self,
        table: VhostTable,
    ) {
        // The master does not build request routers, but owns worker exit
        // cleanup. Retain destinations of accepted work across route reloads.
        #[cfg(feature = "orbit")]
        if let Some(transport) = self.state.get().and_then(|state| {
            state
                .registry_ref()
                .resolve::<claviron_orbit::transport::TransportService>()
        }) {
            for (_, record) in table.canonical_entries() {
                for route in record.config.orbit() {
                    transport
                        .track_target(route.fleet_name, &route.application);
                }
            }
        }
        self.table.store(Arc::new(table));
        // `send_modify` notifies even while no connection is subscribed.
        self.publications.send_modify(|generation| {
            *generation = generation.wrapping_add(1);
        });
    }

    /// Bind a connection to `hostname` as it resolves now (to `canonical`).
    ///
    /// The binding never re-reads the table on the request path. It only
    /// wakes when a table is published, and [`HostBinding::retired`] completes
    /// once the host no longer resolves to `canonical`, or its captured config
    /// is replaced. Unchanged declarations retain their Arc identity and do
    /// not retire connections to other hosts.
    pub fn bind(
        self: &Arc<Self>,
        hostname: Arc<Host>,
        canonical: Arc<Host>,
    ) -> HostBinding {
        let config = self.snapshot().canonical_config(&canonical);
        self.binding(hostname, canonical, config)
    }

    /// Pin the config actually accepted, including a publication racing the
    /// handshake. Its replacement requests reconnect only for this binding.
    pub fn bind_config(
        self: &Arc<Self>,
        hostname: Arc<Host>,
        canonical: Arc<Host>,
        config: Arc<HostConfig>,
    ) -> HostBinding {
        self.binding(hostname, canonical, Some(config))
    }

    fn binding(
        self: &Arc<Self>,
        hostname: Arc<Host>,
        canonical: Arc<Host>,
        config: Option<Arc<HostConfig>>,
    ) -> HostBinding {
        HostBinding {
            resolver: Arc::clone(self),
            publications: self.publications.subscribe(),
            hostname,
            canonical,
            config,
        }
    }

    fn still_binds(
        &self,
        hostname: &Host,
        canonical: &Host,
        config: Option<&Arc<HostConfig>>,
    ) -> bool {
        self.table.load().resolve(hostname).is_some_and(|(resolved, live)| {
            &resolved == canonical
                && config.is_none_or(|captured| Arc::ptr_eq(captured, &live))
        })
    }

    pub fn snapshot(&self) -> Arc<VhostTable> {
        self.table.load_full()
    }

    /// One current read per admission. Never resolve through an alias or
    /// default fallback here: this key was fixed at the host/auth boundary.
    pub(crate) fn request_policy(
        &self,
        canonical: &Host,
    ) -> crate::service::policy::PolicySnapshot {
        use crate::service::policy::PolicySnapshot;
        let table = self.table.load();
        match table
            .record(canonical)
            .filter(|record| &record.canonical == canonical)
        {
            None => PolicySnapshot::Removed,
            Some(record) => match &record.policy {
                None => PolicySnapshot::Open,
                Some(policy) => PolicySnapshot::Rules(policy.clone()),
            },
        }
    }

    pub fn resolve(
        &self,
        host: &Host,
    ) -> Option<(Host, Arc<HostConfig>)> {
        let table = self.table.load();
        table.resolve(host)
    }

    /// Strict variant of [`Self::resolve`] — does not fall back to the
    /// `default` catchall.
    pub fn resolve_exact(
        &self,
        host: &Host,
    ) -> Option<(Host, Arc<HostConfig>)> {
        let table = self.table.load();
        table.resolve_exact(host)
    }

    async fn build_snapshot(
        &self,
        state: &AppState,
        _hosting: &HostingDeps,
    ) -> crate::Result<VhostTable> {
        // Combine retained sources with Congif's prepared `vhost.yaml` and includes
        // (with `${ENV_KEY}` expanded from the environment bound to this
        // owner) before boot or in the reload batch. Nothing opens the file
        // here.
        let table = self.declared_table(state)?;

        #[allow(unused_mut)]
        let mut snapshot = VhostSnapshot::from_config(table);

        // Hydrate alias rows from postgres (only meaningful under the `db`
        // feature — non-db builds skip the lookup). DbService owns the pool;
        // HostingStore owns every hosting-schema query.
        #[cfg(feature = "db")]
        {
            use claviron_hosting::HostingStore;

            let (db_service, hosting) = _hosting;
            let server_id =
                hosting.require_server_id().map_err(crate::Error::other)?;
            match HostingStore::open(db_service) {
                Ok(store) => {
                    let db_aliases = store
                        .aliases_for_server(server_id)
                        .await
                        .map_err(|e| {
                            crate::Error::other(format!(
                                "Failed to load aliases from DB: {e}"
                            ))
                        })?;
                    snapshot
                        .apply_db_aliases(db_aliases)
                        .map_err(crate::Error::other)?;
                }
                Err(error) => {
                    tracing::warn!(
                        "VhostResolver: {error}; skipping alias hydration"
                    );
                }
            }
        }

        Ok(snapshot.into_table())
    }

    pub async fn replace_all(
        &self,
        state: &AppState,
    ) -> claviron_runtime::registry::Result<()> {
        self.replace_with(state, &VhostDeps::resolve(state.registry_ref())?)
            .await
    }

    async fn replace_with(
        &self,
        state: &AppState,
        (http, hosting, _channels): &VhostDeps,
    ) -> claviron_runtime::registry::Result<()> {
        let next = self.build_snapshot(state, hosting).await?;
        let http = http.as_ref().map(|provider| provider.snapshot());
        next.validate_listeners(http.as_deref())?;
        #[cfg(feature = "access-log")]
        {
            let channels = _channels;
            next.validate_access_logs(|name| {
                channels
                    .as_ref()
                    .is_some_and(|channels| channels.channel(name).is_some())
            })?;
        }
        #[cfg(not(feature = "access-log"))]
        next.validate_access_logs(|_| false)?;
        self.publish(next).map_err(|error| {
            claviron_runtime::registry::Error::msg(error.to_string())
        })?;
        Ok(())
    }

    /// Bring the aliases up to the hosting store's, one alias at a time: the
    /// same alias-only publications NOTIFY's changes make. Hosts and their
    /// configs are left as they are. Prepared dependencies are retained or
    /// rebuilt before each publication. Returns how many aliases were set and
    /// how many removed.
    ///
    /// [`replace_all`]: Self::replace_all
    pub async fn apply_stored_aliases(
        &self,
        state: &AppState,
    ) -> claviron_runtime::registry::Result<(usize, usize)> {
        let (_, hosting, _) = VhostDeps::resolve(state.registry_ref())?;
        let desired = self.build_snapshot(state, &hosting).await?;
        let current = self.snapshot();
        let want: std::collections::HashMap<&Host, &Host> =
            desired.alias_entries().collect();
        let have: std::collections::HashMap<&Host, &Host> =
            current.alias_entries().collect();
        let mut set = 0;
        for (alias, canonical) in &want {
            if have.get(alias) != Some(canonical)
                && self
                    .upsert_alias((*alias).clone(), (*canonical).clone())
                    .map_err(|error| {
                        claviron_runtime::registry::Error::msg(
                            error.to_string(),
                        )
                    })?
            {
                set += 1;
            }
        }
        let mut removed = 0;
        for alias in have.keys() {
            if !want.contains_key(alias) && self.delete_alias(alias) {
                removed += 1;
            }
        }
        Ok((set, removed))
    }

    pub fn upsert_host(
        &self,
        canonical: Host,
        config: Arc<HostConfig>,
    ) {
        let Ok(_publication) = self.publication.lock() else {
            tracing::error!("vhost publication lock poisoned; update rejected");
            return;
        };
        let current = self.snapshot();
        let mut snapshot = VhostSnapshot::from_table(current.as_ref().clone());
        snapshot.upsert_host(canonical, config);
        if let Err(error) = self.publish_locked(snapshot.into_table()) {
            tracing::error!(%error, "vhost update rejected; previous snapshot retained");
        }
    }

    pub fn delete_host(
        &self,
        canonical: &Host,
    ) -> bool {
        let Ok(_publication) = self.publication.lock() else {
            tracing::error!(
                "vhost publication lock poisoned; removal rejected"
            );
            return false;
        };
        let current = self.snapshot();
        let mut snapshot = VhostSnapshot::from_table(current.as_ref().clone());
        let removed = snapshot.delete_host(canonical);
        if removed {
            if let Err(error) = self.publish_locked(snapshot.into_table()) {
                tracing::error!(%error, "vhost removal rejected; previous snapshot retained");
                return false;
            }
        }
        removed
    }

    pub fn upsert_alias(
        &self,
        alias: Host,
        canonical: Host,
    ) -> crate::Result<bool> {
        let _publication = self.publication.lock().map_err(|_| {
            crate::Error::other("vhost publication lock poisoned")
        })?;
        let current = self.snapshot();
        let mut snapshot = VhostSnapshot::from_table(current.as_ref().clone());
        let inserted = snapshot.upsert_alias(alias, canonical)?;
        if inserted {
            self.publish_locked(snapshot.into_table())?;
        }
        Ok(inserted)
    }

    pub fn delete_alias(
        &self,
        alias: &Host,
    ) -> bool {
        let Ok(_publication) = self.publication.lock() else {
            tracing::error!(
                "vhost publication lock poisoned; alias removal rejected"
            );
            return false;
        };
        let current = self.snapshot();
        let mut snapshot = VhostSnapshot::from_table(current.as_ref().clone());
        let removed = snapshot.delete_alias(alias);
        if removed {
            if let Err(error) = self.publish_locked(snapshot.into_table()) {
                tracing::error!(%error, "vhost alias removal rejected; previous snapshot retained");
                return false;
            }
        }
        removed
    }

    pub fn host_count(&self) -> usize {
        self.table.load().host_count()
    }

    pub fn alias_count(&self) -> usize {
        self.table.load().alias_count()
    }
}

/// A connection's hold on the vhost its host resolved to when it was
/// accepted. See [`VhostResolver::bind`].
pub struct HostBinding {
    resolver: Arc<VhostResolver>,
    publications: watch::Receiver<u64>,
    hostname: Arc<Host>,
    canonical: Arc<Host>,
    config: Option<Arc<HostConfig>>,
}

impl HostBinding {
    /// Complete once the host's mapping or captured configuration is replaced.
    ///
    /// Checked once at the start (a table published between the connection's
    /// resolution and this binding is not missed), then only when a table is
    /// published. Never completes while the binding holds, and never on a
    /// clock: the wait ends on a publication, not a deadline.
    pub async fn retired(&mut self) {
        loop {
            if !self.resolver.still_binds(
                &self.hostname,
                &self.canonical,
                self.config.as_ref(),
            ) {
                return;
            }
            if self.publications.changed().await.is_err() {
                // The resolver is gone with the process; nothing can move.
                std::future::pending::<()>().await;
            }
        }
    }

    pub fn hostname(&self) -> &Host {
        &self.hostname
    }

    pub fn canonical(&self) -> &Host {
        &self.canonical
    }
}

#[async_trait]
impl Configurable for VhostResolver {
    type Config = VhostDocument;

    fn source(&self) -> ConfigFile {
        ConfigFile::yaml(&self.config_path)
    }

    /// No `vhost.yaml`: only registration-time sources declare hosts.
    fn absent_source(&self) -> Option<VhostDocument> {
        Some(VhostDocument::default())
    }

    /// The root plus every file its `includes` name, merged.
    async fn read_config(
        &self,
        reader: &ConfigReader,
    ) -> congif::Result<VhostDocument> {
        read_document(self, reader).await
    }

    /// Every host key parses and the aliases do not collide. Listener and
    /// access-log checks need the other owners' snapshots; `replace_all`
    /// makes them before publishing.
    fn validate_config(
        &self,
        config: &VhostDocument,
    ) -> congif::Result<()> {
        config.table().map(|_| ()).map_err(|error| {
            congif::Error::msg(format!(
                "{}: {error}",
                self.config_path.display()
            ))
        })
    }

    /// Holds the declaration; the table is built and published in boot,
    /// once the hosting service it hydrates aliases from has booted.
    async fn configure(
        &self,
        config: &VhostDocument,
    ) -> congif::Result<()> {
        self.document.store(Arc::new(config.clone()));
        Ok(())
    }

    fn as_config_observer(
        &self
    ) -> Option<&dyn ConfigObserver<Config = Self::Config>> {
        Some(self)
    }
    fn as_partial_config_observer(
        &self
    ) -> Option<&dyn PartialConfigObserver<Config = Self::Config>> {
        Some(self)
    }
}

impl VhostResolver {
    /// The file watcher applies only declaration deltas. Hosting changes and
    /// explicit reload use their own paths; this never reads hosting rows.
    fn apply_file_changes(
        &self,
        config: &VhostDocument,
        paths: &[PathBuf],
    ) -> congif::Result<()> {
        let _publication =
            self.publication.lock().map_err(congif::Error::msg)?;
        let previous = self.document.load_full();
        let mut document = config.clone();
        document.retain_unchanged(&previous);
        let Some(state) = self.state.get() else { return Ok(()) };
        let sources = self
            .sources
            .read()
            .map_err(|_| congif::Error::msg("vhost source lock poisoned"))?
            .clone();
        let mut hosts = Vec::new();
        for source in sources {
            hosts.extend(source(state).map_err(congif::Error::msg)?);
        }
        // Check the complete declaration namespace, including registered code
        // sources, without replacing their active records.
        document.table_with(hosts).map_err(congif::Error::msg)?;
        let desired = document.table().map_err(congif::Error::msg)?;
        let declared_previous = previous.table().map_err(congif::Error::msg)?;
        let current = self.snapshot();
        let mut changed = std::collections::HashSet::new();
        for (key, record) in desired.canonical_entries() {
            if !declared_previous
                .canonical_config(key)
                .is_some_and(|old| Arc::ptr_eq(&old, &record.config))
            {
                changed.insert(key.clone());
            }
        }
        for (key, _) in declared_previous.canonical_entries() {
            if desired.canonical_config(key).is_none() {
                changed.insert(key.clone());
            }
        }
        let external_aliases: Vec<_> = current
            .alias_entries()
            .filter(|(alias, canonical)| {
                changed.contains(*canonical)
                    && declared_previous
                        .resolve_exact(alias)
                        .is_none_or(|(declared, _)| &declared != *canonical)
            })
            .map(|(alias, canonical)| (alias.clone(), canonical.clone()))
            .collect();
        let mut next = current.as_ref().clone();
        // Remove old declarations as a batch so an alias can move between two
        // changed file hosts without depending on their iteration order.
        for key in &changed {
            next.remove_host(key);
        }
        for key in &changed {
            let Some(config) = desired.canonical_config(key) else { continue };
            if !next.insert_canonical(key.clone(), config.clone()) {
                return Err(congif::Error::msg(format!(
                    "vhost '{key}' conflicts with a live hostname"
                )));
            }
            for alias in config.aliases() {
                if alias == key {
                    continue;
                }
                // A DB/live mapping changed independently from file intent.
                // Editing request policy must not move that hostname back.
                if let Some((live, _)) = current.resolve_exact(alias)
                    && &live != key
                    && declared_previous
                        .resolve_exact(alias)
                        .is_none_or(|(declared, _)| declared != live)
                {
                    continue;
                }
                if next.resolve_exact(alias).is_some()
                    || !next.insert_alias(
                        alias.clone(),
                        key.clone(),
                        config.clone(),
                    )
                {
                    return Err(congif::Error::msg(format!(
                        "vhost '{key}': alias '{alias}' conflicts with a live hostname"
                    )));
                }
            }
        }
        for (alias, canonical) in external_aliases {
            let Some(config) = next.canonical_config(&canonical) else {
                continue;
            };
            match next.resolve_exact(&alias) {
                Some((owner, _)) if owner != canonical => {
                    return Err(congif::Error::msg(format!(
                        "vhost alias '{alias}' is already assigned to '{owner}'"
                    )));
                }
                Some(_) => {}
                None => {
                    next.insert_alias(alias, canonical, config);
                }
            }
        }
        let http = state
            .registry_ref()
            .resolve::<HttpConfigProvider>()
            .map(|provider| provider.snapshot());
        next.validate_listeners(http.as_deref()).map_err(congif::Error::msg)?;
        #[cfg(feature = "access-log")]
        {
            let channels = state
                .registry_ref()
                .resolve::<claviron_logging::pipeline::LogChannels>(
            );
            next.validate_access_logs(|name| {
                channels
                    .as_ref()
                    .is_some_and(|channels| channels.channel(name).is_some())
            })
            .map_err(congif::Error::msg)?;
        }
        #[cfg(not(feature = "access-log"))]
        next.validate_access_logs(|_| false).map_err(congif::Error::msg)?;
        if !changed.is_empty() {
            self.publish_locked(next).map_err(congif::Error::msg)?;
        }
        self.document.store(Arc::new(document));
        info!(
            changed_vhosts = ?changed,
            paths = ?paths,
            "🧩 Vhost source changed hosts={}, aliases={}",
            self.host_count(),
            self.alias_count(),
        );
        Ok(())
    }
}

#[async_trait]
impl ConfigObserver for VhostResolver {
    async fn config_changed(
        &self,
        config: &VhostDocument,
    ) -> congif::Result<()> {
        self.apply_file_changes(config, std::slice::from_ref(&self.config_path))
    }
}

#[async_trait]
impl PartialConfigObserver for VhostResolver {
    fn partial_sources(
        &self,
        reader: &ConfigReader,
    ) -> congif::Result<Vec<PathBuf>> {
        partial_sources(self, reader)
    }
    async fn read_partial_config(
        &self,
        reader: &ConfigReader,
        sources: &[PathBuf],
    ) -> congif::Result<VhostDocument> {
        read_partial_document(self, reader, sources).await
    }
    async fn partial_config_changed(
        &self,
        config: &VhostDocument,
        sources: &[PathBuf],
    ) -> congif::Result<()> {
        self.apply_file_changes(config, sources)
    }
}

impl Component<AppState> for VhostResolver {
    /// Aliases hydrate through `HostingService.server_id`, so with `db` this
    /// cannot boot before hosting has; `boot_priority` cannot express that.
    /// EARLY without it put this ahead of a NORMAL `HostingService` and boot
    /// failed with "HostingService is not initialized". The HTTP
    /// configuration and the log channels are checked when registered.
    type Deps = VhostDeps;
    fn name(&self) -> &'static str {
        "VhostResolverService"
    }

    /// EARLY so the snapshot is hydrated BEFORE any NORMAL provider that
    /// resolves `VhostResolver`; `Deps` still boot first.
    fn boot_priority(&self) -> Option<u8> {
        Some(priority::EARLY)
    }

    fn as_configurable(&self) -> Option<&dyn congif::ConfigRegistration> {
        Some(self)
    }

    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        Some(self)
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<AppState, Self::Deps>> {
        Some(self)
    }
}

#[async_trait]
impl Bootable<AppState, VhostDeps> for VhostResolver {
    async fn boot(
        &self,
        deps: VhostDeps,
        state: &AppState,
    ) -> claviron_runtime::registry::Result<()> {
        self.replace_with(state, &deps).await?;

        tracing::debug!(
            "vhost resolver loaded hosts={}, aliases={}",
            self.host_count(),
            self.alias_count(),
        );

        Ok(())
    }
}

#[async_trait]
impl Reloadable<AppState> for VhostResolver {
    /// `priority::EARLY` (50). `FIRST` (0) is reserved for DbService
    /// because vhost reload reads alias rows from the DB pool — the
    /// pool must be rebuilt against the refreshed env BEFORE this
    /// provider runs.
    fn priority(&self) -> Option<u8> {
        Some(priority::EARLY)
    }

    async fn reload(
        &self,
        state: &AppState,
    ) -> claviron_runtime::registry::Result<()> {
        let document = state.config_reader().snapshot::<Self>()?;
        let previous = self.document.load_full();
        let mut document = (*document).clone();
        document.retain_unchanged(&previous);
        self.document.store(Arc::new(document));
        if let Err(error) = self.replace_all(state).await {
            self.document.store(previous);
            return Err(error);
        }

        info!(
            "🧩 Vhost reloaded hosts={}, aliases={}",
            self.host_count(),
            self.alias_count(),
        );

        Ok(())
    }
}

#[cfg(all(test, feature = "db"))]
#[path = "../../tests/loader/resolver.rs"]
mod order_tests;

#[cfg(all(test, feature = "server"))]
#[path = "../../tests/loader/binding.rs"]
mod binding_tests;
