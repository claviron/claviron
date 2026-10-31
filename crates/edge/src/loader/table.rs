use std::collections::HashMap;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::{Arc, LazyLock};

use claviron_types::Host;
use serde::Deserialize;

use crate::config::HostConfig;

#[derive(Clone)]
pub struct VhostRecord {
    pub canonical: Host,
    pub config: Arc<HostConfig>,
    #[cfg(feature = "server")]
    pub(crate) policy: Option<Arc<crate::pipeline::host::RequestPolicy>>,
}

/// One published name binding. Keep the declaration separately shareable:
/// application factories may retain declarations, never other applications.
#[derive(Clone)]
struct VhostEntry {
    record: Arc<VhostRecord>,
    #[cfg(feature = "server")]
    application: Option<crate::pipeline::HostApp>,
}

impl Deref for VhostEntry {
    type Target = VhostRecord;

    fn deref(&self) -> &Self::Target {
        &self.record
    }
}

/// The catchall key, built once rather than on every unmatched lookup.
fn default_host() -> &'static Host {
    static DEFAULT: LazyLock<Host> =
        LazyLock::new(|| Host::Single(Arc::from("default")));
    &DEFAULT
}

#[derive(Clone, Default)]
pub struct VhostTable {
    inner: Arc<HashMap<Host, VhostEntry>>,
    #[cfg(feature = "server")]
    prepared: HashMap<std::any::TypeId, Arc<dyn std::any::Any + Send + Sync>>,
}

impl VhostTable {
    /// A cold-prepared dependency of this exact table publication.
    #[cfg(feature = "server")]
    pub fn prepared<T: Send + Sync + 'static>(&self) -> Option<Arc<T>> {
        self.prepared.get(&std::any::TypeId::of::<T>())?.clone().downcast().ok()
    }

    #[cfg(feature = "server")]
    pub(crate) fn set_prepared(
        &mut self,
        id: std::any::TypeId,
        value: Arc<dyn std::any::Any + Send + Sync>,
    ) {
        if let Some(apps) =
            value.downcast_ref::<crate::pipeline::HostAppSnapshot>()
        {
            // Bind against this candidate before it publishes. Request-time
            // selection then needs neither TypeId nor a second Host lookup.
            for entry in Arc::make_mut(&mut self.inner).values_mut() {
                entry.application = apps.resolve(&entry.canonical);
            }
        }
        self.prepared.insert(id, value);
    }

    /// Dependencies may retain this host view without retaining themselves.
    #[cfg(feature = "server")]
    pub fn declarations_only(&self) -> Self {
        let inner =
            if self.inner.values().any(|entry| entry.application.is_some()) {
                Arc::new(
                    self.inner
                        .iter()
                        .map(|(host, entry)| {
                            (
                                host.clone(),
                                VhostEntry {
                                    record: entry.record.clone(),
                                    application: None,
                                },
                            )
                        })
                        .collect(),
                )
            } else {
                self.inner.clone()
            };
        Self { inner, prepared: HashMap::new() }
    }

    #[cfg(feature = "server")]
    pub(crate) fn retain_unchanged(
        &mut self,
        previous: &Self,
    ) {
        for (key, record) in Arc::make_mut(&mut self.inner) {
            if let Some(old) = previous.inner.get(key)
                && old.canonical == record.canonical
                && Arc::ptr_eq(&old.config, &record.config)
            {
                record.record = old.record.clone();
            }
        }
    }

    pub fn from_host_map(
        raw: impl IntoIterator<Item = (String, Arc<HostConfig>)>
    ) -> crate::Result<Self> {
        let mut table = VhostTable::default();

        for (key, config) in raw {
            let canonical = Host::from_str(&key)
                .map_err(|e| crate::Error::Invalid(format!("{key}: {e}")))?;

            if !table.insert_canonical(canonical.clone(), Arc::clone(&config)) {
                return Err(crate::Error::Invalid(format!(
                    "duplicate vhost key '{canonical}'"
                )));
            }

            for alias in config.aliases() {
                if let Some((owner, _)) = table.resolve_exact(alias) {
                    if owner != canonical {
                        return Err(crate::Error::Invalid(format!(
                            "vhost '{canonical}': alias '{alias}' already belongs to '{owner}'"
                        )));
                    }
                    continue;
                }
                table.insert_alias(
                    alias.clone(),
                    canonical.clone(),
                    Arc::clone(&config),
                );
            }
        }

        Ok(table)
    }

    pub fn resolve(
        &self,
        host: &Host,
    ) -> Option<(Host, Arc<HostConfig>)> {
        self.resolve_record(host).map(|record| {
            (record.canonical.clone(), Arc::clone(&record.config))
        })
    }

    /// The record `host` resolves to — its exact key, else the `default`
    /// catchall — borrowed: the answer [`resolve`](Self::resolve) gives,
    /// without cloning the key or the configuration.
    pub fn resolve_record(
        &self,
        host: &Host,
    ) -> Option<&VhostRecord> {
        self.resolve_entry(host).map(|entry| entry.record.as_ref())
    }

    /// Select config and application from the same published name binding.
    /// Default fallback is identical to declaration-only resolution.
    #[cfg(feature = "server")]
    pub(crate) fn resolve_with_application(
        &self,
        host: &Host,
    ) -> Option<(&VhostRecord, Option<&crate::pipeline::HostApp>)> {
        self.resolve_entry(host)
            .map(|entry| (entry.record.as_ref(), entry.application.as_ref()))
    }

    fn resolve_entry(
        &self,
        host: &Host,
    ) -> Option<&VhostEntry> {
        self.inner.get(host).or_else(|| self.inner.get(default_host()))
    }

    /// Strict resolution — exact key (canonical or alias) only, no
    /// fallback to the `default` catchall.
    pub fn resolve_exact(
        &self,
        host: &Host,
    ) -> Option<(Host, Arc<HostConfig>)> {
        self.inner.get(host).map(|record| {
            (record.canonical.clone(), Arc::clone(&record.config))
        })
    }

    pub fn host_count(&self) -> usize {
        self.canonical_entries().count()
    }

    /// Cross-reference explicit selectors before publishing a host table.
    /// Omitted selectors do not require an HTTP listener provider (CLI use).
    #[cfg(feature = "server")]
    /// Every host's `access_log` names `off` or a channel `declared` knows.
    /// Checked at load, so a misspelt channel is a configuration error and
    /// not a host whose records silently go nowhere.
    pub fn validate_access_logs(
        &self,
        declared: impl Fn(&str) -> bool,
    ) -> crate::Result<()> {
        for (host, record) in self.canonical_entries() {
            if let Some(name) = record.config.access_log()
                && name != "off"
                && !declared(name)
            {
                return Err(crate::Error::Invalid(format!(
                    "host '{host}': access_log names '{name}', which logging.yaml does not declare"
                )));
            }
        }
        Ok(())
    }

    pub fn validate_listeners(
        &self,
        http: Option<&claviron_http::config::HttpSnapshot>,
    ) -> crate::Result<()> {
        for (host, record) in self.canonical_entries() {
            if let Some(ids) = &record.config.listeners {
                for (index, id) in ids.iter().enumerate() {
                    if ids[..index].contains(id) {
                        return Err(crate::Error::Invalid(format!(
                            "host '{host}': duplicate listener '{id}'"
                        )));
                    }
                    if !http.is_some_and(|http| http.listeners.contains_key(id))
                    {
                        return Err(crate::Error::Invalid(format!(
                            "host '{host}': unknown listener '{id}'"
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    pub fn alias_count(&self) -> usize {
        self.inner.len().saturating_sub(self.host_count())
    }

    /// Every alias and the canonical host it names.
    pub fn alias_entries(&self) -> impl Iterator<Item = (&Host, &Host)> {
        self.inner
            .iter()
            .filter(|(key, record)| *key != &record.canonical)
            .map(|(alias, record)| (alias, &record.canonical))
    }

    pub fn canonical_entries(
        &self
    ) -> impl Iterator<Item = (&Host, &Arc<VhostRecord>)> {
        self.inner
            .iter()
            .filter(|(key, record)| *key == &record.canonical)
            .map(|(key, entry)| (key, &entry.record))
    }

    pub fn canonical_config(
        &self,
        canonical: &Host,
    ) -> Option<Arc<HostConfig>> {
        self.inner
            .get(canonical)
            .filter(|record| record.canonical == *canonical)
            .map(|record| Arc::clone(&record.config))
    }

    #[cfg(feature = "server")]
    pub(crate) fn host_keys_for_canonical(
        &self,
        canonical: &Host,
    ) -> Vec<Host> {
        self.inner
            .iter()
            .filter_map(|(key, record)| {
                (record.canonical == *canonical).then_some(key.clone())
            })
            .collect()
    }

    #[cfg(feature = "server")]
    pub(crate) fn record(
        &self,
        key: &Host,
    ) -> Option<&Arc<VhostRecord>> {
        self.inner.get(key).map(|entry| &entry.record)
    }

    pub(crate) fn insert_canonical(
        &mut self,
        canonical: Host,
        config: Arc<HostConfig>,
    ) -> bool {
        self.insert_record(canonical.clone(), canonical, config)
    }

    pub(crate) fn insert_alias(
        &mut self,
        alias: Host,
        canonical: Host,
        config: Arc<HostConfig>,
    ) -> bool {
        if !is_alias_allowed(&canonical, &alias) {
            tracing::warn!(
                "Ignoring alias {:?} for canonical {:?} (type mismatch)",
                alias,
                canonical
            );
            return false;
        }

        self.insert_record(alias, canonical, config)
    }

    fn insert_record(
        &mut self,
        key: Host,
        canonical: Host,
        config: Arc<HostConfig>,
    ) -> bool {
        use std::collections::hash_map::Entry;

        let next = Arc::make_mut(&mut self.inner);
        // Aliases share their canonical's compiled request policy; compile only at
        // table construction, never when a connection/request is accepted.
        #[cfg(feature = "server")]
        let policy = next
            .get(&canonical)
            .filter(|record| record.canonical == canonical)
            .map(|record| record.policy.clone())
            .unwrap_or_else(|| {
                crate::pipeline::host::RequestPolicy::compile(&config)
            });
        match next.entry(key) {
            Entry::Occupied(e) => {
                tracing::trace!("Duplicate vhost entry: {}", e.key());
                false
            }
            Entry::Vacant(v) => {
                v.insert(VhostEntry {
                    record: Arc::new(VhostRecord {
                        canonical,
                        config,
                        #[cfg(feature = "server")]
                        policy,
                    }),
                    #[cfg(feature = "server")]
                    application: None,
                });
                true
            }
        }
    }

    #[cfg(feature = "server")]
    pub(crate) fn remove_host(
        &mut self,
        canonical: &Host,
    ) -> usize {
        let next = Arc::make_mut(&mut self.inner);
        let before = next.len();
        next.retain(|_, record| &record.canonical != canonical);
        before.saturating_sub(next.len())
    }

    #[cfg(feature = "server")]
    pub(crate) fn remove_alias(
        &mut self,
        alias: &Host,
    ) -> bool {
        let next = Arc::make_mut(&mut self.inner);
        match next.get(alias) {
            Some(record) if &record.canonical != alias => {
                next.remove(alias).is_some()
            }
            _ => false,
        }
    }

    #[cfg(feature = "server")]
    pub(crate) fn remove_exact(
        &mut self,
        key: &Host,
    ) -> bool {
        Arc::make_mut(&mut self.inner).remove(key).is_some()
    }
}

impl<'de> Deserialize<'de> for VhostTable {
    fn deserialize<D>(de: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = HashMap::<String, Arc<HostConfig>>::deserialize(de)?;
        VhostTable::from_host_map(raw).map_err(serde::de::Error::custom)
    }
}

fn is_alias_allowed(
    canonical: &Host,
    alias: &Host,
) -> bool {
    matches!(
        (canonical, alias),
        (Host::Dns(_), Host::Dns(_))
            | (Host::Ip(_), Host::Ip(_))
            | (_, Host::Single(_))
            | (Host::Single(_), _)
    )
}

#[cfg(test)]
#[path = "../../tests/loader/table.rs"]
mod tests;
