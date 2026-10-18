#[cfg(feature = "db")]
use std::collections::HashMap;
use std::sync::Arc;

use claviron_types::Host;

use crate::config::HostConfig;
use crate::loader::table::VhostTable;

pub struct VhostSnapshot {
    table: VhostTable,
}

impl VhostSnapshot {
    pub fn from_config(table: VhostTable) -> Self {
        Self { table }
    }

    pub fn from_table(table: VhostTable) -> Self {
        Self { table }
    }

    /// Hydrating a snapshot from the hosting database — the only caller is the
    /// resolver's `db` path, so this follows that feature rather than standing
    /// as unreachable code in a build without it.
    #[cfg(feature = "db")]
    pub fn apply_db_aliases(
        &mut self,
        aliases: HashMap<String, Vec<String>>,
    ) -> claviron_runtime::registry::Result<()> {
        for (vhost_key, hostnames) in aliases {
            let canonical = match vhost_key.parse::<Host>() {
                Ok(host) => host,
                Err(e) => {
                    tracing::warn!(
                        "DB alias references invalid vhost_key '{}': {}",
                        vhost_key,
                        e
                    );
                    continue;
                }
            };

            let Some(config) = self.table.canonical_config(&canonical) else {
                tracing::warn!(
                    "DB alias references non-existent vhost_key '{}'",
                    vhost_key
                );
                continue;
            };

            for alias in hostnames {
                let host = match alias.parse::<Host>() {
                    Ok(host) => host,
                    Err(e) => {
                        tracing::warn!(
                            "DB alias for vhost_key '{}' references invalid hostname '{}': {}",
                            vhost_key,
                            alias,
                            e
                        );
                        continue;
                    }
                };
                self.table.insert_alias(
                    host,
                    canonical.clone(),
                    Arc::clone(&config),
                );
            }
        }

        Ok(())
    }

    pub fn into_table(self) -> VhostTable {
        self.table
    }

    pub fn upsert_host(
        &mut self,
        canonical: Host,
        config: Arc<HostConfig>,
    ) {
        self.table.remove_exact(&canonical);
        self.table.remove_host(&canonical);
        self.table.insert_canonical(canonical.clone(), Arc::clone(&config));

        for alias in config.aliases() {
            self.table.insert_alias(
                alias.clone(),
                canonical.clone(),
                Arc::clone(&config),
            );
        }
    }

    pub fn delete_host(
        &mut self,
        canonical: &Host,
    ) -> bool {
        self.table.remove_host(canonical) > 0
    }

    pub fn upsert_alias(
        &mut self,
        alias: Host,
        canonical: Host,
    ) -> crate::Result<bool> {
        if alias == canonical {
            return Ok(false);
        }

        let Some(config) = self.table.canonical_config(&canonical) else {
            return Err(crate::Error::Missing(format!(
                "vhost not found for alias target: {}",
                canonical
            )));
        };

        if let Some(record) = self.table.record(&alias)
            && record.canonical == alias
        {
            return Err(crate::Error::other(format!(
                "cannot overwrite canonical vhost key with alias: {}",
                alias
            )));
        }

        self.table.remove_alias(&alias);
        Ok(self.table.insert_alias(alias, canonical, config))
    }

    pub fn delete_alias(
        &mut self,
        alias: &Host,
    ) -> bool {
        self.table.remove_alias(alias)
    }
}

#[cfg(all(test, feature = "db"))]
#[path = "../../tests/loader/snapshot.rs"]
mod tests;
